//! Manifest rewrite support.
//!
//! [`needs_manifest_rewrite`] decides whether rewriting a branch's data manifests would
//! change how their entries are clustered, so callers can skip a no-op rewrite commit.

use iceberg::spec::{ManifestContentType, ManifestFile};
use iceberg::table::Table;
use tracing::info;

use crate::{CompactionError, Result};

mod classify;
mod loading;

use classify::{SummaryDecision, decide_from_summaries};
use loading::needs_rewrite_by_loading;

/// Checks whether rewriting the data manifests of `branch` would change their clustering.
///
/// Manifests are clustered by the first `clustered_fields` partition fields (and the
/// partition spec id). A rewrite is needed when:
///
/// - a manifest has live entries for more than one cluster key, or
/// - manifests added by *different* snapshots share a cluster key and could be
///   consolidated. Same-key manifests added by one snapshot are not duplicates, because
///   a rewrite rolls a large key over into several manifests at the target manifest size
///   and would otherwise be reported as needed on every check. As a consequence, a
///   commit that rewrites existing manifests stamps them all with its own snapshot id,
///   so some real duplicates go unnoticed until another snapshot touches the same key.
///
/// Only data manifests are considered; delete manifests are always kept by a rewrite.
/// The check assumes the rewrite has no manifest predicate.
///
/// Partition summaries in the manifest list decide first. If any data manifest's summary
/// is inconclusive (missing, containing NaN, or malformed) and no other manifest already
/// proves a rewrite is needed, every data manifest is loaded and its live entries are
/// inspected instead, with up to `load_concurrency` manifests loading at once.
///
/// `clustered_fields` selects the cluster key:
///
/// - `1` clusters by the first partition field only.
/// - A value of at least the spec's field count clusters by the whole partition.
/// - `0` clusters by the partition spec id alone, so any two manifests of one spec added
///   by different snapshots count as duplicates.
///
/// A rewrite that follows this check must use the same cluster key, otherwise the check
/// and the rewrite disagree on what "clustered" means.
///
/// Returns `false` when `branch` does not exist or has no snapshot.
///
/// # Errors
///
/// Returns [`CompactionError::Config`] if `load_concurrency` is `0`, and
/// [`CompactionError::Iceberg`] if the manifest list or a manifest cannot be read.
pub async fn needs_manifest_rewrite(
    table: &Table,
    branch: &str,
    clustered_fields: usize,
    load_concurrency: usize,
) -> Result<bool> {
    if load_concurrency == 0 {
        return Err(CompactionError::Config(
            "load_concurrency must be at least 1".to_owned(),
        ));
    }

    let metadata = table.metadata_ref();
    let Some(snapshot) = metadata.snapshot_for_ref(branch) else {
        return Ok(false);
    };
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &metadata)
        .await?;

    needs_rewrite(
        table,
        manifest_list.entries(),
        clustered_fields,
        load_concurrency,
    )
    .await
}

/// Runs the summary pass over the data manifests in `manifests`, falling back to loading
/// them when no summary is conclusive.
async fn needs_rewrite(
    table: &Table,
    manifests: &[ManifestFile],
    clustered_fields: usize,
    load_concurrency: usize,
) -> Result<bool> {
    let data_manifests = || {
        manifests
            .iter()
            .filter(|manifest_file| manifest_file.content == ManifestContentType::Data)
    };

    let inconclusive_manifests = match decide_from_summaries(data_manifests(), clustered_fields) {
        SummaryDecision::NeedsRewrite => return Ok(true),
        SummaryDecision::NoRewrite => return Ok(false),
        SummaryDecision::Inconclusive { manifests } => manifests,
    };

    // Summary keys and loaded-entry keys use different encodings, so the fallback
    // re-checks every data manifest rather than only the inconclusive ones.
    info!(
        inconclusive_manifests,
        data_manifest_count = data_manifests().count(),
        "Partition summaries inconclusive; loading manifests for rewrite no-op check"
    );
    needs_rewrite_by_loading(
        table,
        data_manifests().cloned().collect(),
        clustered_fields,
        load_concurrency,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        NestedField, PrimitiveLiteral, PrimitiveType, Schema, Struct, Transform, Type,
        UnboundPartitionSpec,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

    const LOAD_CONCURRENCY: usize = 4;

    struct TestEnv {
        _temp_dir: TempDir,
        catalog: Arc<MemoryCatalog>,
        table: Table,
    }

    /// Creates a table partitioned by identity(`id`), identity(`name`).
    async fn create_partitioned_env() -> TestEnv {
        let temp_dir = TempDir::new().unwrap();
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_owned(),
                        temp_dir.path().to_str().unwrap().to_owned(),
                    )]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("test_namespace".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        let partition_spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "id", Transform::Identity)
            .unwrap()
            .add_partition_field(2, "name", Transform::Identity)
            .unwrap()
            .build();
        let table_ident = TableIdent::new(namespace.clone(), "test_table".into());
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(schema)
                    .partition_spec(partition_spec)
                    .build(),
            )
            .await
            .unwrap();
        let table = catalog.load_table(&table_ident).await.unwrap();

        TestEnv {
            _temp_dir: temp_dir,
            catalog,
            table,
        }
    }

    /// Only manifests are read by the check, so the data file need not exist.
    fn data_file(id: i32, name: &str, path_suffix: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("s3://bucket/{id}-{name}-{path_suffix}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([
                Some(Literal::Primitive(PrimitiveLiteral::Int(id))),
                Some(Literal::Primitive(PrimitiveLiteral::String(
                    name.to_owned(),
                ))),
            ]))
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(1)
            .build()
            .unwrap()
    }

    /// Commits one fast-append snapshot, which adds exactly one manifest, holding a
    /// file for each `(id, name)` key.
    async fn append_keys(env: &TestEnv, table: &Table, keys: &[(i32, &str)]) -> Table {
        let snapshot_index = table.metadata().snapshots().count();
        let files = keys
            .iter()
            .enumerate()
            .map(|(seq, (id, name))| data_file(*id, name, &format!("{snapshot_index}-{seq}")))
            .collect::<Vec<_>>();
        let transaction = Transaction::new(table);
        let tx = transaction
            .fast_append()
            .add_data_files(files)
            .apply(transaction)
            .unwrap();
        tx.commit(env.catalog.as_ref()).await.unwrap()
    }

    /// Builds a table with one snapshot per entry of `snapshots`.
    async fn table_with_snapshots(snapshots: &[&[(i32, &str)]]) -> (TestEnv, Table) {
        let env = create_partitioned_env().await;
        let mut table = env.table.clone();
        for keys in snapshots {
            table = append_keys(&env, &table, keys).await;
        }
        (env, table)
    }

    async fn data_manifests(table: &Table) -> Vec<ManifestFile> {
        let metadata = table.metadata_ref();
        let snapshot = metadata.snapshot_for_ref(MAIN_BRANCH).unwrap();
        let manifest_list = table
            .object_cache()
            .get_manifest_list(snapshot, &metadata)
            .await
            .unwrap();
        manifest_list.entries().to_vec()
    }

    /// Manifests whose summaries are dropped, so the check must load them.
    async fn manifests_without_summaries(table: &Table) -> Vec<ManifestFile> {
        let mut manifests = data_manifests(table).await;
        for manifest in &mut manifests {
            manifest.partitions = None;
        }
        manifests
    }

    #[tokio::test]
    async fn no_snapshot_or_missing_branch_needs_no_rewrite() {
        let env = create_partitioned_env().await;
        for clustered_fields in [0, 1, 2] {
            assert!(
                !needs_manifest_rewrite(&env.table, MAIN_BRANCH, clustered_fields, 1)
                    .await
                    .unwrap(),
                "empty table, clustered_fields={clustered_fields}"
            );
        }

        let (_env, table) = table_with_snapshots(&[&[(1, "a")], &[(1, "a")]]).await;
        assert!(
            needs_manifest_rewrite(&table, MAIN_BRANCH, 2, 1)
                .await
                .unwrap(),
            "main has duplicate keys"
        );
        assert!(
            !needs_manifest_rewrite(&table, "missing", 2, 1)
                .await
                .unwrap(),
            "a branch without a snapshot is a no-op"
        );
    }

    #[tokio::test]
    async fn zero_load_concurrency_is_rejected() {
        let (_env, table) = table_with_snapshots(&[&[(1, "a")]]).await;
        let err = needs_manifest_rewrite(&table, MAIN_BRANCH, 1, 0)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid configuration: load_concurrency must be at least 1"
        );
    }

    #[tokio::test]
    async fn needs_rewrite_ignores_delete_manifests() {
        let (_env, table) = table_with_snapshots(&[&[(1, "a"), (2, "b")]]).await;
        let mut manifests = data_manifests(&table).await;
        manifests[0].content = ManifestContentType::Deletes;

        assert!(
            !needs_rewrite(&table, &manifests, 2, LOAD_CONCURRENCY)
                .await
                .unwrap(),
            "a mixed delete manifest is never rewritten"
        );
    }

    type Keys<'a> = &'a [(i32, &'a str)];

    /// `(name, snapshots, [(clustered_fields, expected)])`; each snapshot is one manifest.
    type Case<'a> = (&'a str, Vec<Keys<'a>>, Vec<(usize, bool)>);

    fn clustering_cases<'a>() -> Vec<Case<'a>> {
        vec![
            (
                "one manifest with one key",
                vec![&[(1, "a"), (1, "a")]],
                vec![(0, false), (1, false), (2, false), (3, false)],
            ),
            (
                "one manifest with two names under one id",
                vec![&[(1, "a"), (1, "b")]],
                vec![(0, false), (1, false), (2, true), (3, true)],
            ),
            (
                "one manifest with two ids",
                vec![&[(1, "a"), (2, "a")]],
                vec![(0, false), (1, true), (2, true), (3, true)],
            ),
            (
                "same key in two snapshots",
                vec![&[(1, "a")], &[(1, "a")]],
                vec![(0, true), (1, true), (2, true), (3, true)],
            ),
            (
                "distinct ids in two snapshots",
                vec![&[(1, "a")], &[(2, "a")]],
                vec![(0, true), (1, false), (2, false), (3, false)],
            ),
            (
                "same id, distinct names in two snapshots",
                vec![&[(1, "a")], &[(1, "b")]],
                vec![(0, true), (1, true), (2, false), (3, false)],
            ),
            (
                "distinct keys across many snapshots",
                vec![&[(1, "a")], &[(2, "a")], &[(3, "a")], &[(4, "a")], &[(
                    5, "a",
                )]],
                vec![(1, false), (2, false), (3, false)],
            ),
        ]
    }

    #[tokio::test]
    async fn needs_manifest_rewrite_from_summaries() {
        for (name, snapshots, expectations) in clustering_cases() {
            let (_env, table) = table_with_snapshots(&snapshots).await;
            for (clustered_fields, expected) in expectations {
                assert_eq!(
                    needs_manifest_rewrite(&table, MAIN_BRANCH, clustered_fields, 1)
                        .await
                        .unwrap(),
                    expected,
                    "{name}, clustered_fields={clustered_fields}"
                );
            }
        }
    }

    #[tokio::test]
    async fn needs_rewrite_by_loading_matches_summaries() {
        for (name, snapshots, expectations) in clustering_cases() {
            let (_env, table) = table_with_snapshots(&snapshots).await;
            let manifests = manifests_without_summaries(&table).await;
            for (clustered_fields, expected) in expectations {
                // Concurrency below, at, and above the manifest count.
                for load_concurrency in [1, snapshots.len(), snapshots.len() + 3] {
                    assert_eq!(
                        needs_rewrite(&table, &manifests, clustered_fields, load_concurrency)
                            .await
                            .unwrap(),
                        expected,
                        "{name}, clustered_fields={clustered_fields}, \
                         load_concurrency={load_concurrency}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn needs_rewrite_by_loading_scopes_duplicates_by_snapshot_and_spec() {
        let (_env, table) = table_with_snapshots(&[&[(1, "a")], &[(1, "a")]]).await;
        let manifests = manifests_without_summaries(&table).await;
        assert_eq!(manifests.len(), 2);

        let mut same_snapshot = manifests.clone();
        same_snapshot[1].added_snapshot_id = same_snapshot[0].added_snapshot_id;
        assert!(
            !needs_rewrite(&table, &same_snapshot, 2, LOAD_CONCURRENCY)
                .await
                .unwrap(),
            "same key added by one snapshot is rollover, not duplication"
        );

        let mut other_spec = manifests;
        other_spec[1].partition_spec_id = other_spec[0].partition_spec_id + 1;
        assert!(
            !needs_rewrite(&table, &other_spec, 2, LOAD_CONCURRENCY)
                .await
                .unwrap(),
            "the same partition value under another spec is a different key"
        );
    }

    #[tokio::test]
    async fn needs_rewrite_falls_back_to_loading_only_when_inconclusive() {
        let (_env, table) = table_with_snapshots(&[&[(1, "a")], &[(2, "a")], &[(3, "a")]]).await;

        // The fallback loads every manifest and finds three distinct single-key manifests.
        let mut manifests = data_manifests(&table).await;
        manifests[0].partitions = None;
        assert!(
            !needs_rewrite(&table, &manifests, 2, LOAD_CONCURRENCY)
                .await
                .unwrap()
        );

        // A duplicate proven by summaries wins without loading: this manifest's path
        // does not exist, so loading it would fail.
        let mut duplicate = data_manifests(&table).await;
        duplicate.push(duplicate[0].clone());
        duplicate[3].added_snapshot_id = duplicate[1].added_snapshot_id;
        duplicate[3].partitions = duplicate[0].partitions.clone();
        duplicate[2].partitions = None;
        duplicate[2].manifest_path = "missing.avro".to_owned();
        assert!(
            needs_rewrite(&table, &duplicate, 2, LOAD_CONCURRENCY)
                .await
                .unwrap()
        );

        // An inconclusive manifest that cannot be loaded surfaces as an Iceberg error.
        let mut unreadable = data_manifests(&table).await;
        unreadable[2].partitions = None;
        unreadable[2].manifest_path = "missing.avro".to_owned();
        let err = needs_rewrite(&table, &unreadable, 2, LOAD_CONCURRENCY)
            .await
            .unwrap_err();
        assert!(matches!(err, CompactionError::Iceberg(_)), "{err:?}");
    }
}
