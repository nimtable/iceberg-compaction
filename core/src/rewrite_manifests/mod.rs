//! Manifest rewrite support.
//!
//! [`needs_manifest_rewrite`] decides whether rewriting a branch's data manifests would
//! change how their entries are clustered, so callers can skip a no-op rewrite commit.
//!
//! A rewrite can also run in independent batches, which bounds the memory and time of each
//! step and lets the caller run them in parallel:
//!
//! 1. [`plan_manifest_batches`] splits the manifests that need rewriting into
//!    [`ManifestBatch`]es of a [`ManifestRewritePlan`], by ranges of the first partition
//!    field. Each batch is planned around [`BatchedRewriteManifestsConfig`]'s targets for
//!    manifests and entries, which are not hard caps: manifests straddling a cut, with
//!    unusable bounds or without live entries are added on top, and a manifest over a
//!    target, or a run of identical lower bounds, forms a batch of its own. It returns
//!    `None` if the table can't be split or batched safely (unpartitioned, a single
//!    first-field value, an unresolvable first-field type, or encrypted), and the caller
//!    should use a global rewrite.
//! 2. [`rewrite_manifest_batch`] writes the new manifests of one batch. The caller owns
//!    concurrency: plans, batches and results are owned and `Send`, so each batch can run
//!    on a spawned task.
//! 3. [`commit_manifest_batches`] replaces the inputs of the batches that succeeded with
//!    their outputs in one snapshot. Failed batches, and the batches that share input
//!    manifests with them, are left out, so a partially failed rewrite still commits. A
//!    group of batches whose written entry count doesn't match the file counts of its
//!    inputs is left out too.
//!
//! Manifests written by batches that a successful commit leaves out stay in storage
//! unreferenced. Nothing written is deleted when a commit fails either, and a commit error
//! doesn't prove the outputs are unreferenced: the commit may have been applied anyway,
//! for example when the catalog reports that the commit state is unknown. Orphan-file
//! cleanup is out of scope. Encrypted tables are not supported by the batched rewrite.

use iceberg::spec::{ManifestContentType, ManifestFile};
use iceberg::table::Table;
use tracing::info;

use crate::{CompactionError, Result};

mod classify;
mod commit;
mod loading;
mod planner;
mod rewrite;
mod sweep;
mod types;

use classify::{SummaryDecision, decide_from_summaries};
pub use commit::commit_manifest_batches;
use loading::needs_rewrite_by_loading;
pub use planner::plan_manifest_batches;
pub use rewrite::rewrite_manifest_batch;
pub use types::{
    BatchedRewriteManifestsConfig, CommittedManifestRewrite, FirstFieldRange, ManifestBatch,
    ManifestRewritePlan, RewrittenManifestBatch,
};

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
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::sync::Arc;

    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal,
        MAIN_BRANCH, NestedField, PrimitiveLiteral, PrimitiveType, Schema, SnapshotReference,
        SnapshotRetention, Struct, TableMetadataBuilder, TableProperties, Transform, Type,
        UnboundPartitionSpec,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{
        Catalog, CatalogBuilder, MetadataLocation, NamespaceIdent, TableCreation, TableIdent,
    };
    use tempfile::TempDir;

    use super::*;

    pub(super) const LOAD_CONCURRENCY: usize = 4;

    pub(super) struct TestEnv {
        _temp_dir: TempDir,
        pub(super) catalog: Arc<MemoryCatalog>,
        pub(super) table: Table,
    }

    pub(super) fn test_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    /// Creates an empty table with `schema` and `spec`.
    pub(super) async fn create_env(
        schema: Schema,
        spec: UnboundPartitionSpec,
        format_version: FormatVersion,
        properties: &[(&str, &str)],
    ) -> TestEnv {
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

        let table_ident = TableIdent::new(namespace.clone(), "test_table".into());
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(schema)
                    .partition_spec(spec)
                    .format_version(format_version)
                    .properties(
                        properties
                            .iter()
                            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
                    )
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

    /// The spec with identity partition fields on the given `(source id, name)` pairs.
    pub(super) fn identity_spec(fields: &[(i32, &str)]) -> UnboundPartitionSpec {
        let mut builder = UnboundPartitionSpec::builder();
        for (source_id, name) in fields {
            builder = builder
                .add_partition_field(*source_id, *name, Transform::Identity)
                .unwrap();
        }
        builder.build()
    }

    /// Creates a v2 table partitioned by identity(`id`), identity(`name`).
    pub(super) async fn create_partitioned_env() -> TestEnv {
        create_env(
            test_schema(),
            identity_spec(&[(1, "id"), (2, "name")]),
            FormatVersion::V2,
            &[],
        )
        .await
    }

    /// Registers the metadata that `evolve` makes of `table`'s as a new table `name`, so
    /// tests can add specs and refs, which the transaction API can't.
    pub(super) async fn register_evolved(
        env: &TestEnv,
        table: &Table,
        name: &str,
        evolve: impl FnOnce(TableMetadataBuilder) -> iceberg::Result<TableMetadataBuilder>,
    ) -> Table {
        let builder = TableMetadataBuilder::new_from_metadata(table.metadata().clone(), None);
        let metadata = evolve(builder).unwrap().build().unwrap().metadata;
        let location = MetadataLocation::try_new_with_metadata(&metadata).unwrap();
        metadata.write_to(table.file_io(), &location).await.unwrap();
        let ident = TableIdent::new(table.identifier().namespace().clone(), name.to_owned());
        env.catalog
            .register_table(&ident, location.to_string())
            .await
            .unwrap()
    }

    /// Only manifests are read by the check, so the data file need not exist.
    pub(super) fn file(spec_id: i32, partition: Vec<Option<Literal>>, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("s3://bucket/{path}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter(partition))
            .partition_spec_id(spec_id)
            .record_count(path.len() as u64)
            .file_size_in_bytes(1)
            .build()
            .unwrap()
    }

    /// A file of the default `(id, name)` layout under spec 0.
    fn data_file(id: i32, name: &str, path_suffix: &str) -> DataFile {
        file(
            0,
            vec![
                Some(Literal::Primitive(PrimitiveLiteral::Int(id))),
                Some(Literal::Primitive(PrimitiveLiteral::String(
                    name.to_owned(),
                ))),
            ],
            &format!("{id}-{name}-{path_suffix}"),
        )
    }

    /// Commits one fast-append snapshot to `branch`, which adds exactly one manifest
    /// holding `files`.
    pub(super) async fn append_files(
        env: &TestEnv,
        table: &Table,
        branch: &str,
        files: Vec<DataFile>,
    ) -> Table {
        let transaction = Transaction::new(table);
        let tx = transaction
            .fast_append()
            .set_target_branch(branch.to_owned())
            .add_data_files(files)
            .apply(transaction)
            .unwrap();
        tx.commit(env.catalog.as_ref()).await.unwrap()
    }

    /// Commits one fast-append snapshot to `branch`, with a file for each `(id, name)` key.
    pub(super) async fn append_keys_to(
        env: &TestEnv,
        table: &Table,
        branch: &str,
        keys: &[(i32, &str)],
    ) -> Table {
        let snapshot_index = table.metadata().snapshots().count();
        let files = keys
            .iter()
            .enumerate()
            .map(|(seq, (id, name))| data_file(*id, name, &format!("{snapshot_index}-{seq}")))
            .collect();
        append_files(env, table, branch, files).await
    }

    /// Commits one fast-append snapshot to main, which adds exactly one manifest, holding a
    /// file for each `(id, name)` key.
    pub(super) async fn append_keys(env: &TestEnv, table: &Table, keys: &[(i32, &str)]) -> Table {
        append_keys_to(env, table, MAIN_BRANCH, keys).await
    }

    /// Builds a table with one snapshot per entry of `snapshots`.
    pub(super) async fn table_with_snapshots(snapshots: &[&[(i32, &str)]]) -> (TestEnv, Table) {
        let env = create_partitioned_env().await;
        let mut table = env.table.clone();
        for keys in snapshots {
            table = append_keys(&env, &table, keys).await;
        }
        (env, table)
    }

    /// The manifests of the snapshot `branch` points to.
    pub(super) async fn manifests_of(table: &Table, branch: &str) -> Vec<ManifestFile> {
        let metadata = table.metadata_ref();
        let snapshot = metadata.snapshot_for_ref(branch).unwrap();
        let manifest_list = table
            .object_cache()
            .get_manifest_list(snapshot, &metadata)
            .await
            .unwrap();
        manifest_list.entries().to_vec()
    }

    pub(super) async fn data_manifests(table: &Table) -> Vec<ManifestFile> {
        manifests_of(table, MAIN_BRANCH).await
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

    pub(super) fn batch_config(max_batch_manifests: usize) -> BatchedRewriteManifestsConfig {
        BatchedRewriteManifestsConfig {
            max_batch_entries: 1_000_000,
            max_batch_manifests,
            load_concurrency: LOAD_CONCURRENCY,
            max_read_amplification: None,
        }
    }

    /// The manifest added by the snapshot with index `snapshot_index` (0 = first append).
    async fn manifest_of_snapshot(table: &Table, snapshot_index: usize) -> ManifestFile {
        let mut snapshots: Vec<_> = table.metadata().snapshots().collect();
        snapshots.sort_by_key(|s| s.sequence_number());
        let snapshot_id = snapshots[snapshot_index].snapshot_id();
        let mut manifests = data_manifests(table).await;
        manifests.retain(|m| m.added_snapshot_id == snapshot_id);
        assert_eq!(manifests.len(), 1);
        manifests.remove(0)
    }

    #[tokio::test]
    async fn plan_manifest_batches_plans_the_branch_snapshot() {
        let (_env, table) =
            table_with_snapshots(&[&[(1, "a"), (2, "a"), (3, "a")], &[(10, "a")], &[(2, "a")]])
                .await;

        // Callers spawn planning on a runtime.
        fn assert_send<T: Send>(future: T) -> T {
            future
        }
        let config = batch_config(100);
        let plan = assert_send(plan_manifest_batches(&table, MAIN_BRANCH, 1, &config))
            .await
            .unwrap()
            .unwrap();
        let mixed = manifest_of_snapshot(&table, 0).await;
        let overlapped = manifest_of_snapshot(&table, 2).await;
        assert_eq!(plan.branch(), MAIN_BRANCH);
        assert_eq!(
            plan.base_snapshot_id(),
            table.metadata().current_snapshot_id().unwrap()
        );
        assert_eq!(plan.clustered_fields(), 1);
        assert!(
            table
                .metadata()
                .snapshots()
                .all(|s| s.snapshot_id() != plan.snapshot_id())
        );
        let batches = plan.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].index(), 0);
        assert_eq!(batches[0].partition_spec_id(), 0);
        assert_eq!(batches[0].range(), &FirstFieldRange::Values {
            lower: None,
            upper: None
        });
        // The clean manifest of snapshot 1 is skipped; the inputs are the exact list entries.
        assert_eq!(batches[0].inputs(), [mixed, overlapped]);

        let other = plan_manifest_batches(&table, MAIN_BRANCH, 1, &batch_config(100))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(other.snapshot_id(), plan.snapshot_id());
    }

    #[tokio::test]
    async fn plan_manifest_batches_splits_by_first_field_ranges() {
        let (_env, table) = table_with_snapshots(&[
            &[(1, "a"), (2, "a")],
            &[(2, "b"), (3, "a")],
            &[(10, "a"), (11, "a")],
        ])
        .await;

        let plan = plan_manifest_batches(&table, MAIN_BRANCH, 1, &batch_config(2))
            .await
            .unwrap()
            .unwrap();
        let ranges: Vec<_> = plan
            .batches()
            .iter()
            .map(|b| (b.index(), b.range().clone(), b.inputs().len()))
            .collect();
        assert_eq!(ranges, vec![
            (
                0,
                FirstFieldRange::Values {
                    lower: None,
                    upper: Some(Datum::int(10))
                },
                2
            ),
            (
                1,
                FirstFieldRange::Values {
                    lower: Some(Datum::int(10)),
                    upper: None
                },
                1
            ),
        ]);

        // Every entry of every input is in exactly one range among the batches reading it.
        for input in plan.batches().iter().flat_map(|b| b.inputs()) {
            let manifest = input.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                let writers = plan
                    .batches()
                    .iter()
                    .filter(|b| b.inputs().contains(input))
                    .filter(|b| b.range().contains(entry.data_file().partition()).unwrap())
                    .count();
                assert_eq!(writers, 1, "{}", entry.data_file().file_path());
            }
        }
    }

    #[tokio::test]
    async fn plan_manifest_batches_returns_none_when_a_global_rewrite_is_needed() {
        let empty = create_partitioned_env().await;
        let (_env, one_value) = table_with_snapshots(&[&[(1, "a")], &[(1, "a")]]).await;
        let (_env2, two_values) = table_with_snapshots(&[&[(1, "a")], &[(2, "a")]]).await;

        let none = |table: &Table, branch: &str| {
            let table = table.clone();
            let branch = branch.to_owned();
            async move {
                plan_manifest_batches(&table, &branch, 1, &batch_config(100))
                    .await
                    .unwrap()
                    .map(|plan| plan.batches().len())
            }
        };
        assert_eq!(none(&empty.table, MAIN_BRANCH).await, None, "no snapshot");
        assert_eq!(none(&one_value, "missing").await, None, "no such branch");
        assert_eq!(none(&one_value, MAIN_BRANCH).await, None, "one value");
        assert_eq!(
            none(&two_values, MAIN_BRANCH).await,
            Some(0),
            "two values, nothing to rewrite"
        );
    }

    #[tokio::test]
    async fn plan_manifest_batches_rejects_tags() {
        let (env, table) = table_with_snapshots(&[&[(1, "a"), (2, "a")], &[(3, "a")]]).await;
        let snapshot_id = table.metadata().current_snapshot_id().unwrap();
        let tagged = register_evolved(&env, &table, "tagged", |builder| {
            builder.set_ref(
                "release",
                SnapshotReference::new(snapshot_id, SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                }),
            )
        })
        .await;

        let err = plan_manifest_batches(&tagged, "release", 1, &batch_config(100))
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid configuration: release is a tag, not a branch; a rewrite can only be \
             committed to a branch"
        );
        // The same snapshot is planned through the branch.
        let plan = plan_manifest_batches(&tagged, MAIN_BRANCH, 1, &batch_config(100))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(plan.base_snapshot_id(), snapshot_id);
    }

    #[tokio::test]
    async fn encrypted_tables_are_planned_as_global_and_not_rewritten_in_batches() {
        let (env, table) = table_with_snapshots(&[&[(1, "a"), (2, "a")], &[(3, "a")]]).await;
        let plan = plan_of(&table, 100).await;
        assert_eq!(plan.batches().len(), 1);

        // A key id without an encryption manager, as on a table below v3: the rewrite
        // would write its manifests in plaintext.
        let encrypted = set_table_property(
            &env,
            &table,
            TableProperties::PROPERTY_ENCRYPTION_KEY_ID,
            "key",
        )
        .await;
        let planned = plan_manifest_batches(&encrypted, MAIN_BRANCH, 1, &batch_config(100))
            .await
            .unwrap();
        assert_eq!(planned.map(|plan| plan.batches().len()), None);

        let mut with_key_metadata = plan.clone();
        with_key_metadata.batches[0].inputs[0].key_metadata = Some(vec![1, 2, 3]);
        let cases = [
            ("key id property", &encrypted, &plan),
            ("input key metadata", &table, &with_key_metadata),
        ];
        for (name, table, plan) in cases {
            let err = rewrite_manifest_batch(table, plan, &plan.batches()[0], &batch_config(100))
                .await
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                "Execution failed: Batched manifest rewrite does not support encrypted tables",
                "{name}"
            );
        }
    }

    /// A live entry's record count, snapshot id, data sequence number and file sequence number.
    pub(super) type LiveEntry = (u64, Option<i64>, Option<i64>, Option<i64>);

    /// The live entries of `manifests` by data file path, as loaded (inherited values filled in).
    pub(super) async fn live_entries_of(
        table: &Table,
        manifests: &[ManifestFile],
    ) -> BTreeMap<String, LiveEntry> {
        let mut entries = BTreeMap::new();
        for manifest_file in manifests {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                let value = (
                    entry.record_count(),
                    entry.snapshot_id(),
                    entry.sequence_number(),
                    entry.file_sequence_number,
                );
                assert_eq!(entries.insert(entry.file_path().to_owned(), value), None);
            }
        }
        entries
    }

    /// The live entries of the data manifests of the table's main branch.
    pub(super) async fn live_entries(table: &Table) -> BTreeMap<String, LiveEntry> {
        live_entries_of(table, &data_manifests(table).await).await
    }

    /// The first-field keys of the live entries of `manifest`.
    pub(super) async fn first_field_keys(
        table: &Table,
        manifest: &ManifestFile,
    ) -> BTreeSet<String> {
        let manifest = manifest.load_manifest(table.file_io()).await.unwrap();
        manifest
            .entries()
            .iter()
            .filter(|e| e.is_alive())
            .map(|e| format!("{:?}", e.data_file().partition().fields()[0]))
            .collect()
    }

    /// Rewrites all data manifests of `table` with the global rewrite, clustered by the
    /// first partition field.
    pub(super) async fn global_rewrite(env: &TestEnv, table: &Table) -> Table {
        let transaction = Transaction::new(table);
        let tx = transaction
            .rewrite_manifests()
            .cluster_by(Box::new(|file| {
                format!("{:?}", file.partition().fields()[0])
            }))
            .apply(transaction)
            .unwrap();
        tx.commit(env.catalog.as_ref()).await.unwrap()
    }

    pub(super) async fn set_table_property(
        env: &TestEnv,
        table: &Table,
        key: &str,
        value: &str,
    ) -> Table {
        let transaction = Transaction::new(table);
        let tx = transaction
            .update_table_properties()
            .set(key.to_owned(), value.to_owned())
            .apply(transaction)
            .unwrap();
        tx.commit(env.catalog.as_ref()).await.unwrap()
    }

    /// Two appends, a global rewrite of both (whose entries are then "existing" with their
    /// original snapshots), and two more appends. 12 live files in 6 manifests; each of
    /// the keys 1 to 7 is in at least one.
    pub(super) async fn table_with_existing_and_added_entries() -> (TestEnv, Table) {
        let (env, table) = table_with_snapshots(&[&[(1, "a"), (2, "a"), (3, "a"), (3, "a")], &[
            (2, "a"),
            (3, "b"),
            (5, "a"),
        ]])
        .await;
        let table = global_rewrite(&env, &table).await;
        let table = append_keys(&env, &table, &[(1, "b"), (4, "a"), (6, "a")]).await;
        let table = append_keys(&env, &table, &[(4, "b"), (7, "a")]).await;
        (env, table)
    }

    pub(super) async fn plan_of(table: &Table, max_batch_manifests: usize) -> ManifestRewritePlan {
        plan_manifest_batches(table, MAIN_BRANCH, 1, &batch_config(max_batch_manifests))
            .await
            .unwrap()
            .unwrap()
    }

    pub(super) async fn rewrite_all(
        table: &Table,
        plan: &ManifestRewritePlan,
        config: &BatchedRewriteManifestsConfig,
    ) -> Vec<RewrittenManifestBatch> {
        let mut rewritten = Vec::new();
        for batch in plan.batches() {
            rewritten.push(
                rewrite_manifest_batch(table, plan, batch, config)
                    .await
                    .unwrap(),
            );
        }
        rewritten
    }

    pub(super) fn key(id: i32) -> String {
        format!("Some(Primitive(Int({id})))")
    }
}
