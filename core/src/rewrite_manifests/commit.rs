//! Commits rewritten batches in one snapshot.

use std::collections::{BTreeMap, HashMap, HashSet};

use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind};
use tracing::{info, warn};

use super::types::{
    CommittedManifestRewrite, ManifestBatch, ManifestRewritePlan, RewrittenManifestBatch,
};
use crate::{CompactionError, Result};

/// Commits the rewritten batches of `plan` in one replace snapshot, which swaps each
/// committed batch's input manifests for its output manifests.
///
/// `results` holds the result of [`rewrite_manifest_batch`](super::rewrite_manifest_batch)
/// for every batch, in the order of [`ManifestRewritePlan::batches`]: `results[i]` belongs
/// to batch `i`. A failed result carries no batch index, so the position is what ties it
/// to its batch.
///
/// A batch can only be committed together with every batch that shares an input manifest
/// with it, directly or through other batches, because a manifest that straddles two
/// batches is replaced as a whole. These batches form a group. A failed batch drops its
/// whole group: the group's other batches count as dropped.
///
/// Each group that is still standing is also verified before it is committed: the entries
/// its batches wrote must add up to the live entries its input manifests list, that is
/// the sum of their added and existing file counts, counting each input once. A mismatch
/// means some entry was lost or written twice, for example because a manifest's partition
/// summary didn't describe its entries. A group whose count can't be checked because an
/// input lists no file counts is dropped the same way. All batches of a group that fails
/// verification count as failed, since each rewrote without error and the group's outputs
/// can't be trusted.
///
/// Dropped and failed batches' outputs are not referenced by the snapshot, and their
/// inputs stay in it. Nothing is deleted from storage.
///
/// If no batch survives, because the plan has none or all are dropped or failed, nothing
/// is committed and the returned table is `table`. Otherwise the snapshot summary gets
/// `snapshot_properties` plus `rewrite-batches`, `rewrite-batches-failed` and
/// `rewrite-batches-dropped` with the counts of
/// [`CommittedManifestRewrite`], on top of the summary of a manifest rewrite
/// (`manifests-created`, `manifests-replaced`, ...).
///
/// The commit uses the plan's snapshot id on its branch, writes its manifest list under a
/// fresh commit UUID, so commits of the same plan never overwrite each other's, and relies on the
/// transaction's retries. A retry reloads the table, so manifests appended to the branch
/// since the plan was made are kept. If a concurrent commit removed an input manifest of a
/// batch, such as another rewrite, that batch's group counts as dropped and the other
/// groups are committed in a new transaction. If every remaining group lost an input, the
/// commit fails with the action's error.
///
/// # Errors
///
/// Returns [`CompactionError::Execution`] if `results` doesn't match `plan`, and
/// [`CompactionError::Iceberg`] if the commit fails. A commit error doesn't mean the
/// snapshot wasn't committed: the catalog may have applied it and then failed to respond,
/// such as on a REST timeout or 5xx, for which the commit state is unknown. Outputs are
/// left in storage either way.
pub async fn commit_manifest_batches(
    catalog: &dyn Catalog,
    table: &Table,
    plan: &ManifestRewritePlan,
    results: Vec<Result<RewrittenManifestBatch>>,
    snapshot_properties: HashMap<String, String>,
) -> Result<CommittedManifestRewrite> {
    let batches = plan.batches();
    if results.len() != batches.len() {
        return Err(CompactionError::Execution(format!(
            "Expected one result for each of the plan's {} batches, got {}",
            batches.len(),
            results.len()
        )));
    }
    for (position, result) in results.iter().enumerate() {
        let Ok(rewritten) = result else { continue };
        if rewritten.batch_index != position {
            return Err(CompactionError::Execution(format!(
                "Result {position} is for batch {}; results must be in plan order",
                rewritten.batch_index
            )));
        }
        if let Some(foreign) = rewritten
            .manifests
            .iter()
            .find(|manifest| manifest.added_snapshot_id != plan.snapshot_id())
        {
            return Err(CompactionError::Execution(format!(
                "Manifest {} of batch {position} was not written for this plan",
                foreign.manifest_path
            )));
        }
    }

    let groups = batch_groups(batches);
    let errored: Vec<bool> = results.iter().map(Result::is_err).collect();
    let mut dropped = group_flags(&groups, &errored);
    let mut miscounted = vec![false; batches.len()];
    for members in group_members(&groups).values() {
        if dropped[members[0]] {
            continue;
        }
        if let Err(reason) = verify_group(batches, &results, members) {
            warn!(
                batches = ?members,
                reason,
                "Manifest rewrite batches wrote an unexpected number of entries; dropping their group"
            );
            for &member in members {
                miscounted[member] = true;
                dropped[member] = true;
            }
        }
    }
    let batches_failed = (0..batches.len())
        .filter(|&i| errored[i] || miscounted[i])
        .count();

    for (position, result) in results.iter().enumerate() {
        if let Err(err) = result {
            warn!(batch = position, error = %err, "Manifest rewrite batch failed; dropping its group");
        }
    }

    loop {
        let batches_dropped = dropped.iter().filter(|&&dropped| dropped).count() - batches_failed;
        let batches_committed = batches.len() - batches_failed - batches_dropped;
        if batches_committed == 0 {
            info!(
                batches = batches.len(),
                batches_failed, "No manifest rewrite batch to commit"
            );
            return Ok(CommittedManifestRewrite {
                table: table.clone(),
                batches_committed,
                batches_failed,
                batches_dropped,
            });
        }

        let transaction = Transaction::new(table);
        let mut action = transaction
            .rewrite_manifests()
            .set_snapshot_id(plan.snapshot_id())
            .set_target_branch(plan.branch().to_owned());
        let mut deleted_paths = HashSet::new();
        for ((batch, result), dropped) in batches.iter().zip(&results).zip(&dropped) {
            let Ok(rewritten) = result else { continue };
            if *dropped {
                continue;
            }
            for input in batch.inputs() {
                if deleted_paths.insert(input.manifest_path.as_str()) {
                    action = action.delete_manifest(input.clone());
                }
            }
            for output in &rewritten.manifests {
                action = action.add_manifest(output.clone());
            }
        }

        let mut properties = snapshot_properties.clone();
        properties.insert("rewrite-batches".to_owned(), batches_committed.to_string());
        properties.insert(
            "rewrite-batches-failed".to_owned(),
            batches_failed.to_string(),
        );
        properties.insert(
            "rewrite-batches-dropped".to_owned(),
            batches_dropped.to_string(),
        );
        let transaction = action
            .set_snapshot_properties(properties)
            .apply(transaction)?;
        let err = match transaction.commit(catalog).await {
            Ok(committed) => {
                return Ok(CommittedManifestRewrite {
                    table: committed,
                    batches_committed,
                    batches_failed,
                    batches_dropped,
                });
            }
            Err(err) => err,
        };

        // The action rejects an input that a concurrent commit removed from the branch
        // as `DataInvalid`, which the transaction doesn't retry. Drop the groups that read
        // a removed input and commit the rest; if no surviving group did, or every one
        // did, the error is not ours to recover from.
        if err.kind() != ErrorKind::DataInvalid {
            return Err(err.into());
        }
        let current = current_manifest_paths(catalog, table, plan.branch()).await?;
        let conflicted: HashSet<usize> = (0..batches.len())
            .filter(|&i| {
                !dropped[i]
                    && batches[i]
                        .inputs()
                        .iter()
                        .any(|input| !current.contains(input.manifest_path.as_str()))
            })
            .map(|i| groups[i])
            .collect();
        let conflicted_members: Vec<usize> = (0..batches.len())
            .filter(|&i| conflicted.contains(&groups[i]))
            .collect();
        if conflicted_members.is_empty()
            || (0..batches.len()).all(|i| dropped[i] || conflicted.contains(&groups[i]))
        {
            return Err(err.into());
        }
        warn!(
            batches = ?conflicted_members,
            error = %err,
            "A concurrent commit removed input manifests of manifest rewrite batches; \
             dropping their groups and committing the rest"
        );
        for member in conflicted_members {
            dropped[member] = true;
        }
    }
}

/// The paths of the manifests `branch` of the catalog's current `table` lists; empty if
/// the branch is gone.
async fn current_manifest_paths(
    catalog: &dyn Catalog,
    table: &Table,
    branch: &str,
) -> Result<HashSet<String>> {
    let current = catalog.load_table(table.identifier()).await?;
    let metadata = current.metadata_ref();
    let Some(snapshot) = metadata.snapshot_for_ref(branch) else {
        return Ok(HashSet::new());
    };
    let manifest_list = current
        .object_cache()
        .get_manifest_list(snapshot, &metadata)
        .await?;
    Ok(manifest_list
        .entries()
        .iter()
        .map(|manifest| manifest.manifest_path.clone())
        .collect())
}

/// For each batch, the representative of its group: batches linked by shared input
/// manifests, directly or through other batches, have the same representative.
fn batch_groups(batches: &[ManifestBatch]) -> Vec<usize> {
    fn root(parent: &mut [usize], mut node: usize) -> usize {
        while parent[node] != node {
            parent[node] = parent[parent[node]];
            node = parent[node];
        }
        node
    }

    let mut parent: Vec<usize> = (0..batches.len()).collect();
    let mut first_reader: HashMap<&str, usize> = HashMap::new();
    for (index, batch) in batches.iter().enumerate() {
        for input in batch.inputs() {
            let other = *first_reader
                .entry(input.manifest_path.as_str())
                .or_insert(index);
            let (a, b) = (root(&mut parent, index), root(&mut parent, other));
            parent[a] = b;
        }
    }
    (0..batches.len())
        .map(|index| root(&mut parent, index))
        .collect()
}

/// For each batch, whether its group contains a batch with `flagged` set.
fn group_flags(groups: &[usize], flagged: &[bool]) -> Vec<bool> {
    let flagged_groups: HashSet<usize> = groups
        .iter()
        .zip(flagged)
        .filter(|(_, flagged)| **flagged)
        .map(|(group, _)| *group)
        .collect();
    groups
        .iter()
        .map(|group| flagged_groups.contains(group))
        .collect()
}

/// The batch indexes of each group, ascending, keyed by group representative.
fn group_members(groups: &[usize]) -> BTreeMap<usize, Vec<usize>> {
    let mut members: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (index, group) in groups.iter().enumerate() {
        members.entry(*group).or_default().push(index);
    }
    members
}

/// Checks that the batches `members` of one group, all rewritten successfully, wrote as
/// many entries as the group's inputs list as live. Each input counts once, however many
/// of the group's batches read it.
fn verify_group(
    batches: &[ManifestBatch],
    results: &[Result<RewrittenManifestBatch>],
    members: &[usize],
) -> std::result::Result<(), String> {
    let mut written: u64 = 0;
    for &member in members {
        if let Ok(rewritten) = &results[member] {
            written += rewritten.entries_written;
        }
    }
    let mut seen: HashSet<&str> = HashSet::new();
    let mut listed: u64 = 0;
    for input in members.iter().flat_map(|&member| batches[member].inputs()) {
        if !seen.insert(input.manifest_path.as_str()) {
            continue;
        }
        let (Some(added), Some(existing)) = (input.added_files_count, input.existing_files_count)
        else {
            return Err(format!(
                "input manifest {} doesn't list its file counts",
                input.manifest_path
            ));
        };
        listed += u64::from(added) + u64::from(existing);
    }
    if written == listed {
        Ok(())
    } else {
        Err(format!(
            "batches wrote {written} entries but their inputs list {listed} live entries"
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use iceberg::spec::{
        DataFile, Datum, FormatVersion, Literal, MAIN_BRANCH, ManifestContentType, ManifestFile,
        Operation,
    };

    use super::super::tests::{
        LOAD_CONCURRENCY, LiveEntry, TestEnv, append_files, append_keys, append_keys_to,
        batch_config, create_env, create_partitioned_env, data_manifests, file, first_field_keys,
        global_rewrite, identity_spec, key, live_entries, live_entries_of, manifests_of, plan_of,
        register_evolved, rewrite_all, set_table_property, table_with_existing_and_added_entries,
        table_with_snapshots, test_schema,
    };
    use super::super::types::FirstFieldRange;
    use super::super::{needs_manifest_rewrite, plan_manifest_batches, rewrite_manifest_batch};
    use super::*;

    fn ok(rewritten: Vec<RewrittenManifestBatch>) -> Vec<Result<RewrittenManifestBatch>> {
        rewritten.into_iter().map(Ok).collect()
    }

    fn boom() -> CompactionError {
        CompactionError::Execution("boom".to_owned())
    }

    async fn manifest_paths(table: &Table) -> BTreeSet<String> {
        data_manifests(table)
            .await
            .into_iter()
            .map(|m| m.manifest_path)
            .collect()
    }

    fn summary(table: &Table) -> HashMap<String, String> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);
        snapshot.summary().additional_properties.clone()
    }

    /// The entries of `expected` in `summary`, which may hold more.
    fn assert_summary(table: &Table, expected: &[(&str, &str)]) {
        let summary = summary(table);
        let actual: Vec<_> = expected
            .iter()
            .map(|(name, _)| (*name, summary[*name].as_str()))
            .collect();
        assert_eq!(actual, expected);
    }

    /// Five manifests, each with two keys: the first holds both keys of the second and
    /// third as well, so batches of at most two manifests are `[1, 2, 10, 11]` and
    /// `[10, 11]` (sharing the first manifest), and `[20, 21]` on its own.
    async fn table_with_two_groups_of_batches() -> (TestEnv, Table) {
        table_with_snapshots(&[
            &[(1, "a"), (2, "a"), (10, "a"), (11, "a")],
            &[(1, "a"), (2, "a")],
            &[(10, "a"), (11, "a")],
            &[(20, "a"), (21, "a")],
            &[(20, "a"), (21, "a")],
        ])
        .await
    }

    #[tokio::test]
    async fn commit_manifest_batches_replaces_inputs_with_clustered_outputs() {
        let (env, table) = table_with_existing_and_added_entries().await;
        let config = batch_config(2);
        let plan = plan_of(&table, 2).await;
        let rewritten = rewrite_all(&table, &plan, &config).await;
        let outputs: BTreeSet<String> = rewritten
            .iter()
            .flat_map(|batch| batch.manifests().iter().map(|m| m.manifest_path.clone()))
            .collect();
        let before = live_entries(&table).await;
        assert!(
            needs_manifest_rewrite(&table, MAIN_BRANCH, 1, LOAD_CONCURRENCY)
                .await
                .unwrap()
        );

        let committed = commit_manifest_batches(
            env.catalog.as_ref(),
            &table,
            &plan,
            ok(rewritten),
            HashMap::from([("caller".to_owned(), "kept".to_owned())]),
        )
        .await
        .unwrap();

        assert_eq!(
            (
                committed.batches_committed,
                committed.batches_failed,
                committed.batches_dropped
            ),
            (3, 0, 0)
        );
        let after = committed.table;
        let snapshot = after.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.snapshot_id(), plan.snapshot_id());
        assert_eq!(snapshot.parent_snapshot_id(), Some(plan.base_snapshot_id()));
        assert_summary(&after, &[
            ("caller", "kept"),
            ("rewrite-batches", "3"),
            ("rewrite-batches-failed", "0"),
            ("rewrite-batches-dropped", "0"),
            ("manifests-created", "7"),
            ("manifests-kept", "0"),
            ("manifests-replaced", "6"),
            ("entries-processed", "0"),
        ]);
        assert_eq!(
            env.catalog
                .load_table(table.identifier())
                .await
                .unwrap()
                .metadata_location(),
            after.metadata_location()
        );

        // Same live files, with their original snapshot ids and sequence numbers.
        assert_eq!(live_entries(&after).await, before);
        // Every key is in exactly one manifest, and those are exactly the outputs.
        assert_eq!(manifest_paths(&after).await, outputs);
        let mut keys = Vec::new();
        for manifest in data_manifests(&after).await {
            keys.push(first_field_keys(&after, &manifest).await);
        }
        keys.sort();
        assert_eq!(
            keys,
            (1..=7)
                .map(|id| BTreeSet::from([key(id)]))
                .collect::<Vec<_>>()
        );
        assert!(
            !needs_manifest_rewrite(&after, MAIN_BRANCH, 1, LOAD_CONCURRENCY)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn commit_manifest_batches_commits_rolled_over_manifests() {
        let (env, table) = table_with_existing_and_added_entries().await;
        let table =
            set_table_property(&env, &table, "commit.manifest.target-size-bytes", "200").await;
        let plan = plan_of(&table, 100).await;
        let rewritten = rewrite_all(&table, &plan, &batch_config(100)).await;
        let before = live_entries(&table).await;

        let committed = commit_manifest_batches(
            env.catalog.as_ref(),
            &table,
            &plan,
            ok(rewritten),
            HashMap::new(),
        )
        .await
        .unwrap();

        let after = committed.table;
        assert_eq!(live_entries(&after).await, before);
        assert_summary(&after, &[
            ("manifests-created", "8"),
            ("manifests-replaced", "6"),
        ]);
        // Several manifests for one key, all added by the rewrite, are not duplicates.
        assert!(
            !needs_manifest_rewrite(&after, MAIN_BRANCH, 1, LOAD_CONCURRENCY)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn commit_manifest_batches_drops_groups_with_a_failed_batch() {
        // (failed batch, (committed, failed, dropped), manifests (created, kept, replaced)).
        // Batches 0 and 1 share the first manifest, so they stand or fall together.
        let cases = [
            (0, (1, 1, 1), (2, 3, 2)),
            (1, (1, 1, 1), (2, 3, 2)),
            (2, (2, 1, 0), (4, 2, 3)),
        ];
        for (failed, counts, manifests) in cases {
            let (env, table) = table_with_two_groups_of_batches().await;
            let plan = plan_of(&table, 2).await;
            assert_eq!(plan.batches().len(), 3);
            let mut results = ok(rewrite_all(&table, &plan, &batch_config(2)).await);
            results[failed] = Err(boom());
            let before = live_entries(&table).await;
            let before_paths = manifest_paths(&table).await;

            let committed = commit_manifest_batches(
                env.catalog.as_ref(),
                &table,
                &plan,
                results,
                HashMap::new(),
            )
            .await
            .unwrap();

            let name = format!("failed batch {failed}");
            assert_eq!(
                (
                    committed.batches_committed,
                    committed.batches_failed,
                    committed.batches_dropped
                ),
                counts,
                "{name}"
            );
            let after = committed.table;
            assert_eq!(live_entries(&after).await, before, "{name}");
            assert_summary(&after, &[
                ("rewrite-batches", &counts.0.to_string()),
                ("rewrite-batches-failed", &counts.1.to_string()),
                ("rewrite-batches-dropped", &counts.2.to_string()),
                ("manifests-created", &manifests.0.to_string()),
                ("manifests-kept", &manifests.1.to_string()),
                ("manifests-replaced", &manifests.2.to_string()),
            ]);
            // The inputs of dropped batches are still there, those of committed ones are gone.
            let after_paths = manifest_paths(&after).await;
            let kept: BTreeSet<_> = before_paths.intersection(&after_paths).cloned().collect();
            assert_eq!(kept.len(), manifests.1, "{name}");
        }
    }

    #[tokio::test]
    async fn commit_manifest_batches_fails_groups_whose_entry_counts_do_not_add_up() {
        type Tamper = fn(&mut ManifestRewritePlan, &mut [Result<RewrittenManifestBatch>]);
        type Counts = (usize, usize, usize);
        fn written(results: &mut [Result<RewrittenManifestBatch>], batch: usize) -> &mut u64 {
            &mut results[batch].as_mut().unwrap().entries_written
        }
        // Batches 0 and 1 form one group, batch 2 another; a group's batches all fail
        // together, and nothing else is affected.
        // (name, tamper, (committed, failed, dropped), manifests (created, kept, replaced)).
        let cases: [(&str, Tamper, Counts, Counts); 5] = [
            (
                "an entry too many in the single-batch group",
                |_, results| *written(results, 2) += 1,
                (2, 1, 0),
                (4, 2, 3),
            ),
            (
                "an entry lost in the straddling group",
                |_, results| *written(results, 0) -= 1,
                (1, 2, 0),
                (2, 3, 2),
            ),
            (
                "an entry written twice by the straddling group",
                |_, results| *written(results, 1) += 1,
                (1, 2, 0),
                (2, 3, 2),
            ),
            (
                "an unknown count in the straddling group",
                |plan, _| plan.batches[0].inputs[0].existing_files_count = None,
                (1, 2, 0),
                (2, 3, 2),
            ),
            (
                "an unknown count in the single-batch group",
                |plan, _| plan.batches[2].inputs[0].added_files_count = None,
                (2, 1, 0),
                (4, 2, 3),
            ),
        ];
        for (name, tamper, counts, manifests) in cases {
            let (env, table) = table_with_two_groups_of_batches().await;
            let mut plan = plan_of(&table, 2).await;
            assert_eq!(plan.batches().len(), 3);
            let mut results = ok(rewrite_all(&table, &plan, &batch_config(2)).await);
            let before = live_entries(&table).await;
            tamper(&mut plan, &mut results);

            let committed = commit_manifest_batches(
                env.catalog.as_ref(),
                &table,
                &plan,
                results,
                HashMap::new(),
            )
            .await
            .unwrap();

            assert_eq!(
                (
                    committed.batches_committed,
                    committed.batches_failed,
                    committed.batches_dropped
                ),
                counts,
                "{name}"
            );
            let after = committed.table;
            assert_eq!(live_entries(&after).await, before, "{name}");
            assert_summary(&after, &[
                ("rewrite-batches", &counts.0.to_string()),
                ("rewrite-batches-failed", &counts.1.to_string()),
                ("rewrite-batches-dropped", &counts.2.to_string()),
                ("manifests-created", &manifests.0.to_string()),
                ("manifests-kept", &manifests.1.to_string()),
                ("manifests-replaced", &manifests.2.to_string()),
            ]);
        }
    }

    #[test]
    fn verify_group_compares_written_entries_with_the_listed_live_entries_of_unique_inputs() {
        let input = |path: &str, added: Option<u32>, existing: Option<u32>| ManifestFile {
            manifest_path: path.to_owned(),
            manifest_length: 0,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 1,
            added_files_count: added,
            existing_files_count: existing,
            deleted_files_count: Some(7),
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        };
        let batch = |index: usize, inputs: Vec<ManifestFile>| ManifestBatch {
            index,
            partition_spec_id: 0,
            range: FirstFieldRange::All,
            inputs,
        };
        let rewritten = |index: usize, entries_written: u64| {
            Ok(RewrittenManifestBatch {
                batch_index: index,
                manifests: vec![],
                inputs_read: 0,
                entries_written,
                duration: Default::default(),
            })
        };

        let shared = input("shared", Some(3), Some(2));
        // A manifest without live entries lists 0 and contributes 0, whatever it deleted.
        let empty = input("empty", Some(0), Some(0));
        let batches = [
            batch(0, vec![shared.clone(), empty.clone()]),
            batch(1, vec![shared.clone(), input("own", Some(1), Some(0))]),
        ];
        let verify = |written: [u64; 2]| {
            verify_group(
                &batches,
                &[rewritten(0, written[0]), rewritten(1, written[1])],
                &[0, 1],
            )
        };
        // The shared input's 5 entries are split between the batches and counted once.
        assert_eq!(verify([2, 4]), Ok(()));
        assert_eq!(
            verify([3, 4]),
            Err("batches wrote 7 entries but their inputs list 6 live entries".to_owned())
        );
        assert_eq!(
            verify([2, 3]),
            Err("batches wrote 5 entries but their inputs list 6 live entries".to_owned())
        );

        let unknown = [batch(0, vec![input("unknown", None, Some(1))])];
        assert_eq!(
            verify_group(&unknown, &[rewritten(0, 1)], &[0]),
            Err("input manifest unknown doesn't list its file counts".to_owned())
        );
    }

    #[test]
    fn batch_groups_link_batches_through_shared_inputs() {
        let batch = |index: usize, paths: &[&str]| ManifestBatch {
            index,
            partition_spec_id: 0,
            range: FirstFieldRange::All,
            inputs: paths
                .iter()
                .map(|path| ManifestFile {
                    manifest_path: (*path).to_owned(),
                    manifest_length: 0,
                    partition_spec_id: 0,
                    content: ManifestContentType::Data,
                    sequence_number: 0,
                    min_sequence_number: 0,
                    added_snapshot_id: 1,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: None,
                    existing_rows_count: None,
                    deleted_rows_count: None,
                    partitions: None,
                    key_metadata: None,
                    first_row_id: None,
                })
                .collect(),
        };

        // (name, batches, failed batch, groups, dropped).
        let cases = [
            (
                // Batches 0 and 2 share nothing, but each shares an input with batch 1.
                "chain",
                vec![
                    batch(0, &["m1", "m2"]),
                    batch(1, &["m2", "m3"]),
                    batch(2, &["m3", "m4"]),
                    batch(3, &["m5"]),
                ],
                2,
                vec![0, 0, 0, 3],
                vec![true, true, true, false],
            ),
            (
                // The last batch joins two groups that were separate until then.
                "late link",
                vec![
                    batch(0, &["m1"]),
                    batch(1, &["m2"]),
                    batch(2, &["m5"]),
                    batch(3, &["m1", "m2"]),
                ],
                0,
                vec![1, 1, 2, 1],
                vec![true, true, false, true],
            ),
        ];
        for (name, batches, failed, groups, dropped) in cases {
            let actual = batch_groups(&batches);
            assert_eq!(actual, groups, "{name}");
            let mut flagged = vec![false; batches.len()];
            flagged[failed] = true;
            assert_eq!(group_flags(&actual, &flagged), dropped, "{name}");
        }
    }

    #[tokio::test]
    async fn commit_manifest_batches_commits_nothing_when_no_batch_survives() {
        let (env, table) = table_with_two_groups_of_batches().await;
        let plan = plan_of(&table, 2).await;
        let (straddling_env, straddling_table) = table_with_existing_and_added_entries().await;
        let straddling_plan = plan_of(&straddling_table, 2).await;
        let (empty_env, empty_table) = table_with_snapshots(&[&[(1, "a")], &[(2, "a")]]).await;
        let empty_plan = plan_of(&empty_table, 3).await;
        assert!(empty_plan.batches().is_empty());

        let mut one_failure =
            ok(rewrite_all(&straddling_table, &straddling_plan, &batch_config(2)).await);
        one_failure[1] = Err(boom());
        let cases = [
            // (name, env, table, plan, results, (failed, dropped))
            (
                "all failed",
                &env,
                &table,
                &plan,
                vec![Err(boom()), Err(boom()), Err(boom())],
                (3, 0),
            ),
            (
                "one failure drops the one group",
                &straddling_env,
                &straddling_table,
                &straddling_plan,
                one_failure,
                (1, 2),
            ),
            (
                "no batches",
                &empty_env,
                &empty_table,
                &empty_plan,
                vec![],
                (0, 0),
            ),
        ];
        for (name, env, table, plan, results, (failed, dropped)) in cases {
            let committed =
                commit_manifest_batches(env.catalog.as_ref(), table, plan, results, HashMap::new())
                    .await
                    .unwrap();

            assert_eq!(
                (
                    committed.batches_committed,
                    committed.batches_failed,
                    committed.batches_dropped
                ),
                (0, failed, dropped),
                "{name}"
            );
            assert_eq!(
                committed.table.metadata_location(),
                table.metadata_location(),
                "{name}"
            );
            assert_eq!(
                env.catalog
                    .load_table(table.identifier())
                    .await
                    .unwrap()
                    .metadata_location(),
                table.metadata_location(),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn commit_manifest_batches_carries_forward_concurrent_appends_only() {
        let (env, table) = table_with_existing_and_added_entries().await;
        let plan = plan_of(&table, 2).await;
        let rewritten = rewrite_all(&table, &plan, &batch_config(2)).await;
        let before = live_entries(&table).await;

        // Another writer appends after the plan was made; the commit still succeeds, with
        // the table it was given being stale.
        let appended = append_keys(&env, &table, &[(8, "a")]).await;
        let committed = commit_manifest_batches(
            env.catalog.as_ref(),
            &table,
            &plan,
            ok(rewritten.clone()),
            HashMap::new(),
        )
        .await
        .unwrap();

        let after = committed.table;
        let mut expected = live_entries(&appended).await;
        assert_eq!(expected.len(), 13);
        assert_eq!(
            expected
                .keys()
                .filter(|path| !before.contains_key(*path))
                .count(),
            1
        );
        assert_eq!(live_entries(&after).await, std::mem::take(&mut expected));
        assert_summary(&after, &[
            ("manifests-created", "7"),
            ("manifests-kept", "1"),
            ("manifests-replaced", "6"),
        ]);

        // A concurrent commit that removed an input is an error, not a silent skip.
        let (env, table) = table_with_existing_and_added_entries().await;
        let plan = plan_of(&table, 2).await;
        let rewritten = rewrite_all(&table, &plan, &batch_config(2)).await;
        let concurrent = global_rewrite(&env, &table).await;
        let err = commit_manifest_batches(
            env.catalog.as_ref(),
            &table,
            &plan,
            ok(rewritten),
            HashMap::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CompactionError::Iceberg(_)), "{err:?}");
        assert!(
            err.to_string()
                .contains("Deleted manifest does not exist in the current snapshot"),
            "{err}"
        );
        assert_eq!(
            env.catalog
                .load_table(table.identifier())
                .await
                .unwrap()
                .metadata_location(),
            concurrent.metadata_location()
        );
    }

    #[tokio::test]
    async fn commit_manifest_batches_drops_groups_whose_inputs_a_concurrent_commit_removed() {
        let env = create_env(
            test_schema(),
            identity_spec(&[(1, "id")]),
            FormatVersion::V2,
            &[],
        )
        .await;
        // Batches never mix specs, so each spec's batches form their own groups.
        let mut table = register_evolved(&env, &env.table, "two_specs", |builder| {
            builder.add_partition_spec(identity_spec(&[(2, "name")]))
        })
        .await;
        for (suffix, (ids, names)) in [(vec![1, 2], vec!["a", "b"]), (vec![2, 3], vec!["b", "c"])]
            .into_iter()
            .enumerate()
        {
            let mut files: Vec<DataFile> = ids
                .iter()
                .map(|id| {
                    file(
                        0,
                        vec![Some(Literal::int(*id))],
                        &format!("id-{id}-{suffix}"),
                    )
                })
                .collect();
            files.extend(names.iter().map(|name| {
                file(
                    1,
                    vec![Some(Literal::string(*name))],
                    &format!("name-{name}-{suffix}"),
                )
            }));
            table = append_files(&env, &table, MAIN_BRANCH, files).await;
        }
        let plan = plan_of(&table, 100).await;
        let specs: Vec<i32> = plan
            .batches()
            .iter()
            .map(|b| b.partition_spec_id())
            .collect();
        assert_eq!(specs, vec![0, 1]);
        let rewritten = rewrite_all(&table, &plan, &batch_config(100)).await;

        // Another writer rewrites spec 1's manifests after the plan was made.
        let transaction = Transaction::new(&table);
        let concurrent = transaction
            .rewrite_manifests()
            .rewrite_if(Box::new(|m| m.partition_spec_id == 1))
            .cluster_by(Box::new(|file| {
                format!("{:?}", file.partition().fields()[0])
            }))
            .apply(transaction)
            .unwrap()
            .commit(env.catalog.as_ref())
            .await
            .unwrap();
        let spec_1_after_concurrent: BTreeSet<String> = data_manifests(&concurrent)
            .await
            .into_iter()
            .filter(|m| m.partition_spec_id == 1)
            .map(|m| m.manifest_path)
            .collect();

        let committed = commit_manifest_batches(
            env.catalog.as_ref(),
            &table,
            &plan,
            ok(rewritten.clone()),
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            (
                committed.batches_committed,
                committed.batches_failed,
                committed.batches_dropped
            ),
            (1, 0, 1)
        );
        let after = committed.table;
        assert_summary(&after, &[
            ("rewrite-batches", "1"),
            ("rewrite-batches-dropped", "1"),
        ]);
        let manifests = data_manifests(&after).await;
        // Spec 0 now holds the batch's outputs; spec 1 the concurrent rewrite's.
        let spec_0: BTreeSet<String> = manifests
            .iter()
            .filter(|m| m.partition_spec_id == 0)
            .map(|m| m.manifest_path.clone())
            .collect();
        let outputs: BTreeSet<String> = rewritten[0]
            .manifests()
            .iter()
            .map(|m| m.manifest_path.clone())
            .collect();
        assert_eq!(spec_0, outputs);
        let spec_1: BTreeSet<String> = manifests
            .iter()
            .filter(|m| m.partition_spec_id == 1)
            .map(|m| m.manifest_path.clone())
            .collect();
        assert_eq!(spec_1, spec_1_after_concurrent);
        assert_eq!(live_entries(&after).await, live_entries(&concurrent).await);
    }

    #[tokio::test]
    async fn commit_manifest_batches_rejects_results_that_do_not_match_the_plan() {
        let (env, table) = table_with_existing_and_added_entries().await;
        let plan = plan_of(&table, 2).await;
        let other_plan = plan_of(&table, 2).await;
        let rewritten = rewrite_all(&table, &plan, &batch_config(2)).await;
        let foreign = rewrite_manifest_batch(
            &table,
            &other_plan,
            &other_plan.batches()[0],
            &batch_config(2),
        )
        .await
        .unwrap();
        let foreign_path = foreign.manifests()[0].manifest_path.clone();

        let mut short = ok(rewritten.clone());
        short.pop();
        let mut reordered = ok(rewritten.clone());
        reordered.swap(0, 1);
        let mut foreign_results = ok(rewritten);
        foreign_results[0] = Ok(foreign);

        let cases = [
            (
                short,
                "Execution failed: Expected one result for each of the plan's 3 batches, got 2"
                    .to_owned(),
            ),
            (
                reordered,
                "Execution failed: Result 0 is for batch 1; results must be in plan order"
                    .to_owned(),
            ),
            (
                foreign_results,
                format!(
                    "Execution failed: Manifest {foreign_path} of batch 0 was not written for \
                     this plan"
                ),
            ),
        ];
        for (results, expected) in cases {
            let err = commit_manifest_batches(
                env.catalog.as_ref(),
                &table,
                &plan,
                results,
                HashMap::new(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.to_string(), expected);
        }
    }

    /// Plans, rewrites and commits `table`'s `branch` with batches of at most
    /// `max_batch_manifests` manifests, returning the plan and the committed table.
    async fn batched_rewrite(
        env: &TestEnv,
        table: &Table,
        branch: &str,
        max_batch_manifests: usize,
    ) -> (ManifestRewritePlan, Table) {
        let config = batch_config(max_batch_manifests);
        let plan = plan_manifest_batches(table, branch, 1, &config)
            .await
            .unwrap()
            .unwrap();
        let rewritten = rewrite_all(table, &plan, &config).await;
        let committed = commit_manifest_batches(
            env.catalog.as_ref(),
            table,
            &plan,
            ok(rewritten),
            HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            (committed.batches_failed, committed.batches_dropped),
            (0, 0)
        );
        (plan, committed.table)
    }

    /// The first-field keys of each manifest of `manifests`, sorted.
    async fn keys_per_manifest(table: &Table, manifests: &[ManifestFile]) -> Vec<BTreeSet<String>> {
        let mut keys = Vec::new();
        for manifest in manifests {
            keys.push(first_field_keys(table, manifest).await);
        }
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn batched_rewrite_commits_to_the_planned_branch_only() {
        let env = create_partitioned_env().await;
        let mut table = env.table.clone();
        for keys in [
            &[(1, "a"), (2, "a"), (3, "a")][..],
            &[(2, "a"), (3, "a")],
            &[(7, "a"), (8, "a")],
            &[(8, "a")],
        ] {
            table = append_keys_to(&env, &table, "audit", keys).await;
        }
        table = append_keys(&env, &table, &[(1, "a"), (2, "a")]).await;
        table = append_keys(&env, &table, &[(1, "a")]).await;
        let main_snapshot = table.metadata().current_snapshot_id();
        let main_manifests = manifests_of(&table, MAIN_BRANCH).await;
        let audit_before = live_entries_of(&table, &manifests_of(&table, "audit").await).await;
        assert_eq!(audit_before.len(), 8);

        let (plan, after) = batched_rewrite(&env, &table, "audit", 2).await;

        assert_eq!(plan.branch(), "audit");
        assert_eq!(plan.batches().len(), 2);
        let audit = after.metadata().snapshot_for_ref("audit").unwrap();
        assert_eq!(audit.snapshot_id(), plan.snapshot_id());
        assert_eq!(audit.parent_snapshot_id(), Some(plan.base_snapshot_id()));
        let audit_manifests = manifests_of(&after, "audit").await;
        assert_eq!(
            live_entries_of(&after, &audit_manifests).await,
            audit_before
        );
        assert_eq!(
            keys_per_manifest(&after, &audit_manifests).await,
            [1, 2, 3, 7, 8].map(|id| BTreeSet::from([key(id)])).to_vec()
        );
        // Main was not touched.
        assert_eq!(after.metadata().current_snapshot_id(), main_snapshot);
        assert_eq!(manifests_of(&after, MAIN_BRANCH).await, main_manifests);
    }

    #[tokio::test]
    async fn batched_rewrite_clusters_string_first_fields() {
        let env = create_env(
            test_schema(),
            identity_spec(&[(2, "name"), (1, "id")]),
            FormatVersion::V2,
            &[],
        )
        .await;
        let named = |name: &str, id: i32, suffix: usize| {
            file(
                0,
                vec![Some(Literal::string(name)), Some(Literal::int(id))],
                &format!("{name}-{id}-{suffix}"),
            )
        };
        let mut table = env.table.clone();
        for (suffix, names) in [
            vec!["apple", "banana", "cherry"],
            vec!["banana", "cherry", "date"],
            vec!["fig", "grape"],
            vec!["fig", "kiwi", "kiwi"],
            vec!["pear"],
        ]
        .into_iter()
        .enumerate()
        {
            let files = names
                .iter()
                .enumerate()
                .map(|(id, name)| named(name, id as i32, suffix))
                .collect();
            table = append_files(&env, &table, MAIN_BRANCH, files).await;
        }
        let before = live_entries(&table).await;
        assert_eq!(before.len(), 12);

        let (plan, after) = batched_rewrite(&env, &table, MAIN_BRANCH, 2).await;

        let fig = Datum::string("fig");
        let batches: Vec<(FirstFieldRange, usize)> = plan
            .batches()
            .iter()
            .map(|b| (b.range().clone(), b.inputs().len()))
            .collect();
        assert_eq!(batches, vec![
            (
                FirstFieldRange::Values {
                    lower: None,
                    upper: Some(fig.clone())
                },
                2
            ),
            (
                FirstFieldRange::Values {
                    lower: Some(fig),
                    upper: None
                },
                2
            ),
        ]);
        assert_eq!(live_entries(&after).await, before);
        let string_key = |name: &str| format!("Some(Primitive(String(\"{name}\")))");
        assert_eq!(
            keys_per_manifest(&after, &data_manifests(&after).await).await,
            [
                "apple", "banana", "cherry", "date", "fig", "grape", "kiwi", "pear"
            ]
            .map(|name| BTreeSet::from([string_key(name)]))
            .to_vec()
        );
        assert!(
            !needs_manifest_rewrite(&after, MAIN_BRANCH, 1, LOAD_CONCURRENCY)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn batched_rewrite_keeps_partition_specs_apart() {
        let env = create_env(
            test_schema(),
            identity_spec(&[(1, "id")]),
            FormatVersion::V2,
            &[],
        )
        .await;
        // Spec 1 partitions by the name instead; files of both specs are in the table.
        let table = register_evolved(&env, &env.table, "two_specs", |builder| {
            builder.add_partition_spec(identity_spec(&[(2, "name")]))
        })
        .await;
        assert_eq!(table.metadata().partition_specs_iter().count(), 2);
        let by_id = |id: i32, suffix: usize| {
            file(
                0,
                vec![Some(Literal::int(id))],
                &format!("id-{id}-{suffix}"),
            )
        };
        let by_name = |name: &str, suffix: usize| {
            file(
                1,
                vec![Some(Literal::string(name))],
                &format!("name-{name}-{suffix}"),
            )
        };
        let mut table = table;
        for (suffix, (ids, names)) in [
            (vec![1, 2, 3], vec!["a", "b"]),
            (vec![2, 3, 4], vec!["b", "c"]),
            (vec![10, 11], vec!["x", "y"]),
        ]
        .into_iter()
        .enumerate()
        {
            let mut files: Vec<DataFile> = ids.iter().map(|id| by_id(*id, suffix)).collect();
            files.extend(names.iter().map(|name| by_name(name, suffix)));
            table = append_files(&env, &table, MAIN_BRANCH, files).await;
        }
        let before = live_entries(&table).await;
        assert_eq!(before.len(), 14);
        assert_eq!(data_manifests(&table).await.len(), 6);

        let (plan, after) = batched_rewrite(&env, &table, MAIN_BRANCH, 2).await;

        let batch_specs: Vec<(i32, usize)> = plan
            .batches()
            .iter()
            .map(|b| (b.partition_spec_id(), b.inputs().len()))
            .collect();
        // Each spec's three manifests are split in two, two manifests and one.
        assert_eq!(batch_specs, vec![(0, 2), (0, 1), (1, 2), (1, 1)]);
        for batch in plan.batches() {
            assert!(
                batch
                    .inputs()
                    .iter()
                    .all(|m| m.partition_spec_id == batch.partition_spec_id())
            );
        }
        assert_eq!(live_entries(&after).await, before);
        // One manifest per key and spec, and each output stays in its input's spec.
        let mut per_spec = Vec::new();
        for manifest in data_manifests(&after).await {
            per_spec.push((
                manifest.partition_spec_id,
                first_field_keys(&after, &manifest).await,
            ));
        }
        per_spec.sort();
        let string_key = |name: &str| format!("Some(Primitive(String(\"{name}\")))");
        let mut expected: Vec<(i32, BTreeSet<String>)> = [1, 2, 3, 4, 10, 11]
            .map(|id| (0, BTreeSet::from([key(id)])))
            .into_iter()
            .chain(["a", "b", "c", "x", "y"].map(|name| (1, BTreeSet::from([string_key(name)]))))
            .collect();
        expected.sort();
        assert_eq!(per_spec, expected);
    }

    #[tokio::test]
    async fn batched_rewrite_drops_empty_manifests_of_specs_with_nothing_else_to_rewrite() {
        let build = || async {
            let env = create_env(
                test_schema(),
                identity_spec(&[(1, "id")]),
                FormatVersion::V2,
                &[],
            )
            .await;
            let table = register_evolved(&env, &env.table, "two_specs", |builder| {
                builder.add_partition_spec(identity_spec(&[(2, "name")]))
            })
            .await;
            let by_id = |id: i32, suffix: usize| {
                file(
                    0,
                    vec![Some(Literal::int(id))],
                    &format!("id-{id}-{suffix}"),
                )
            };
            let by_name = file(1, vec![Some(Literal::string("a"))], "name-a");
            let table = append_files(&env, &table, MAIN_BRANCH, vec![
                by_id(1, 0),
                by_id(2, 0),
                by_name.clone(),
            ])
            .await;
            // Key 2 again from another snapshot, so spec 0 needs rewriting.
            let table =
                append_files(&env, &table, MAIN_BRANCH, vec![by_id(2, 1), by_id(3, 1)]).await;
            // Leaves spec 1 with only a manifest without live entries.
            let transaction = Transaction::new(&table);
            let table = transaction
                .overwrite_files()
                .delete_files(vec![by_name])
                .apply(transaction)
                .unwrap()
                .commit(env.catalog.as_ref())
                .await
                .unwrap();
            (env, table)
        };
        let (env, table) = build().await;
        let (global_env, global_table) = build().await;
        let empty: Vec<String> = data_manifests(&table)
            .await
            .into_iter()
            .filter(|m| m.partition_spec_id == 1)
            .inspect(|m| {
                assert_eq!(
                    (m.added_files_count, m.existing_files_count),
                    (Some(0), Some(0))
                )
            })
            .map(|m| m.manifest_path)
            .collect();
        assert_eq!(empty.len(), 1);

        let (plan, batched) = batched_rewrite(&env, &table, MAIN_BRANCH, 100).await;
        let global = global_rewrite(&global_env, &global_table).await;

        let spec_1_batch = plan.batches().last().unwrap();
        assert_eq!(
            (
                spec_1_batch.partition_spec_id(),
                spec_1_batch.range(),
                spec_1_batch
                    .inputs()
                    .iter()
                    .map(|m| m.manifest_path.clone())
                    .collect::<Vec<_>>()
            ),
            (1, &FirstFieldRange::All, empty)
        );
        let specs = |table: Table| async move {
            let mut specs: Vec<i32> = data_manifests(&table)
                .await
                .iter()
                .map(|m| m.partition_spec_id)
                .collect();
            specs.sort();
            specs
        };
        // Like the global rewrite, one manifest per key of spec 0 and none of spec 1.
        assert_eq!(specs(batched.clone()).await, vec![0, 0, 0]);
        assert_eq!(specs(global).await, vec![0, 0, 0]);
        assert_eq!(live_entries(&batched).await, live_entries(&table).await);
    }

    #[tokio::test]
    async fn batched_rewrite_matches_global_rewrite_on_other_format_versions() {
        for format_version in [FormatVersion::V1, FormatVersion::V3] {
            let build = || async {
                let env = create_env(
                    test_schema(),
                    identity_spec(&[(1, "id"), (2, "name")]),
                    format_version,
                    &[],
                )
                .await;
                let mut table = env.table.clone();
                for keys in [
                    &[(1, "a"), (2, "a"), (3, "a")][..],
                    &[(2, "a"), (3, "b"), (5, "a")],
                    &[(1, "b"), (4, "a"), (6, "a")],
                ] {
                    table = append_keys(&env, &table, keys).await;
                }
                (env, table)
            };
            let (env, table) = build().await;
            let (global_env, global_table) = build().await;

            let (plan, batched) = batched_rewrite(&env, &table, MAIN_BRANCH, 2).await;
            let global = global_rewrite(&global_env, &global_table).await;

            let name = format!("{format_version:?}");
            assert_eq!(plan.batches().len(), 2, "{name}");
            // Entries as the global rewrite leaves them: files, and for v3 their row ids.
            let rows = |table: Table| async move {
                let mut rows = BTreeMap::new();
                for manifest in data_manifests(&table).await {
                    let manifest = manifest.load_manifest(table.file_io()).await.unwrap();
                    for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                        let file = entry.data_file();
                        rows.insert(
                            file.file_path().to_owned(),
                            (file.record_count(), file.first_row_id()),
                        );
                    }
                }
                rows
            };
            let batched_rows = rows(batched.clone()).await;
            // Row ids, assigned when the files were appended, survive the rewrite.
            let mut row_ids: Vec<_> = batched_rows.values().map(|(_, row_id)| *row_id).collect();
            row_ids.sort();
            let expected_row_ids: Vec<Option<i64>> = (0..9)
                .map(|i| (format_version == FormatVersion::V3).then_some(7 * i))
                .collect();
            assert_eq!(row_ids, expected_row_ids, "{name}");
            assert_eq!(batched_rows.len(), 9, "{name}");
            assert_eq!(batched_rows, rows(global.clone()).await, "{name}");
            assert_eq!(
                live_entries(&batched).await.len(),
                live_entries(&table).await.len(),
                "{name}"
            );
            assert_eq!(
                keys_per_manifest(&batched, &data_manifests(&batched).await)
                    .await
                    .len(),
                6,
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn batched_rewrite_matches_global_rewrite() {
        let (env, table) = table_with_existing_and_added_entries().await;
        let (global_env, global_table) = table_with_existing_and_added_entries().await;

        let plan = plan_of(&table, 2).await;
        let rewritten = rewrite_all(&table, &plan, &batch_config(2)).await;
        let batched = commit_manifest_batches(
            env.catalog.as_ref(),
            &table,
            &plan,
            ok(rewritten),
            HashMap::new(),
        )
        .await
        .unwrap()
        .table;
        let global = global_rewrite(&global_env, &global_table).await;

        // Snapshot ids differ between the two tables, so compare files and their grouping.
        let files = |entries: &BTreeMap<String, LiveEntry>| -> BTreeMap<String, u64> {
            entries
                .iter()
                .map(|(path, (rows, ..))| (path.clone(), *rows))
                .collect()
        };
        assert_eq!(
            files(&live_entries(&batched).await),
            files(&live_entries(&global).await)
        );
        let grouping = |table: Table| async move {
            let mut groups = BTreeSet::new();
            for manifest in data_manifests(&table).await {
                groups.insert(
                    live_entries_of(&table, &[manifest])
                        .await
                        .into_keys()
                        .collect::<BTreeSet<_>>(),
                );
            }
            groups
        };
        assert_eq!(grouping(batched).await, grouping(global).await);
    }
}
