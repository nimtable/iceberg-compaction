//! Rewrites the manifests of one planned batch.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use iceberg::spec::{Manifest, ManifestContentType, ManifestFile, ManifestWriter, TableProperties};
use iceberg::table::Table;
use iceberg::transaction::{
    MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT, ManifestWriterContext,
};
use tokio_util::task::AbortOnDropHandle;
use uuid::Uuid;

use super::loading::cluster_key;
use super::types::{
    BatchedRewriteManifestsConfig, ManifestBatch, ManifestRewritePlan, RewrittenManifestBatch,
};
use crate::{CompactionError, Result};

/// Rewrites one batch of `plan`: loads its input manifests and writes the live entries in
/// the batch's range into new manifests, one cluster key per manifest.
///
/// Entries keep their original snapshot id and sequence numbers. A key's manifest is
/// rolled over once it reaches the table's `commit.manifest.target-size-bytes` (8 MiB if
/// unset or malformed), like the global rewrite.
///
/// The manifests are written but not committed; pass the result of every batch to
/// [`commit_manifest_batches`](super::commit_manifest_batches). Batches are independent,
/// so callers can rewrite them concurrently. Every call names its manifests with a fresh
/// UUID, so running a batch again, even after a commit that used an earlier run's
/// outputs, never overwrites a manifest another run wrote. A failed or repeated batch may
/// leave unreferenced manifest files behind.
///
/// At most `config.load_concurrency` inputs are loaded at once, on spawned tasks.
///
/// Encrypted tables are not supported: the manifests would be written in plaintext. A
/// table counts as encrypted if it has the `encryption.key-id` property, an encryption
/// manager, or an input with key metadata. [`plan_manifest_batches`](super::plan_manifest_batches)
/// returns `None` for such tables.
///
/// # Errors
///
/// Returns [`CompactionError::Config`] if `config` has a zero limit,
/// [`CompactionError::Execution`] if `batch` is not part of `plan`, the table is
/// encrypted, an input whose file counts list no live files has a live entry, or an
/// entry lacks the snapshot id or data sequence number a rewrite must preserve, and [`CompactionError::Iceberg`] if a manifest cannot be read or written,
/// including when an entry lacks a file sequence number, which the manifest writer
/// rejects as invalid data.
pub async fn rewrite_manifest_batch(
    table: &Table,
    plan: &ManifestRewritePlan,
    batch: &ManifestBatch,
    config: &BatchedRewriteManifestsConfig,
) -> Result<RewrittenManifestBatch> {
    let start = Instant::now();
    config.validate()?;
    if plan
        .batches()
        .get(batch.index())
        .is_none_or(|planned| planned.inputs() != batch.inputs())
    {
        return Err(CompactionError::Execution(format!(
            "Batch {} is not part of the plan",
            batch.index()
        )));
    }
    // `ManifestWriterContext::new` takes an `Arc<EncryptionManager>`, but a `Table` only
    // lends its manager (`Table::encryption_manager` returns a reference), so a writer
    // for an encrypted table can't be built here. Reject the table-level signals of
    // encryption too, because a table can have the key-id property and no manager (below
    // v3), and the manifests would then be written in plaintext.
    if is_encrypted(table, batch.inputs()) {
        return Err(CompactionError::Execution(
            "Batched manifest rewrite does not support encrypted tables".to_owned(),
        ));
    }

    let metadata = table.metadata_ref();
    let partition_spec = metadata
        .partition_spec_by_id(batch.partition_spec_id())
        .ok_or_else(|| {
            CompactionError::Execution(format!(
                "Batch {} uses unknown partition spec {}",
                batch.index(),
                batch.partition_spec_id()
            ))
        })?;
    let target_size = metadata
        .properties()
        .get(MANIFEST_TARGET_SIZE_BYTES)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| u64::from(MANIFEST_TARGET_SIZE_BYTES_DEFAULT));

    // Outputs only need the plan's snapshot id to be committed; their paths, named
    // `{uuid}-m{n}.avro`, are unique to this call.
    let writer_context = ManifestWriterContext::new(
        metadata.metadata_location()?,
        Uuid::now_v7(),
        Arc::new(AtomicU64::new(0)),
        metadata.format_version(),
        plan.snapshot_id(),
        table.file_io().clone(),
        None,
    );

    let mut loads = OrderedLoads::new(table, batch.inputs().to_vec(), config.load_concurrency);

    let mut writers: BTreeMap<String, ManifestWriter> = BTreeMap::new();
    let mut manifests = Vec::new();
    let mut inputs_read = 0;
    let mut entries_written = 0;
    while let Some((input, manifest)) = loads.next().await? {
        inputs_read += 1;
        // The planner puts a manifest whose counts list no live files in a single batch,
        // whatever its entries' ranges, and the commit's entry count expects nothing from
        // it. Live entries in it would be lost, so fail instead.
        let lists_no_live_files =
            input.added_files_count == Some(0) && input.existing_files_count == Some(0);
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let data_file = entry.data_file();
            if lists_no_live_files {
                return Err(CompactionError::Execution(format!(
                    "Manifest {} lists no live files but has live entry {}",
                    input.manifest_path,
                    data_file.file_path()
                )));
            }
            if !batch.range().contains(data_file.partition())? {
                continue;
            }

            let key = cluster_key(data_file.partition(), plan.clustered_fields());
            if !writers.contains_key(&key) {
                writers.insert(
                    key.clone(),
                    writer_context.new_manifest_writer(
                        ManifestContentType::Data,
                        metadata.current_schema(),
                        partition_spec,
                    )?,
                );
            }
            let writer = writers.get_mut(&key).expect("writer was just inserted");

            // Loading fills in inherited values of added entries; what is still missing
            // would be invalid in an existing entry.
            let missing = |what: &str| {
                CompactionError::Execution(format!(
                    "Entry {} in manifest {} has no {what}",
                    data_file.file_path(),
                    input.manifest_path
                ))
            };
            writer.add_existing_file(
                data_file.clone(),
                entry.snapshot_id().ok_or_else(|| missing("snapshot id"))?,
                entry
                    .sequence_number()
                    .ok_or_else(|| missing("data sequence number"))?,
                entry.file_sequence_number,
            )?;
            entries_written += 1;

            if writer.estimated_manifest_size() >= target_size {
                let sealed = writers.remove(&key).expect("writer is in the map");
                manifests.push(sealed.write_manifest_file().await?);
            }
        }
    }
    for writer in std::mem::take(&mut writers).into_values() {
        manifests.push(writer.write_manifest_file().await?);
    }

    Ok(RewrittenManifestBatch {
        batch_index: batch.index(),
        manifests,
        inputs_read,
        entries_written,
        duration: start.elapsed(),
    })
}

/// Returns whether `table` is encrypted or may be: it has an encryption manager or the
/// `encryption.key-id` property, or one of `manifests` has key metadata.
pub(super) fn is_encrypted(table: &Table, manifests: &[ManifestFile]) -> bool {
    table.encryption_manager().is_some()
        || table
            .metadata()
            .properties()
            .contains_key(TableProperties::PROPERTY_ENCRYPTION_KEY_ID)
        || manifests
            .iter()
            .any(|manifest| manifest.key_metadata.is_some())
}

/// Loads manifests on spawned tasks, at most `concurrency` ahead of the consumer, and
/// yields them in input order so output is deterministic. Dropping it aborts the loads
/// still in flight.
struct OrderedLoads {
    file_io: iceberg::io::FileIO,
    pending: std::vec::IntoIter<ManifestFile>,
    in_flight: VecDeque<(ManifestFile, AbortOnDropHandle<iceberg::Result<Manifest>>)>,
    concurrency: usize,
}

impl OrderedLoads {
    fn new(table: &Table, inputs: Vec<ManifestFile>, concurrency: usize) -> Self {
        Self {
            file_io: table.file_io().clone(),
            pending: inputs.into_iter(),
            in_flight: VecDeque::new(),
            concurrency,
        }
    }

    async fn next(&mut self) -> Result<Option<(ManifestFile, Manifest)>> {
        while self.in_flight.len() < self.concurrency {
            let Some(input) = self.pending.next() else {
                break;
            };
            let file_io = self.file_io.clone();
            let to_load = input.clone();
            let handle = tokio::spawn(async move { to_load.load_manifest(&file_io).await });
            self.in_flight
                .push_back((input, AbortOnDropHandle::new(handle)));
        }
        let Some((input, handle)) = self.in_flight.pop_front() else {
            return Ok(None);
        };
        let manifest = handle
            .await
            .map_err(|err| {
                CompactionError::Execution(format!(
                    "Manifest load task for {} failed: {err}",
                    input.manifest_path
                ))
            })?
            .map_err(CompactionError::Iceberg)?;
        Ok(Some((input, manifest)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tokio::task::JoinSet;

    use super::super::tests::{
        batch_config, data_manifests, first_field_keys, key, live_entries, live_entries_of,
        plan_of, rewrite_all, set_table_property, table_with_existing_and_added_entries,
    };
    use super::*;

    /// The UUID v7 that names every output of `rewritten`, which must be the same for all
    /// of them, and the sorted manifest counters of the outputs, which sit in the table's
    /// metadata directory as `{uuid}-m{counter}.avro`.
    fn output_naming(table: &Table, rewritten: &RewrittenManifestBatch) -> (Uuid, Vec<u64>) {
        let dir = format!("{}/", table.metadata().metadata_location().unwrap());
        let mut uuids = BTreeSet::new();
        let mut counters = Vec::new();
        for manifest in rewritten.manifests() {
            let name = manifest
                .manifest_path
                .strip_prefix(&dir)
                .and_then(|name| name.strip_suffix(".avro"))
                .unwrap_or_else(|| panic!("unexpected path {}", manifest.manifest_path));
            let (uuid, counter) = name.split_once("-m").unwrap();
            uuids.insert(Uuid::parse_str(uuid).unwrap());
            counters.push(counter.parse::<u64>().unwrap());
        }
        assert_eq!(uuids.len(), 1, "{uuids:?}");
        let uuid = uuids.into_iter().next().unwrap();
        assert_eq!(uuid.get_version_num(), 7);
        counters.sort();
        (uuid, counters)
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_writes_one_manifest_per_key_preserving_entries() {
        let (_env, table) = table_with_existing_and_added_entries().await;
        let plan = plan_of(&table, 100).await;
        assert_eq!(plan.batches().len(), 1);
        let rewritten =
            rewrite_manifest_batch(&table, &plan, &plan.batches()[0], &batch_config(100))
                .await
                .unwrap();

        assert_eq!(rewritten.batch_index(), 0);
        assert_eq!(rewritten.inputs_read(), 6);
        assert_eq!(rewritten.entries_written(), 12);

        let mut keys = Vec::new();
        for manifest in rewritten.manifests() {
            keys.push(first_field_keys(&table, manifest).await);
            assert_eq!(manifest.added_snapshot_id, plan.snapshot_id());
            assert_eq!(manifest.added_files_count, Some(0));
        }
        keys.sort();
        assert_eq!(
            keys,
            (1..=7)
                .map(|id| BTreeSet::from([key(id)]))
                .collect::<Vec<_>>()
        );

        assert_eq!(
            output_naming(&table, &rewritten).1,
            (0..7).collect::<Vec<_>>()
        );

        let before = live_entries(&table).await;
        assert_eq!(before.len(), 12);
        assert_eq!(live_entries_of(&table, rewritten.manifests()).await, before);
        // Existing entries keep the sequence number of the append that added them, not
        // the global rewrite's (3).
        let sequence_numbers: BTreeSet<_> = before.values().map(|(_, _, seq, _)| *seq).collect();
        assert_eq!(
            sequence_numbers,
            BTreeSet::from([Some(1), Some(2), Some(4), Some(5)])
        );
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_writes_only_its_range_under_unique_paths() {
        let (_env, table) = table_with_existing_and_added_entries().await;
        let config = batch_config(2);
        let plan = plan_of(&table, 2).await;
        assert_eq!(plan.batches().len(), 3);
        let rewritten = rewrite_all(&table, &plan, &config).await;

        // (inputs read, entries written, first-field keys of each output) per batch.
        let mut summary = Vec::new();
        for batch in &rewritten {
            let mut keys = Vec::new();
            for manifest in batch.manifests() {
                keys.push(first_field_keys(&table, manifest).await);
            }
            keys.sort();
            summary.push((batch.inputs_read(), batch.entries_written(), keys));
        }
        let keys = |ids: &[i32]| -> Vec<BTreeSet<String>> {
            ids.iter().map(|id| BTreeSet::from([key(*id)])).collect()
        };
        // Cut at 2 and 4; the appended manifest holding keys 1, 4 and 6 is read by all three.
        assert_eq!(summary, vec![
            (2, 2, keys(&[1])),
            (3, 5, keys(&[2, 3])),
            (3, 5, keys(&[4, 5, 6, 7])),
        ]);

        // Each batch names its manifests with its own UUID, numbered from 0.
        let naming: Vec<_> = rewritten
            .iter()
            .map(|batch| output_naming(&table, batch))
            .collect();
        assert_eq!(
            naming
                .iter()
                .map(|(_, counters)| counters.clone())
                .collect::<Vec<_>>(),
            vec![vec![0], vec![0, 1], vec![0, 1, 2, 3]]
        );
        assert_eq!(
            naming
                .iter()
                .map(|(uuid, _)| uuid)
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );

        // No file is written by two batches, and together they hold every live file.
        let all_outputs: Vec<_> = rewritten
            .iter()
            .flat_map(|batch| batch.manifests().iter().cloned())
            .collect();
        assert_eq!(
            live_entries_of(&table, &all_outputs).await,
            live_entries(&table).await
        );
    }

    /// The size a manifest writer estimates for each live entry of the table, which must
    /// be the same for all of them.
    async fn estimated_entry_size(table: &Table) -> u64 {
        let metadata = table.metadata_ref();
        let writer_context = ManifestWriterContext::new(
            metadata.metadata_location().unwrap(),
            Uuid::now_v7(),
            Arc::new(AtomicU64::new(0)),
            metadata.format_version(),
            1,
            table.file_io().clone(),
            None,
        );
        let mut sizes = BTreeSet::new();
        for manifest_file in data_manifests(table).await {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                let mut writer = writer_context
                    .new_manifest_writer(
                        ManifestContentType::Data,
                        metadata.current_schema(),
                        metadata.default_partition_spec(),
                    )
                    .unwrap();
                writer
                    .add_existing_file(entry.data_file().clone(), 1, 1, Some(1))
                    .unwrap();
                sizes.insert(writer.estimated_manifest_size());
            }
        }
        assert_eq!(sizes.len(), 1, "{sizes:?}");
        sizes.into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_rolls_over_at_the_target_manifest_size() {
        let (env, table) = table_with_existing_and_added_entries().await;
        // Every entry has the same estimated size, so a target of two entries rolls a key's
        // manifest over after every second entry.
        let target = 2 * estimated_entry_size(&table).await;
        let table = set_table_property(
            &env,
            &table,
            MANIFEST_TARGET_SIZE_BYTES,
            &target.to_string(),
        )
        .await;
        let plan = plan_of(&table, 100).await;
        let rewritten =
            rewrite_manifest_batch(&table, &plan, &plan.batches()[0], &batch_config(100))
                .await
                .unwrap();

        let mut per_manifest = Vec::new();
        for manifest in rewritten.manifests() {
            let keys = first_field_keys(&table, manifest).await;
            per_manifest.push((keys, manifest.existing_files_count));
        }
        per_manifest.sort();
        let expected_counts = [
            (1, 2),
            (2, 2),
            (3, 1),
            (3, 2),
            (4, 2),
            (5, 1),
            (6, 1),
            (7, 1),
        ];
        assert_eq!(
            per_manifest,
            expected_counts
                .iter()
                .map(|&(id, count)| (BTreeSet::from([key(id)]), Some(count)))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            rewritten
                .manifests()
                .iter()
                .map(|m| &m.manifest_path)
                .collect::<BTreeSet<_>>()
                .len(),
            8
        );
        assert_eq!(
            live_entries_of(&table, rewritten.manifests()).await,
            live_entries(&table).await
        );
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_run_again_never_overwrites_an_earlier_run() {
        let (env, table) = table_with_existing_and_added_entries().await;
        let plan = plan_of(&table, 100).await;
        let first = rewrite_manifest_batch(&table, &plan, &plan.batches()[0], &batch_config(100))
            .await
            .unwrap();

        // A second run of the same batch that splits manifests differently, as after a
        // change of the target size between a retried job's attempts.
        let target = 2 * estimated_entry_size(&table).await;
        let resized = set_table_property(
            &env,
            &table,
            MANIFEST_TARGET_SIZE_BYTES,
            &target.to_string(),
        )
        .await;
        let second =
            rewrite_manifest_batch(&resized, &plan, &plan.batches()[0], &batch_config(100))
                .await
                .unwrap();

        let (first_uuid, first_counters) = output_naming(&table, &first);
        let (second_uuid, second_counters) = output_naming(&table, &second);
        assert_ne!(first_uuid, second_uuid);
        assert_eq!(first_counters, (0..7).collect::<Vec<_>>());
        assert_eq!(second_counters, (0..8).collect::<Vec<_>>());

        // The first run's manifests still hold exactly what its result describes.
        let before = live_entries(&table).await;
        assert_eq!(live_entries_of(&table, first.manifests()).await, before);
        assert_eq!(live_entries_of(&table, second.manifests()).await, before);
        for manifest in first.manifests() {
            let length = table
                .file_io()
                .new_input(&manifest.manifest_path)
                .unwrap()
                .metadata()
                .await
                .unwrap()
                .size;
            assert_eq!(length, manifest.manifest_length as u64);
        }
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_fails_on_live_entries_in_an_input_listing_none() {
        let (_env, table) = table_with_existing_and_added_entries().await;
        let mut plan = plan_of(&table, 100).await;
        let input = &mut plan.batches[0].inputs[0];
        input.added_files_count = Some(0);
        input.existing_files_count = Some(0);
        let path = input.manifest_path.clone();
        let first_live = input
            .load_manifest(table.file_io())
            .await
            .unwrap()
            .entries()
            .iter()
            .find(|entry| entry.is_alive())
            .unwrap()
            .data_file()
            .file_path()
            .to_owned();

        let err = rewrite_manifest_batch(&table, &plan, &plan.batches()[0], &batch_config(100))
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "Execution failed: Manifest {path} lists no live files but has live entry {first_live}"
            )
        );
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_rejects_bad_arguments_and_unreadable_inputs() {
        let (_env, table) = table_with_existing_and_added_entries().await;
        let plan = plan_of(&table, 2).await;
        let batch = &plan.batches()[0];

        // A batch of another plan: same index, different inputs.
        let other_plan = plan_of(&table, 3).await;
        let err = rewrite_manifest_batch(&table, &plan, &other_plan.batches()[1], &batch_config(2))
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Execution failed: Batch 1 is not part of the plan"
        );
        let mut out_of_range = batch.clone();
        out_of_range.index = 99;
        let err = rewrite_manifest_batch(&table, &plan, &out_of_range, &batch_config(2))
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Execution failed: Batch 99 is not part of the plan"
        );

        let mut missing = plan.clone();
        missing.batches[0].inputs[0].manifest_path = "missing.avro".to_owned();
        let err = rewrite_manifest_batch(&table, &missing, &missing.batches()[0], &batch_config(2))
            .await
            .unwrap_err();
        assert!(matches!(err, CompactionError::Iceberg(_)), "{err:?}");
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_runs_on_spawned_tasks() {
        let (_env, table) = table_with_existing_and_added_entries().await;
        let config = batch_config(2);
        let plan = plan_of(&table, 2).await;

        // Callers spawn each batch with owned clones.
        let mut tasks = JoinSet::new();
        for batch in plan.batches() {
            let (table, plan, batch, config) =
                (table.clone(), plan.clone(), batch.clone(), config.clone());
            tasks
                .spawn(async move { rewrite_manifest_batch(&table, &plan, &batch, &config).await });
        }
        let mut entries = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            let rewritten = joined.unwrap().unwrap();
            entries.push((rewritten.batch_index(), rewritten.entries_written()));
        }
        entries.sort();
        assert_eq!(entries, vec![(0, 2), (1, 5), (2, 5)]);
    }
}
