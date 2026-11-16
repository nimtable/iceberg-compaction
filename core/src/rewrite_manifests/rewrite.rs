//! Rewrites the manifests of one planned batch.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use iceberg::spec::{
    Datum, Literal, Manifest, ManifestContentType, ManifestFile, ManifestWriter, PrimitiveLiteral,
    PrimitiveType, TableProperties, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{
    MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT, ManifestWriterContext,
};
use tokio::sync::Semaphore;
use tokio_util::task::AbortOnDropHandle;
use uuid::Uuid;

use super::loading::cluster_key;
use super::sweep::{Extent, decode_extent};
use super::types::{
    BatchedRewriteManifestsConfig, ManifestBatch, ManifestRewritePlan, RewrittenManifestBatch,
    compare, order,
};
use crate::common::available_parallelism;
use crate::{CompactionError, Result};

/// Rewrites one batch of `plan`: loads its input manifests and writes the live entries in
/// the batch's range into new manifests, one cluster key per manifest.
///
/// Entries keep their original snapshot id and sequence numbers. A key's manifest is
/// rolled over once it reaches the table's `commit.manifest.target-size-bytes` (8 MiB if
/// unset or malformed), like the global rewrite.
///
/// Inputs are read in ascending order of the first-field lower bound in their
/// manifest-list summary, those without a usable bound first. A key's manifest is written
/// as soon as no remaining input can hold the key, which is once every unread input's
/// lower bound is above the key's first-field value. So the manifests open in memory are
/// those of the keys that remaining inputs can still hold, not those of every key of the
/// batch. Keys whose first-field value is null stay open until the last input.
///
/// The lower bounds only decide when a manifest is written. If an input holds an entry
/// below its recorded lower bound, the entry's key may already have been written, and the
/// entry then starts another manifest for that key. Every entry is still written exactly
/// once, and every manifest still holds a single key.
///
/// The manifests are written but not committed; pass the result of every batch to
/// [`commit_manifest_batches`](super::commit_manifest_batches). Batches are independent,
/// so callers can rewrite them concurrently. Every call names its manifests with a fresh
/// UUID, so running a batch again, even after a commit that used an earlier run's
/// outputs, never overwrites a manifest another run wrote. A failed or repeated batch may
/// leave unreferenced manifest files behind.
///
/// At most `config.load_concurrency` inputs are loaded at once, and at most one output
/// per available core is written at once, on spawned tasks. Outputs are returned in the
/// order their writes finish.
///
/// Encrypted tables are not supported: the manifests would be written in plaintext. A
/// table counts as encrypted if it has the `encryption.key-id` property, an encryption
/// manager, or an input with key metadata. [`plan_manifest_batches`](super::plan_manifest_batches)
/// returns [`PlanOutcome::Unsplittable`](super::PlanOutcome::Unsplittable) for such tables.
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
    config.validate()?;
    let load_permits = Arc::new(Semaphore::new(config.load_concurrency));
    rewrite_manifest_batch_sharing_loads(table, plan, batch, config, load_permits).await
}

/// [`rewrite_manifest_batch`] with the manifest loads limited by `load_permits` instead
/// of `config.load_concurrency`: a load holds one permit while it reads and decodes.
///
/// Callers that run several batches at once can give them all the same semaphore. The
/// batches then share its permits, so a batch that is writing its outputs, or has
/// finished, leaves its share of the loads to the others, which a fixed
/// `load_concurrency` per batch can't do. `config.load_concurrency` still bounds how many
/// loads one batch requests at once, and so how many loaded manifests it holds.
///
/// # Errors
///
/// As [`rewrite_manifest_batch`], and [`CompactionError::Execution`] if `load_permits`
/// is closed.
pub async fn rewrite_manifest_batch_sharing_loads(
    table: &Table,
    plan: &ManifestRewritePlan,
    batch: &ManifestBatch,
    config: &BatchedRewriteManifestsConfig,
    load_permits: Arc<Semaphore>,
) -> Result<RewrittenManifestBatch> {
    rewrite_batch(
        table,
        plan,
        batch,
        config,
        load_permits,
        available_parallelism().get(),
    )
    .await
}

/// [`rewrite_manifest_batch`] with at most `write_concurrency` outputs written at once.
async fn rewrite_batch(
    table: &Table,
    plan: &ManifestRewritePlan,
    batch: &ManifestBatch,
    config: &BatchedRewriteManifestsConfig,
    load_permits: Arc<Semaphore>,
    write_concurrency: usize,
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

    // Read the inputs in ascending order of their smallest first-field value. Once the
    // next input starts at `v`, no later input holds a key below `v`, so the writers of
    // those keys are complete and can be written out while the reads continue.
    let first_field = match partition_spec
        .partition_type(metadata.current_schema())
        .ok()
        .and_then(|ty| ty.fields().first().map(|f| f.field_type.as_ref().clone()))
    {
        Some(Type::Primitive(ty)) => Some(ty),
        _ => None,
    };
    let (inputs, mut frontiers) = read_order(batch.inputs(), first_field.as_ref());
    let mut loads = OrderedLoads::new(table, inputs, config.load_concurrency, load_permits);

    // Each writer with its key's first-field value (`None` if null or not a primitive).
    let mut writers: BTreeMap<String, (Option<PrimitiveLiteral>, ManifestWriter)> = BTreeMap::new();
    let mut manifests = Vec::new();
    let write_concurrency = write_concurrency.max(1);
    let mut writes_in_flight = FuturesUnordered::new();
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
            // A key written out early gets a new writer, and so another manifest, if an
            // input's lower bound was wrong and it still holds entries of the key.
            if !writers.contains_key(&key) {
                let first_value = match data_file.partition().fields().first() {
                    Some(Some(Literal::Primitive(value))) => Some(value.clone()),
                    _ => None,
                };
                writers.insert(
                    key.clone(),
                    (
                        first_value,
                        writer_context.new_manifest_writer(
                            ManifestContentType::Data,
                            metadata.current_schema(),
                            partition_spec,
                        )?,
                    ),
                );
            }
            let (_, writer) = writers.get_mut(&key).expect("writer was just inserted");

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
                let (_, sealed) = writers
                    .remove(&key)
                    .expect("writer over the target size is in the map");
                spawn_bounded_write(
                    sealed,
                    &mut writes_in_flight,
                    write_concurrency,
                    &mut manifests,
                )
                .await?;
            }
        }
        // `frontiers` yields, after each input, the smallest value a later input can hold,
        // only when it has grown.
        if let Some(Some(frontier)) = frontiers.next() {
            let complete: Vec<String> = writers
                .iter()
                .filter(|(_, (value, _))| {
                    value.as_ref().is_some_and(|value| {
                        matches!(compare(value, &frontier), Ok(std::cmp::Ordering::Less))
                    })
                })
                .map(|(key, _)| key.clone())
                .collect();
            for key in complete {
                let (_, sealed) = writers
                    .remove(&key)
                    .expect("writer below the frontier is in the map");
                spawn_bounded_write(
                    sealed,
                    &mut writes_in_flight,
                    write_concurrency,
                    &mut manifests,
                )
                .await?;
            }
        }
    }
    for (_, writer) in std::mem::take(&mut writers).into_values() {
        spawn_bounded_write(
            writer,
            &mut writes_in_flight,
            write_concurrency,
            &mut manifests,
        )
        .await?;
    }
    while let Some(written) = writes_in_flight.next().await {
        manifests.push(join_write(written)?);
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

/// The order a batch reads `inputs` in, and what that order proves after each input.
///
/// Inputs are sorted by the smallest first-field value their summary allows, keeping
/// manifest-list order between equals; inputs whose summary gives no such value (no
/// usable bounds, only nulls, or `first_field` is `None`) come first. The iterator yields
/// one item per input: `Some(v)` if every later input only holds non-null first-field
/// values of at least `v` and `v` is above what the earlier items proved, else `None`.
fn read_order(
    inputs: &[ManifestFile],
    first_field: Option<&PrimitiveType>,
) -> (Vec<ManifestFile>, std::vec::IntoIter<Option<Datum>>) {
    let mut ordered: Vec<(Option<Datum>, &ManifestFile)> = inputs
        .iter()
        .map(|input| {
            let lower = first_field.and_then(|ty| match decode_extent(input, ty) {
                Extent::Known {
                    values: Some((lower, _)),
                    ..
                } => Some(lower),
                _ => None,
            });
            (lower, input)
        })
        .collect();
    ordered.sort_by(|(a, _), (b, _)| match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => order(a.literal(), b.literal()),
    });

    let mut frontiers = Vec::with_capacity(ordered.len());
    let mut proved: Option<&Datum> = None;
    for position in 0..ordered.len() {
        let next = ordered
            .get(position + 1)
            .and_then(|(next, _)| next.as_ref());
        let grown = match (next, proved) {
            (Some(next), Some(proved)) => order(next.literal(), proved.literal()).is_gt(),
            // The first input with a known lower bound ends the unordered prefix.
            (Some(_), None) => true,
            (None, _) => false,
        };
        if grown {
            proved = next;
            frontiers.push(next.cloned());
        } else {
            frontiers.push(None);
        }
    }
    (
        ordered
            .into_iter()
            .map(|(_, input)| input.clone())
            .collect(),
        frontiers.into_iter(),
    )
}

/// Loads manifests on spawned tasks and yields them in the order given so output is
/// deterministic. At most `window` manifests are requested ahead of the consumer, which
/// bounds the loaded manifests held in memory, and each load holds a permit of `permits`
/// while it runs. Dropping it aborts the loads still in flight.
struct OrderedLoads {
    file_io: iceberg::io::FileIO,
    pending: std::vec::IntoIter<ManifestFile>,
    in_flight: VecDeque<(ManifestFile, AbortOnDropHandle<Result<Manifest>>)>,
    window: usize,
    permits: Arc<Semaphore>,
}

impl OrderedLoads {
    fn new(
        table: &Table,
        inputs: Vec<ManifestFile>,
        window: usize,
        permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            file_io: table.file_io().clone(),
            pending: inputs.into_iter(),
            in_flight: VecDeque::new(),
            window,
            permits,
        }
    }

    async fn next(&mut self) -> Result<Option<(ManifestFile, Manifest)>> {
        while self.in_flight.len() < self.window {
            let Some(input) = self.pending.next() else {
                break;
            };
            let file_io = self.file_io.clone();
            let to_load = input.clone();
            let permits = Arc::clone(&self.permits);
            let handle = tokio::spawn(async move {
                let _permit = permits.acquire_owned().await.map_err(|_| {
                    CompactionError::Execution("The manifest load permits were closed".to_owned())
                })?;
                to_load
                    .load_manifest(&file_io)
                    .await
                    .map_err(CompactionError::Iceberg)
            });
            self.in_flight
                .push_back((input, AbortOnDropHandle::new(handle)));
        }
        let Some((input, handle)) = self.in_flight.pop_front() else {
            return Ok(None);
        };
        let manifest = handle.await.map_err(|err| {
            CompactionError::Execution(format!(
                "Manifest load task for {} failed: {err}",
                input.manifest_path
            ))
        })??;
        Ok(Some((input, manifest)))
    }
}

/// A spawned manifest write; aborted if dropped.
type WriteHandle = AbortOnDropHandle<iceberg::Result<ManifestFile>>;

/// Spawns `writer`'s encode and upload, first waiting for one in-flight write to finish
/// if `write_concurrency` are already running. Writes abort on drop, so an early error
/// doesn't keep uploading orphan manifests.
async fn spawn_bounded_write(
    writer: ManifestWriter,
    writes_in_flight: &mut FuturesUnordered<WriteHandle>,
    write_concurrency: usize,
    written: &mut Vec<ManifestFile>,
) -> Result<()> {
    if writes_in_flight.len() >= write_concurrency
        && let Some(done) = writes_in_flight.next().await
    {
        written.push(join_write(done)?);
    }
    writes_in_flight.push(AbortOnDropHandle::new(tokio::spawn(
        writer.write_manifest_file(),
    )));
    Ok(())
}

fn join_write(
    joined: std::result::Result<iceberg::Result<ManifestFile>, tokio::task::JoinError>,
) -> Result<ManifestFile> {
    joined
        .map_err(|err| CompactionError::Execution(format!("Manifest write task failed: {err}")))?
        .map_err(CompactionError::Iceberg)
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
        let expected: Vec<_> = expected_counts
            .iter()
            .map(|&(id, count)| (BTreeSet::from([key(id)]), Some(count)))
            .collect();
        // One write at a time, fewer than the 8 outputs, and more than the outputs.
        for write_concurrency in [1, 3, 16] {
            let rewritten = rewrite_batch(
                &table,
                &plan,
                &plan.batches()[0],
                &batch_config(100),
                Arc::new(Semaphore::new(100)),
                write_concurrency,
            )
            .await
            .unwrap();

            // Outputs come back in write-completion order.
            let mut per_manifest = Vec::new();
            for manifest in rewritten.manifests() {
                let keys = first_field_keys(&table, manifest).await;
                per_manifest.push((keys, manifest.existing_files_count));
            }
            per_manifest.sort();
            assert_eq!(
                per_manifest, expected,
                "write_concurrency={write_concurrency}"
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
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_fails_when_an_output_cannot_be_written() {
        let (_env, table) = table_with_existing_and_added_entries().await;
        let plan = plan_of(&table, 100).await;

        // The test catalog's storage is in memory, which never fails a write. Copy the
        // inputs to the local filesystem at the same paths and read them from there.
        for input in plan.batches()[0].inputs() {
            let bytes = table
                .file_io()
                .new_input(&input.manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            let path = std::path::Path::new(&input.manifest_path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
        // Outputs go under `write.metadata.path`; a directory can't be created under a
        // regular file, so every write fails while the inputs still load.
        let dir = tempfile::TempDir::new().unwrap();
        let blocker = dir.path().join("not-a-directory");
        std::fs::write(&blocker, b"").unwrap();
        let mut properties = table.metadata().properties().clone();
        properties.insert(
            TableProperties::PROPERTY_WRITE_METADATA_PATH.to_owned(),
            blocker.join("metadata").to_str().unwrap().to_owned(),
        );
        let metadata =
            iceberg::spec::TableMetadataBuilder::new_from_metadata(table.metadata().clone(), None)
                .set_properties(properties)
                .unwrap()
                .build()
                .unwrap()
                .metadata;
        let unwritable = Table::builder()
            .metadata(metadata)
            .identifier(table.identifier().clone())
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata_location(table.metadata_location().unwrap())
            .runtime(iceberg::Runtime::current())
            .build()
            .unwrap();

        for write_concurrency in [1, 16] {
            let err = rewrite_batch(
                &unwritable,
                &plan,
                &plan.batches()[0],
                &batch_config(100),
                Arc::new(Semaphore::new(100)),
                write_concurrency,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(&err, CompactionError::Iceberg(e) if e.to_string().contains("Failed to create directory")),
                "{err:?}"
            );
        }
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

    /// The first-field lower bound in `input`'s summary, as an `i32`.
    fn int_lower_bound(input: &ManifestFile) -> Option<i32> {
        match decode_extent(input, &PrimitiveType::Int) {
            Extent::Known {
                values: Some((lower, _)),
                ..
            } => match lower.literal() {
                PrimitiveLiteral::Int(value) => Some(*value),
                other => panic!("unexpected lower bound {other:?}"),
            },
            _ => None,
        }
    }

    /// Sets the first-field lower bound in `input`'s summary to `lower`.
    fn set_int_lower_bound(input: &mut ManifestFile, lower: i32) {
        let summary = &mut input.partitions.as_mut().unwrap()[0];
        summary.lower_bound = Some(iceberg::spec::ByteBuf::from(lower.to_le_bytes().to_vec()));
    }

    #[tokio::test]
    async fn read_order_sorts_by_lower_bound_and_yields_growing_frontiers() {
        let (_env, table) = table_with_existing_and_added_entries().await;
        let plan = plan_of(&table, 100).await;
        let mut inputs = plan.batches()[0].inputs().to_vec();
        // The global rewrite wrote keys 1, 2, 3 and 5; the appends wrote 1, 4, 6 and 4, 7.
        let mut lower_bounds: Vec<_> = inputs.iter().map(int_lower_bound).collect();
        lower_bounds.sort();
        assert_eq!(lower_bounds, [1, 1, 2, 3, 4, 5].map(Some));

        // An input without a usable bound comes first.
        let key_5 = inputs
            .iter()
            .position(|input| int_lower_bound(input) == Some(5))
            .unwrap();
        inputs[key_5].partitions = None;
        let unbounded = inputs[key_5].manifest_path.clone();
        let list_position = |path: &str| {
            inputs
                .iter()
                .position(|input| input.manifest_path == path)
                .unwrap()
        };

        let (ordered, frontiers) = read_order(&inputs, Some(&PrimitiveType::Int));
        assert_eq!(ordered[0].manifest_path, unbounded);
        assert_eq!(
            ordered.iter().map(int_lower_bound).collect::<Vec<_>>(),
            vec![None, Some(1), Some(1), Some(2), Some(3), Some(4)]
        );
        // The two inputs starting at 1 keep their manifest-list order.
        assert!(
            list_position(&ordered[1].manifest_path) < list_position(&ordered[2].manifest_path)
        );
        // A frontier is yielded only when the next input's lower bound grows.
        assert_eq!(frontiers.collect::<Vec<_>>(), vec![
            Some(Datum::int(1)),
            None,
            Some(Datum::int(2)),
            Some(Datum::int(3)),
            Some(Datum::int(4)),
            None,
        ]);

        // Without a first-field type nothing is known: manifest-list order, no frontier.
        let (ordered, frontiers) = read_order(&inputs, None);
        assert_eq!(
            ordered
                .iter()
                .map(|input| &input.manifest_path)
                .collect::<Vec<_>>(),
            inputs
                .iter()
                .map(|input| &input.manifest_path)
                .collect::<Vec<_>>()
        );
        assert_eq!(frontiers.collect::<Vec<_>>(), vec![None; inputs.len()]);
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_writes_keys_once_past_the_frontier() {
        let (_env, table) = table_with_existing_and_added_entries().await;
        let mut plan = plan_of(&table, 100).await;
        // Claim that the append holding keys 4 and 7 starts at 6. It is then read last, and
        // key 4 of the other append is written out once the frontier reaches 6, so the
        // append's key-4 entry starts a second manifest for that key.
        let append = plan.batches[0]
            .inputs
            .iter_mut()
            .find(|input| int_lower_bound(input) == Some(4))
            .unwrap();
        set_int_lower_bound(append, 6);

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
        let expected: Vec<_> = [
            (1, 2),
            (2, 2),
            (3, 3),
            (4, 1),
            (4, 1),
            (5, 1),
            (6, 1),
            (7, 1),
        ]
        .iter()
        .map(|&(id, count)| (BTreeSet::from([key(id)]), Some(count)))
        .collect();
        assert_eq!(per_manifest, expected);
        assert_eq!(
            live_entries_of(&table, rewritten.manifests()).await,
            live_entries(&table).await
        );
    }

    #[tokio::test]
    async fn rewrite_manifest_batch_sharing_loads_draws_on_the_given_permits() {
        let (_env, table) = table_with_existing_and_added_entries().await;
        let config = batch_config(2);
        let plan = plan_of(&table, 2).await;

        // Concurrent batches sharing one permit all finish and give the permit back.
        let permits = Arc::new(Semaphore::new(1));
        let mut tasks = JoinSet::new();
        for batch in plan.batches() {
            let (table, plan, batch, config, permits) = (
                table.clone(),
                plan.clone(),
                batch.clone(),
                config.clone(),
                Arc::clone(&permits),
            );
            tasks.spawn(async move {
                rewrite_manifest_batch_sharing_loads(&table, &plan, &batch, &config, permits).await
            });
        }
        let mut entries = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            let rewritten = joined.unwrap().unwrap();
            entries.push((rewritten.batch_index(), rewritten.entries_written()));
        }
        entries.sort();
        assert_eq!(entries, vec![(0, 2), (1, 5), (2, 5)]);
        assert_eq!(permits.available_permits(), 1);

        // Loads wait on the given permits, so closed permits fail the batch.
        permits.close();
        let err = rewrite_manifest_batch_sharing_loads(
            &table,
            &plan,
            &plan.batches()[0],
            &config,
            permits,
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Execution failed: The manifest load permits were closed"
        );
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
