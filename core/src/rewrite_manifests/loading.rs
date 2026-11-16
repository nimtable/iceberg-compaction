//! Fallback no-op check that loads manifest files and inspects their live entries.

use std::collections::HashMap;

use iceberg::spec::{Manifest, ManifestFile, Struct};
use iceberg::table::Table;
use tokio::task::JoinSet;

use super::classify::is_cross_snapshot_duplicate;
use crate::{CompactionError, Result};

/// Cluster key of a loaded entry's partition: its first `clustered_fields` values.
pub(super) fn cluster_key(partition: &Struct, clustered_fields: usize) -> String {
    let fields = partition.fields();
    format!("{:?}", &fields[..fields.len().min(clustered_fields)])
}

/// Loads every data manifest and checks the live entries' cluster keys. Both checks are
/// order-independent, so manifests are loaded in parallel on spawned tasks, at most
/// `load_concurrency` at a time, and processed in completion order.
///
/// `load_concurrency` must be at least 1.
pub(super) async fn needs_rewrite_by_loading(
    table: &Table,
    data_manifests: Vec<ManifestFile>,
    clustered_fields: usize,
    load_concurrency: usize,
) -> Result<bool> {
    let mut pending = data_manifests.into_iter();

    // Each task owns its `ManifestFile` and `FileIO` clone so the future stays `Send`
    // for callers that spawn it. Dropping the `JoinSet` on an early return aborts the
    // loads still in flight.
    type Load = iceberg::Result<(ManifestFile, Manifest)>;
    let mut loads: JoinSet<Load> = JoinSet::new();
    let spawn_next = |loads: &mut JoinSet<Load>, pending: &mut std::vec::IntoIter<ManifestFile>| {
        if let Some(manifest_file) = pending.next() {
            let file_io = table.file_io().clone();
            loads.spawn(async move {
                let manifest = manifest_file.load_manifest(&file_io).await?;
                Ok((manifest_file, manifest))
            });
        }
    };
    for _ in 0..load_concurrency {
        spawn_next(&mut loads, &mut pending);
    }

    // Track which snapshot added each `(spec_id, partition key)`, so we can detect
    // cross-manifest duplication. The spec id matches the summary path.
    let mut key_snapshots: HashMap<(i32, String), i64> = HashMap::new();

    while let Some(joined) = loads.join_next().await {
        spawn_next(&mut loads, &mut pending);

        let (manifest_file, manifest) = joined
            .map_err(|err| {
                CompactionError::Execution(format!(
                    "Manifest load task failed during no-op check: {err}"
                ))
            })?
            .map_err(CompactionError::Iceberg)?;

        // Intra-manifest mixing: live entries for more than one partition key.
        let mut manifest_key: Option<String> = None;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let key = cluster_key(entry.data_file().partition(), clustered_fields);
            match &manifest_key {
                None => manifest_key = Some(key),
                Some(existing) if *existing != key => return Ok(true),
                Some(_) => {}
            }
        }

        // Cross-manifest duplication.
        if let Some(key) = manifest_key {
            let key = (manifest_file.partition_spec_id, key);
            if is_cross_snapshot_duplicate(&mut key_snapshots, key, &manifest_file) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}
