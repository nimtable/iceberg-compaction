//! Splits the data manifests of a branch into batches that can be rewritten independently.
//!
//! Batches are ranges of the first partition field's value, so every cluster key (which
//! always starts with that field) falls into exactly one batch. A manifest holding keys
//! of several ranges is read by each of those batches; each batch writes only the entries
//! in its range.

use std::collections::{BTreeMap, HashMap, HashSet};

use iceberg::spec::{
    Datum, ManifestContentType, ManifestFile, PrimitiveLiteral, PrimitiveType, Type,
};
use iceberg::table::Table;

use super::classify::{
    SummaryClass, SummaryKey, classify_from_summary, is_cross_snapshot_duplicate,
};
use super::rewrite::is_encrypted;
use super::sweep::{Extent, Limits, OverlapIndex, ValueExtent, choose_cuts, decode_extent};
use super::types::{
    BatchedRewriteManifestsConfig, FirstFieldRange, ManifestBatch, ManifestRewritePlan, order,
};
use crate::{CompactionError, Result};

impl BatchedRewriteManifestsConfig {
    pub(super) fn validate(&self) -> Result<()> {
        let zero = if self.max_batch_entries == 0 {
            "max_batch_entries"
        } else if self.max_batch_manifests == 0 {
            "max_batch_manifests"
        } else if self.load_concurrency == 0 {
            "load_concurrency"
        } else if self
            .max_read_amplification
            .is_some_and(|max| !(max >= 1.0 && max.is_finite()))
        {
            "max_read_amplification"
        } else {
            return Ok(());
        };
        Err(CompactionError::Config(format!(
            "{zero} must be at least 1"
        )))
    }

    fn limits(&self) -> Limits {
        // Ranges are cut at 80% so a batch rarely ends up just over the limit.
        Limits {
            target_entries: (u128::from(self.max_batch_entries) * 4 / 5) as u64,
            max_manifests: self.max_batch_manifests,
        }
    }
}

/// Plans a rewrite of the data manifests of `branch`, clustered by the first
/// `clustered_fields` partition fields, as independent batches.
///
/// Only manifests that need rewriting are read: those with entries for several cluster
/// keys, those sharing a key with a manifest from another snapshot (the rule of
/// [`needs_manifest_rewrite`](super::needs_manifest_rewrite)), those whose summary is
/// inconclusive, and single-key manifests whose first-field range one of those overlaps.
/// Delete manifests are never planned, and batches never mix partition specs.
///
/// Each spec's first-field values are cut into ranges, preferring cuts that few manifests
/// straddle, so that a batch is planned around about
/// [`max_batch_manifests`](BatchedRewriteManifestsConfig::max_batch_manifests) manifests
/// starting in its range and under about 80% of
/// [`max_batch_entries`](BatchedRewriteManifestsConfig::max_batch_entries) entries to
/// write. These are targets, not caps:
///
/// - A manifest weighs its added plus existing file count. Only the share of it a range
///   is estimated to hold counts towards that range: if the first field has a numeric scale (integers, dates, times,
///   timestamps, decimals, floating point), its weight is spread evenly over its
///   first-field bounds; otherwise it all counts where the manifest starts. Entries that
///   cluster in part of a manifest's bounds can put a batch over the target.
/// - A manifest holding values of several ranges is read by each of them, but counts
///   towards the manifest target only where it starts.
/// - A manifest without usable first-field bounds is read by every batch of its spec and
///   doesn't count, since splitting can't reduce it.
/// - Manifests without live entries are added to one batch of their spec so the commit
///   drops them, without counting either. A spec with nothing else to rewrite gets a
///   batch of only those, unless the plan has no other batches.
/// - A single manifest over the targets, or a run of manifests with the same lower bound,
///   which no cut can separate, forms a batch of its own.
///
/// Null values form their own batch.
///
/// The plan reserves the snapshot id of the rewrite's commit.
///
/// Returns `None` if the rewrite can't usefully or safely be split, so callers should fall
/// back to a global rewrite:
///
/// - the branch has no snapshot (a global rewrite then does nothing);
/// - `clustered_fields` is `0`;
/// - the live manifests together show at most one first-field value, as in an
///   unpartitioned table, or one whose first field is always null. Nulls count as a value,
///   and so do all manifests of a spec whose bounds are unusable;
/// - a partition spec with manifests to rewrite has a first field whose type can't be
///   resolved against the table's current schema, for example because its source column
///   was dropped. The batches' writers need that type, so every such batch would fail;
/// - a data manifest doesn't list its added or existing file count, as some V1 manifest
///   lists don't. The commit verifies each batch against those counts, so every batch
///   reading the manifest would be dropped;
/// - the table is encrypted, by the `encryption.key-id` table property or by a manifest
///   with key metadata, which batches can't write (see [`rewrite_manifest_batch`](super::rewrite_manifest_batch));
/// - the batches would read more manifests than
///   [`max_read_amplification`](BatchedRewriteManifestsConfig::max_read_amplification)
///   allows, as when most manifests span most of the first field's values.
///
/// If nothing needs rewriting the plan has no batches; call
/// [`needs_manifest_rewrite`](super::needs_manifest_rewrite) first to avoid planning.
///
/// # Errors
///
/// Returns [`CompactionError::Config`] if a limit in `config` is `0`, its
/// `max_read_amplification` is below 1 or not finite, or `branch` is a tag, and
/// [`CompactionError::Iceberg`] if the manifest list cannot be read.
pub async fn plan_manifest_batches(
    table: &Table,
    branch: &str,
    clustered_fields: usize,
    config: &BatchedRewriteManifestsConfig,
) -> Result<Option<ManifestRewritePlan>> {
    config.validate()?;

    let metadata = table.metadata_ref();
    let Some(snapshot) = metadata.snapshot_for_ref(branch) else {
        return Ok(None);
    };
    if metadata
        .refs()
        .get(branch)
        .is_some_and(|snapshot_ref| !snapshot_ref.is_branch())
    {
        return Err(CompactionError::Config(format!(
            "{branch} is a tag, not a branch; a rewrite can only be committed to a branch"
        )));
    }
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &metadata)
        .await?;
    if is_encrypted(table, manifest_list.entries()) {
        return Ok(None);
    }

    let first_fields = metadata
        .partition_specs_iter()
        .map(|spec| {
            let first_field = if spec.fields().is_empty() {
                FirstField::Unpartitioned
            } else {
                match spec
                    .partition_type(metadata.current_schema())
                    .ok()
                    .and_then(|ty| ty.fields().first().map(|f| f.field_type.as_ref().clone()))
                {
                    Some(Type::Primitive(ty)) => FirstField::Typed(ty),
                    _ => FirstField::Unknown,
                }
            };
            (spec.spec_id(), first_field)
        })
        .collect();

    let Some(batches) = plan_batches(
        manifest_list.entries(),
        &first_fields,
        clustered_fields,
        config,
    ) else {
        return Ok(None);
    };
    Ok(Some(ManifestRewritePlan {
        branch: branch.to_owned(),
        base_snapshot_id: snapshot.snapshot_id(),
        clustered_fields,
        snapshot_id: generate_unique_snapshot_id(|id| {
            metadata.snapshots().any(|s| s.snapshot_id() == id)
        }),
        batches,
    }))
}

/// A random non-negative id for which `is_taken` is false, like iceberg-rust's
/// snapshot id generation.
fn generate_unique_snapshot_id(is_taken: impl Fn(i64) -> bool) -> i64 {
    generate_unique_id(is_taken, || (rand::random::<u64>() >> 1) as i64)
}

fn generate_unique_id(is_taken: impl Fn(i64) -> bool, mut next: impl FnMut() -> i64) -> i64 {
    loop {
        let id = next();
        if !is_taken(id) {
            return id;
        }
    }
}

/// How a partition spec's first field can be read from summaries.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum FirstField {
    /// No partition fields.
    Unpartitioned,
    /// Values of this partition-result type.
    Typed(PrimitiveType),
    /// The type couldn't be resolved, so summaries can't be decoded and a batch's writers
    /// can't be created.
    Unknown,
}

/// A live data manifest being planned.
struct Candidate {
    index: usize,
    extent: Extent,
    weight: u64,
}

/// One distinct first-field value of a spec, for deciding whether the table can be split.
#[derive(Hash, PartialEq, Eq)]
enum Point {
    Unknown,
    Null,
    Value(PrimitiveLiteral),
}

/// The pure core of [`plan_manifest_batches`]; `None` if the manifests can't be split.
pub(super) fn plan_batches(
    manifests: &[ManifestFile],
    first_fields: &HashMap<i32, FirstField>,
    clustered_fields: usize,
    config: &BatchedRewriteManifestsConfig,
) -> Option<Vec<ManifestBatch>> {
    if clustered_fields == 0 {
        return None;
    }

    let mut no_live: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
    let mut live: Vec<(Candidate, SummaryClass)> = Vec::new();
    for (index, manifest) in manifests.iter().enumerate() {
        if manifest.content != ManifestContentType::Data {
            continue;
        }
        // The commit checks each group's written entries against these counts.
        let (Some(added), Some(existing)) =
            (manifest.added_files_count, manifest.existing_files_count)
        else {
            return None;
        };
        match classify_from_summary(manifest, clustered_fields) {
            SummaryClass::NoLiveEntries => no_live
                .entry(manifest.partition_spec_id)
                .or_default()
                .push(index),
            class => {
                let extent = match first_fields.get(&manifest.partition_spec_id) {
                    Some(FirstField::Typed(ty)) => decode_extent(manifest, ty),
                    _ => Extent::Wide,
                };
                let weight = u64::from(added) + u64::from(existing);
                live.push((
                    Candidate {
                        index,
                        extent,
                        weight,
                    },
                    class,
                ));
            }
        }
    }

    if distinct_points(manifests, &live) <= 1 {
        return None;
    }

    // A key shared across snapshots makes all of its manifests dirty, not just the later ones.
    let mut key_snapshots: HashMap<SummaryKey, i64> = HashMap::new();
    let mut duplicated_keys: HashSet<SummaryKey> = HashSet::new();
    for (candidate, class) in &live {
        if let SummaryClass::SingleKey(key) = class
            && is_cross_snapshot_duplicate(
                &mut key_snapshots,
                key.clone(),
                &manifests[candidate.index],
            )
        {
            duplicated_keys.insert(key.clone());
        }
    }

    let mut specs: BTreeMap<i32, (Vec<Candidate>, Vec<Candidate>)> = BTreeMap::new();
    for (candidate, class) in live {
        let dirty = match &class {
            SummaryClass::SingleKey(key) => duplicated_keys.contains(key),
            _ => true,
        };
        let (dirty_set, clean_set) = specs
            .entry(manifests[candidate.index].partition_spec_id)
            .or_default();
        if dirty {
            dirty_set.push(candidate);
        } else {
            clean_set.push(candidate);
        }
    }

    let mut batches = Vec::new();
    // Live inputs read by the batches, counted once per batch and once overall.
    let mut reads = 0usize;
    let mut distinct_reads = 0usize;
    for (spec_id, (mut dirty, clean)) in specs {
        // Checked against the originally dirty manifests only: a clean single-key manifest
        // can only overlap another clean one on a key that the dirty manifest also holds.
        if dirty.is_empty() {
            continue;
        }
        if !matches!(
            first_fields.get(&spec_id),
            Some(FirstField::Unpartitioned | FirstField::Typed(_))
        ) {
            return None;
        }
        let dirty_extents = OverlapIndex::new(dirty.iter().map(|d| &d.extent));
        let overlapped: Vec<Candidate> = clean
            .into_iter()
            .filter(|c| dirty_extents.overlaps(&c.extent))
            .collect();
        dirty.extend(overlapped);

        let mut ranges = split_spec(&dirty, config);
        distinct_reads += dirty.len();
        reads += ranges.iter().map(|(_, inputs)| inputs.len()).sum::<usize>();
        for index in no_live.remove(&spec_id).unwrap_or_default() {
            if let Some((_, inputs)) = ranges.iter_mut().min_by_key(|(_, inputs)| inputs.len()) {
                inputs.push(index);
            }
        }
        for (range, mut inputs) in ranges {
            inputs.sort_unstable();
            batches.push(ManifestBatch {
                index: batches.len(),
                partition_spec_id: spec_id,
                range,
                inputs: inputs.into_iter().map(|i| manifests[i].clone()).collect(),
            });
        }
    }
    // Like a global rewrite, a rewrite drops every manifest without live entries, including
    // those of specs with nothing else to rewrite. Such a batch writes nothing, so it needs
    // no writer and no resolvable first-field type.
    if batches.is_empty() {
        no_live.clear();
    }
    for (spec_id, inputs) in no_live {
        batches.push(ManifestBatch {
            index: batches.len(),
            partition_spec_id: spec_id,
            range: FirstFieldRange::All,
            inputs: inputs.into_iter().map(|i| manifests[i].clone()).collect(),
        });
    }
    if config
        .max_read_amplification
        .is_some_and(|max| reads as f64 > max * distinct_reads as f64)
    {
        return None;
    }
    Some(batches)
}

fn distinct_points(manifests: &[ManifestFile], live: &[(Candidate, SummaryClass)]) -> usize {
    let mut points: HashSet<(i32, Point)> = HashSet::new();
    for (candidate, _) in live {
        let spec_id = manifests[candidate.index].partition_spec_id;
        match &candidate.extent {
            Extent::Wide => {
                points.insert((spec_id, Point::Unknown));
            }
            Extent::Known { values, has_null } => {
                if let Some((lower, upper)) = values {
                    points.insert((spec_id, Point::Value(lower.literal().clone())));
                    points.insert((spec_id, Point::Value(upper.literal().clone())));
                }
                if *has_null {
                    points.insert((spec_id, Point::Null));
                }
            }
        }
    }
    points.len()
}

/// Splits the manifests of one spec, all of which need rewriting, into ranges and the
/// indexes of the manifests each range reads.
///
/// The ranges of one spec never overlap and cover every possible first-field value and
/// null, and a manifest joins every range that intersects its extent (all ranges, if it
/// is wide). Together that means each entry of a manifest lies in exactly one of the
/// ranges that read the manifest.
fn split_spec(
    dirty: &[Candidate],
    config: &BatchedRewriteManifestsConfig,
) -> Vec<(FirstFieldRange, Vec<usize>)> {
    let wide: Vec<usize> = dirty
        .iter()
        .filter(|c| c.extent == Extent::Wide)
        .map(|c| c.index)
        .collect();
    let known: Vec<&Candidate> = dirty.iter().filter(|c| c.extent != Extent::Wide).collect();
    if known.is_empty() {
        return vec![(FirstFieldRange::All, wide)];
    }

    let valued: Vec<(&Candidate, &Datum, &Datum)> = known
        .iter()
        .filter_map(|c| match &c.extent {
            Extent::Known {
                values: Some((lower, upper)),
                ..
            } => Some((*c, lower, upper)),
            _ => None,
        })
        .collect();

    let mut ranges = Vec::new();
    if !valued.is_empty() || !wide.is_empty() {
        let cuts = choose_cuts(
            valued
                .iter()
                .map(|(c, lower, upper)| ValueExtent {
                    lower: (*lower).clone(),
                    upper: (*upper).clone(),
                    weight: c.weight,
                })
                .collect(),
            config.limits(),
        );
        // Range `k` holds the values from cut `k - 1` (inclusive) to cut `k` (exclusive),
        // and the cuts ascend, so a manifest joins the ranges from the one holding its
        // lower bound to the one holding its upper bound.
        let range_of = |value: &Datum| {
            cuts.partition_point(|cut| order(cut.literal(), value.literal()).is_le())
        };
        let mut inputs: Vec<Vec<usize>> = vec![Vec::new(); cuts.len() + 1];
        for (candidate, lower, upper) in &valued {
            for range in &mut inputs[range_of(lower)..=range_of(upper)] {
                range.push(candidate.index);
            }
        }
        let lowers = std::iter::once(None).chain(cuts.iter().cloned().map(Some));
        let uppers = cuts.iter().cloned().map(Some).chain(std::iter::once(None));
        for ((lower, upper), mut inputs) in lowers.zip(uppers).zip(inputs) {
            inputs.extend(&wide);
            ranges.push((FirstFieldRange::Values { lower, upper }, inputs));
        }
    }

    let mut null_inputs: Vec<usize> = known
        .iter()
        .filter(|c| matches!(c.extent, Extent::Known { has_null: true, .. }))
        .map(|c| c.index)
        .collect();
    if !wide.is_empty() || !null_inputs.is_empty() {
        null_inputs.extend(&wide);
        ranges.push((FirstFieldRange::Nulls, null_inputs));
    }
    ranges
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{ByteBuf, FieldSummary, Literal, PrimitiveLiteral, Struct};

    use super::*;

    const DOMAIN: std::ops::RangeInclusive<i32> = 0..=40;

    fn config(max_batch_entries: u64, max_batch_manifests: usize) -> BatchedRewriteManifestsConfig {
        BatchedRewriteManifestsConfig {
            max_batch_entries,
            max_batch_manifests,
            load_concurrency: 1,
            max_read_amplification: None,
        }
    }

    /// Limits that never force a cut.
    fn roomy() -> BatchedRewriteManifestsConfig {
        config(1_000_000, 1_000)
    }

    fn int_field() -> HashMap<i32, FirstField> {
        HashMap::from([(0, FirstField::Typed(PrimitiveType::Int))])
    }

    fn summary(lower: i32, upper: i32, null: bool) -> FieldSummary {
        FieldSummary {
            contains_null: null,
            contains_nan: Some(false),
            lower_bound: Some(ByteBuf::from(lower.to_le_bytes().to_vec())),
            upper_bound: Some(ByteBuf::from(upper.to_le_bytes().to_vec())),
        }
    }

    fn null_summary() -> FieldSummary {
        FieldSummary {
            contains_null: true,
            contains_nan: Some(false),
            lower_bound: None,
            upper_bound: None,
        }
    }

    /// A data manifest of spec 0, added by snapshot 1, with 10 added files.
    fn manifest(path: &str, partitions: Option<Vec<FieldSummary>>) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_owned(),
            manifest_length: 0,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 1,
            added_files_count: Some(10),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions,
            key_metadata: None,
            first_row_id: None,
        }
    }

    /// A manifest holding the integers `lower..=upper`.
    fn span(path: &str, lower: i32, upper: i32) -> ManifestFile {
        manifest(path, Some(vec![summary(lower, upper, false)]))
    }

    /// A manifest holding the single value `value`, added by `snapshot`.
    fn key(path: &str, value: i32, snapshot: i64) -> ManifestFile {
        ManifestFile {
            added_snapshot_id: snapshot,
            ..span(path, value, value)
        }
    }

    fn weighted(mut manifest: ManifestFile, added: Option<u32>) -> ManifestFile {
        manifest.added_files_count = added;
        manifest
    }

    fn no_live(path: &str) -> ManifestFile {
        weighted(
            ManifestFile {
                existing_files_count: Some(0),
                ..span(path, 1, 1)
            },
            Some(0),
        )
    }

    fn describe_range(range: &FirstFieldRange) -> String {
        let int = |d: &iceberg::spec::Datum| match d.literal() {
            PrimitiveLiteral::Int(v) => v.to_string(),
            other => panic!("unexpected bound {other:?}"),
        };
        match range {
            FirstFieldRange::All => "all".to_owned(),
            FirstFieldRange::Nulls => "nulls".to_owned(),
            FirstFieldRange::Values { lower, upper } => format!(
                "{},{}",
                lower
                    .as_ref()
                    .map_or("(-inf".to_owned(), |l| format!("[{}", int(l))),
                upper
                    .as_ref()
                    .map_or("+inf)".to_owned(), |u| format!("{})", int(u))),
            ),
        }
    }

    type Layout<'a> = Vec<(i32, String, Vec<&'a str>)>;

    /// `(spec id, range, input paths)` per batch, in plan order.
    fn layout(batches: &[ManifestBatch]) -> Layout<'_> {
        batches
            .iter()
            .enumerate()
            .map(|(i, batch)| {
                assert_eq!(batch.index(), i);
                (
                    batch.partition_spec_id(),
                    describe_range(batch.range()),
                    batch
                        .inputs()
                        .iter()
                        .map(|m| m.manifest_path.as_str())
                        .collect(),
                )
            })
            .collect()
    }

    fn expect<'a>(spec: i32, range: &str, inputs: &[&'a str]) -> (i32, String, Vec<&'a str>) {
        (spec, range.to_owned(), inputs.to_vec())
    }

    fn partition_of(value: Option<i32>) -> Struct {
        Struct::from_iter([value.map(Literal::int)])
    }

    /// Checks that every live entry of every planned input is written by exactly one batch:
    /// among the batches reading a manifest, exactly one range contains each entry's value.
    /// Entries are every value in a manifest's bounds (the whole `DOMAIN` if it has none)
    /// and null if it may hold nulls. Batches of one spec never share a value.
    fn assert_coverage(batches: &[ManifestBatch]) {
        let mut inputs: Vec<&ManifestFile> = Vec::new();
        for batch in batches {
            for input in batch.inputs() {
                assert_eq!(input.partition_spec_id, batch.partition_spec_id());
                if !inputs
                    .iter()
                    .any(|m| m.manifest_path == input.manifest_path)
                {
                    inputs.push(input);
                }
            }
        }
        for manifest in inputs {
            if manifest.added_files_count == Some(0) && manifest.existing_files_count == Some(0) {
                continue;
            }
            let wide = || DOMAIN.map(Some).chain([None]).collect::<Vec<_>>();
            let decode = |b: &ByteBuf| Some(i32::from_le_bytes(b.as_ref().try_into().ok()?));
            let entries: Vec<Option<i32>> =
                match manifest.partitions.as_ref().and_then(|p| p.first()) {
                    Some(s) if s.contains_nan != Some(true) => {
                        let values = match (&s.lower_bound, &s.upper_bound) {
                            (Some(lower), Some(upper)) => {
                                decode(lower).zip(decode(upper)).map(|(lower, upper)| {
                                    (lower..=upper).map(Some).collect::<Vec<_>>()
                                })
                            }
                            (None, None) if s.contains_null => Some(vec![]),
                            _ => None,
                        };
                        match values {
                            Some(mut entries) => {
                                if s.contains_null {
                                    entries.push(None);
                                }
                                entries
                            }
                            None => wide(),
                        }
                    }
                    _ => wide(),
                };
            for entry in entries {
                let writers: Vec<usize> = batches
                    .iter()
                    .filter(|b| {
                        b.inputs()
                            .iter()
                            .any(|m| m.manifest_path == manifest.manifest_path)
                    })
                    .filter(|b| b.range().contains(&partition_of(entry)).unwrap())
                    .map(ManifestBatch::index)
                    .collect();
                assert_eq!(
                    writers.len(),
                    1,
                    "entry {entry:?} of {} written by batches {writers:?}",
                    manifest.manifest_path
                );
            }
        }
        for spec in batches.iter().map(|b| b.partition_spec_id()) {
            for value in DOMAIN.map(Some).chain([None]) {
                let owners = batches
                    .iter()
                    .filter(|b| b.partition_spec_id() == spec)
                    .filter(|b| b.range().contains(&partition_of(value)).unwrap())
                    .count();
                assert!(
                    owners <= 1,
                    "{value:?} is in {owners} ranges of spec {spec}"
                );
            }
        }
    }

    fn plan(
        manifests: &[ManifestFile],
        config: &BatchedRewriteManifestsConfig,
    ) -> Option<Vec<ManifestBatch>> {
        let batches = plan_batches(manifests, &int_field(), 1, config)?;
        assert_coverage(&batches);
        Some(batches)
    }

    #[test]
    fn plan_skips_well_formed_manifests() {
        let manifests = [
            key("clean-1", 1, 1),
            key("clean-2", 2, 2),
            span("mixed", 5, 7),
            // Inside the mixed manifest's range, so it must be rewritten with it.
            key("overlapped", 6, 4),
            key("clean-3", 9, 5),
            // The same key from one snapshot is rollover, not duplication.
            key("rollover-1", 20, 6),
            key("rollover-2", 20, 6),
            key("duplicate-1", 30, 7),
            key("duplicate-2", 30, 8),
        ];
        let batches = plan(&manifests, &roomy()).unwrap();
        assert_eq!(layout(&batches), vec![expect(0, "(-inf,+inf)", &[
            "mixed",
            "overlapped",
            "duplicate-1",
            "duplicate-2"
        ])]);
    }

    #[test]
    fn plan_reads_straddling_manifests_in_every_batch_they_overlap() {
        // At most two manifests starting in each batch cannot avoid straddlers here. `a`
        // straddles into `[2,7)` but only `b` and `c` start there.
        let manifests = [
            span("a", 1, 4),
            span("b", 2, 5),
            span("c", 3, 6),
            span("d", 7, 9),
        ];
        let batches = plan(&manifests, &config(1_000_000, 2)).unwrap();
        assert_eq!(layout(&batches), vec![
            expect(0, "(-inf,2)", &["a"]),
            expect(0, "[2,7)", &["a", "b", "c"]),
            expect(0, "[7,+inf)", &["d"]),
        ]);

        // A cut with no straddlers exists, so no manifest is read twice.
        let manifests = [
            span("a", 1, 3),
            span("b", 3, 5),
            span("c", 6, 8),
            span("d", 7, 9),
        ];
        let batches = plan(&manifests, &config(1_000_000, 2)).unwrap();
        assert_eq!(layout(&batches), vec![
            expect(0, "(-inf,6)", &["a", "b"]),
            expect(0, "[6,+inf)", &["c", "d"]),
        ]);
    }

    #[test]
    fn plan_falls_back_to_a_global_rewrite_past_the_read_amplification_limit() {
        // Every manifest spans five values, so batches reread the ones straddling in.
        let mut manifests: Vec<ManifestFile> =
            (1..=8).map(|i| span(&format!("m{i}"), i, i + 4)).collect();
        // Attached to a batch but never read, so it doesn't count.
        manifests.push(no_live("empty"));
        let mut config = config(1_000_000, 2);
        let batches = plan(&manifests, &config).unwrap();
        assert_eq!(layout(&batches), vec![
            expect(0, "(-inf,2)", &["m1", "empty"]),
            expect(0, "[2,3)", &["m1", "m2"]),
            expect(0, "[3,4)", &["m1", "m2", "m3"]),
            expect(0, "[4,6)", &["m1", "m2", "m3", "m4", "m5"]),
            expect(0, "[6,8)", &["m2", "m3", "m4", "m5", "m6", "m7"]),
            expect(0, "[8,+inf)", &["m4", "m5", "m6", "m7", "m8"]),
        ]);

        // 22 reads of 8 live manifests.
        config.max_read_amplification = Some(2.75);
        assert_eq!(plan(&manifests, &config).map(|b| b.len()), Some(6));
        config.max_read_amplification = Some(2.7);
        assert_eq!(plan(&manifests, &config).map(|b| b.len()), None);
    }

    #[test]
    fn plan_gives_nulls_their_own_batch() {
        let manifests = [
            manifest("values-and-nulls", Some(vec![summary(1, 4, true)])),
            manifest("all-null", Some(vec![null_summary()])),
            span("mixed", 5, 6),
            // Overlaps no dirty manifest on values or nulls.
            key("clean", 30, 2),
        ];
        let batches = plan(&manifests, &roomy()).unwrap();
        assert_eq!(layout(&batches), vec![
            expect(0, "(-inf,+inf)", &["values-and-nulls", "mixed"]),
            expect(0, "nulls", &["values-and-nulls", "all-null"]),
        ]);

        let manifests = [
            ManifestFile {
                added_snapshot_id: 1,
                ..manifest("null-1", Some(vec![null_summary()]))
            },
            ManifestFile {
                added_snapshot_id: 2,
                ..manifest("null-2", Some(vec![null_summary()]))
            },
            key("clean", 3, 3),
        ];
        let batches = plan(&manifests, &roomy()).unwrap();
        assert_eq!(
            layout(&batches),
            vec![expect(0, "nulls", &["null-1", "null-2"])],
            "duplicate null keys need no value range"
        );
    }

    #[test]
    fn plan_reads_wide_manifests_in_every_batch() {
        let manifests = [
            span("a", 1, 5),
            manifest("no-summary", None),
            span("b", 10, 12),
        ];
        let batches = plan(&manifests, &config(1_000_000, 1)).unwrap();
        assert_eq!(
            layout(&batches),
            vec![
                expect(0, "(-inf,10)", &["a", "no-summary"]),
                expect(0, "[10,+inf)", &["no-summary", "b"]),
                expect(0, "nulls", &["no-summary"]),
            ],
            "the wide manifest is read by every range and doesn't count towards the limit"
        );

        // A NaN or undecodable first field is as wide as a missing summary.
        let nan = FieldSummary {
            contains_nan: Some(true),
            ..summary(1, 1, false)
        };
        let manifests = [
            manifest("nan", Some(vec![nan])),
            manifest(
                "garbage",
                Some(vec![FieldSummary {
                    lower_bound: Some(ByteBuf::from(vec![1u8])),
                    ..summary(1, 1, false)
                }]),
            ),
            key("k", 7, 3),
        ];
        let batches = plan(&manifests, &roomy()).unwrap();
        assert_eq!(layout(&batches), vec![
            expect(0, "(-inf,+inf)", &["nan", "garbage", "k"]),
            expect(0, "nulls", &["nan", "garbage"]),
        ]);
    }

    #[test]
    fn plan_never_mixes_partition_specs() {
        let first_fields = HashMap::from([
            (0, FirstField::Typed(PrimitiveType::Int)),
            (1, FirstField::Unpartitioned),
            (2, FirstField::Typed(PrimitiveType::Int)),
        ]);
        let in_spec = |spec_id: i32, mut m: ManifestFile, snapshot: i64| {
            m.partition_spec_id = spec_id;
            m.added_snapshot_id = snapshot;
            m
        };
        let manifests = [
            in_spec(2, span("other-1", 4, 8), 1),
            span("typed", 1, 3),
            in_spec(1, manifest("unpartitioned-1", Some(vec![])), 1),
            in_spec(1, manifest("unpartitioned-2", Some(vec![])), 2),
            in_spec(2, span("other-2", 5, 9), 2),
            // Same value under another spec is another key, so it stays clean.
            in_spec(0, key("clean", 4, 3), 3),
        ];
        let batches = plan_batches(&manifests, &first_fields, 1, &roomy()).unwrap();
        assert_coverage(&batches);
        assert_eq!(layout(&batches), vec![
            expect(0, "(-inf,+inf)", &["typed"]),
            expect(1, "all", &["unpartitioned-1", "unpartitioned-2"]),
            expect(2, "(-inf,+inf)", &["other-1", "other-2"]),
        ]);
    }

    #[test]
    fn plan_returns_none_when_the_table_cannot_be_split() {
        let dup = |path: &str, snapshot: i64| ManifestFile {
            added_snapshot_id: snapshot,
            ..manifest(path, Some(vec![]))
        };
        let delete = ManifestFile {
            content: ManifestContentType::Deletes,
            ..span("delete", 1, 9)
        };
        let cases: Vec<(&str, Vec<ManifestFile>, FirstField)> = vec![
            (
                "unpartitioned",
                vec![dup("a", 1), dup("b", 2)],
                FirstField::Unpartitioned,
            ),
            (
                "one value",
                vec![key("a", 4, 1), key("b", 4, 2), key("c", 4, 3)],
                FirstField::Typed(PrimitiveType::Int),
            ),
            (
                "one manifest with one value",
                vec![span("a", 4, 4)],
                FirstField::Typed(PrimitiveType::Int),
            ),
            (
                "one wide manifest",
                vec![manifest("a", None)],
                FirstField::Typed(PrimitiveType::Int),
            ),
            (
                "no manifests",
                vec![],
                FirstField::Typed(PrimitiveType::Int),
            ),
            (
                "only delete and empty manifests",
                vec![delete, no_live("empty")],
                FirstField::Typed(PrimitiveType::Int),
            ),
            (
                "first field always null, in one manifest",
                vec![manifest("a", Some(vec![null_summary()]))],
                FirstField::Typed(PrimitiveType::Int),
            ),
            (
                "first field always null, across snapshots",
                vec![
                    ManifestFile {
                        added_snapshot_id: 1,
                        ..manifest("a", Some(vec![null_summary()]))
                    },
                    ManifestFile {
                        added_snapshot_id: 2,
                        ..manifest("b", Some(vec![null_summary()]))
                    },
                ],
                FirstField::Typed(PrimitiveType::Int),
            ),
        ];
        for (name, manifests, first_field) in cases {
            let first_fields = HashMap::from([(0, first_field)]);
            assert_eq!(
                plan_batches(&manifests, &first_fields, 1, &roomy()).map(|b| b.len()),
                None,
                "{name}"
            );
        }

        let splittable = [span("a", 1, 3), span("b", 4, 6)];
        assert_eq!(
            plan_batches(&splittable, &int_field(), 0, &roomy()).map(|b| b.len()),
            None,
            "no key to cluster by"
        );

        // A clean manifest without a count blocks the plan too, and a delete manifest
        // without one doesn't.
        let unknown_existing = ManifestFile {
            existing_files_count: None,
            ..span("unknown-existing", 7, 7)
        };
        let delete_without_count = ManifestFile {
            content: ManifestContentType::Deletes,
            added_files_count: None,
            ..span("delete", 1, 9)
        };
        for (name, unknown) in [
            ("unknown added count", weighted(span("unknown", 3, 4), None)),
            ("unknown existing count", unknown_existing),
        ] {
            let mut manifests = splittable.to_vec();
            manifests.push(unknown);
            assert_eq!(
                plan_batches(&manifests, &int_field(), 1, &roomy()).map(|b| b.len()),
                None,
                "{name}"
            );
        }
        let mut manifests = splittable.to_vec();
        manifests.push(delete_without_count);
        assert_eq!(
            plan_batches(&manifests, &int_field(), 1, &roomy()).map(|b| b.len()),
            Some(1),
            "delete manifest without a count"
        );
    }

    #[test]
    fn plan_returns_none_when_a_spec_to_rewrite_has_an_unresolvable_first_field() {
        let in_spec = |spec_id: i32, mut m: ManifestFile, snapshot: i64| {
            m.partition_spec_id = spec_id;
            m.added_snapshot_id = snapshot;
            m
        };
        let first_fields = HashMap::from([
            (0, FirstField::Typed(PrimitiveType::Int)),
            (1, FirstField::Unknown),
        ]);
        let split_spec_0 = [span("a", 1, 3), span("b", 4, 6)];

        // Spec 1's writers can't be created, so its batch would always fail.
        let mut manifests = split_spec_0.to_vec();
        manifests.push(in_spec(1, span("c", 1, 5), 1));
        assert_eq!(
            plan_batches(&manifests, &first_fields, 1, &roomy()).map(|b| b.len()),
            None,
            "unresolvable spec with a mixed manifest"
        );
        // A spec that has nothing to rewrite doesn't matter.
        let mut manifests = split_spec_0.to_vec();
        manifests.push(in_spec(1, key("c", 1, 1), 1));
        manifests.push(in_spec(1, key("d", 2, 2), 2));
        let batches = plan_batches(&manifests, &first_fields, 1, &roomy()).unwrap();
        assert_eq!(layout(&batches), vec![expect(0, "(-inf,+inf)", &[
            "a", "b"
        ])]);
        // A spec the table doesn't have is as unresolvable.
        let mut manifests = split_spec_0.to_vec();
        manifests.push(in_spec(9, span("c", 1, 5), 1));
        assert_eq!(
            plan_batches(&manifests, &first_fields, 1, &roomy()).map(|b| b.len()),
            None,
            "unknown spec"
        );
    }

    #[test]
    fn plan_covers_every_entry_for_generated_manifests() {
        // Deterministic xorshift, so a failure reproduces.
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let mut planned = 0;
        for round in 0..300 {
            let manifests: Vec<ManifestFile> = (0..next(25) + 2)
                .map(|i| {
                    let path = format!("r{round}-m{i}");
                    let lower = next(30) as i32;
                    let upper = lower + next(8) as i32;
                    let snapshot = next(3) as i64;
                    let m = match next(10) {
                        0 => manifest(&path, None),
                        1 => manifest(&path, Some(vec![null_summary()])),
                        2 => manifest(&path, Some(vec![summary(lower, upper, true)])),
                        3 | 4 => key(&path, lower, snapshot),
                        _ => span(&path, lower, upper),
                    };
                    weighted(m, Some(next(40) as u32))
                })
                .collect();
            let config = config(next(200) + 1, next(6) as usize + 1);
            if plan(&manifests, &config).is_some() {
                planned += 1;
            }
        }
        assert_eq!(planned, 300, "generated tables always have several values");
    }

    /// The first-field summary a writer produces for `entries`: bounds over the non-null
    /// values, `contains_null`, and `contains_nan` only for floating point values (Java
    /// leaves it unset for other types).
    fn summary_of(entries: &[Option<Datum>]) -> FieldSummary {
        let values: Vec<&Datum> = entries.iter().flatten().collect();
        let bound = |pick_greater: bool| {
            values
                .iter()
                .copied()
                .reduce(|a, b| {
                    let greater = order(b.literal(), a.literal()).is_gt();
                    if greater == pick_greater { b } else { a }
                })
                .map(|d| ByteBuf::from(d.to_bytes().unwrap().to_vec()))
        };
        let is_nan = |d: &&Datum| match d.literal() {
            PrimitiveLiteral::Float(f) => f.0.is_nan(),
            PrimitiveLiteral::Double(f) => f.0.is_nan(),
            _ => false,
        };
        let floating = values.iter().any(|d| {
            matches!(
                d.literal(),
                PrimitiveLiteral::Float(_) | PrimitiveLiteral::Double(_)
            )
        });
        FieldSummary {
            contains_null: entries.iter().any(Option::is_none),
            contains_nan: floating.then(|| values.iter().any(is_nan)),
            lower_bound: bound(false),
            upper_bound: bound(true),
        }
    }

    #[test]
    fn plan_covers_every_entry_for_every_first_field_type() {
        let dec = |s: &str| Some(Datum::decimal_from_str(s).unwrap());
        let uuid = |v: u128| Some(Datum::uuid(uuid::Uuid::from_u128(v)));
        let bin = |b: &[u8]| Some(Datum::binary(b.to_vec()));
        let string = |s: &str| Some(Datum::string(s));
        let float = |v: f32| Some(Datum::float(v));
        let double = |v: f64| Some(Datum::double(v));
        let long = |v: i64| Some(Datum::long(v));
        let int = |v: i32| Some(Datum::int(v));
        let date = |v: i32| Some(Datum::date(v));
        let micros = |v: i64| Some(Datum::timestamp_micros(v));

        type Entries = Vec<Option<Datum>>;
        // (name, first field type, entries of each manifest, expected batches)
        let cases: Vec<(&str, PrimitiveType, Vec<Entries>, usize)> = vec![
            (
                "string identity or truncate",
                PrimitiveType::String,
                vec![
                    vec![string("apple"), string("apricot")],
                    vec![string("banana"), string("cherry")],
                    vec![string("cherry"), string("zebra"), None],
                    vec![string(""), string("a")],
                ],
                5,
            ),
            (
                "binary",
                PrimitiveType::Binary,
                vec![
                    vec![bin(&[0, 1]), bin(&[0, 2])],
                    vec![bin(&[1]), bin(&[255, 0])],
                    vec![bin(&[128]), bin(&[255])],
                ],
                3,
            ),
            (
                "timestamp identity",
                PrimitiveType::Timestamp,
                vec![
                    vec![micros(-5), micros(10)],
                    vec![micros(1_700_000_000_000_000), micros(1_700_000_000_000_001)],
                    vec![micros(10), micros(20), None],
                ],
                4,
            ),
            (
                "date from a day transform",
                PrimitiveType::Date,
                vec![
                    vec![date(-1), date(3)],
                    vec![date(19_000), date(19_001)],
                    vec![date(3), date(19_000)],
                ],
                3,
            ),
            (
                "decimal",
                PrimitiveType::Decimal {
                    precision: 38,
                    scale: 2,
                },
                vec![
                    vec![dec("-12.50"), dec("0.00")],
                    vec![dec("0.01"), dec("99.99")],
                    vec![dec("-99999.99"), dec("-12.50")],
                ],
                3,
            ),
            (
                "uuid",
                PrimitiveType::Uuid,
                vec![
                    vec![uuid(1), uuid(5)],
                    vec![uuid(u128::MAX), uuid(1 << 100)],
                    vec![uuid(5), uuid(1 << 100)],
                ],
                3,
            ),
            (
                "float with NaN, infinities and both zeros",
                PrimitiveType::Float,
                vec![
                    vec![float(f32::NEG_INFINITY), float(-0.0)],
                    vec![float(0.0), float(1.5)],
                    // Its summary says it has NaN, so it is read by every batch.
                    vec![float(f32::NAN), float(2.0)],
                    vec![float(3.0), float(f32::INFINITY), None],
                ],
                4,
            ),
            (
                "double with NaN, infinities and both zeros",
                PrimitiveType::Double,
                vec![
                    vec![double(f64::NEG_INFINITY), double(-0.0)],
                    vec![double(0.0), double(1.5)],
                    vec![double(f64::NAN), double(2.0)],
                    vec![double(3.0), double(f64::INFINITY)],
                ],
                4,
            ),
            (
                "long after an int column was promoted",
                PrimitiveType::Long,
                vec![
                    vec![int(-3), int(4)],
                    vec![long(5_000_000_000), long(5_000_000_001)],
                    vec![int(4), long(10)],
                ],
                3,
            ),
            (
                "double after a float column was promoted",
                PrimitiveType::Double,
                vec![
                    vec![float(-1.5), float(2.5)],
                    vec![double(1e300), double(1.5e300)],
                    vec![float(2.5), double(3.0)],
                ],
                3,
            ),
        ];

        for (name, ty, manifest_entries, expected_batches) in cases {
            let manifests: Vec<ManifestFile> = manifest_entries
                .iter()
                .enumerate()
                .map(|(i, entries)| {
                    let mut m = manifest(&format!("m{i}"), Some(vec![summary_of(entries)]));
                    m.added_snapshot_id = i as i64;
                    m
                })
                .collect();
            let first_fields = HashMap::from([(0, FirstField::Typed(ty))]);
            let batches = plan_batches(&manifests, &first_fields, 1, &config(1_000_000, 1))
                .unwrap_or_else(|| panic!("{name}: planned"));
            assert_eq!(batches.len(), expected_batches, "{name}");

            let partition_of = |entry: &Option<Datum>| {
                Struct::from_iter([entry
                    .as_ref()
                    .map(|d| Literal::Primitive(d.literal().clone()))])
            };
            // Every entry is written by exactly one of the batches reading its manifest.
            for (manifest, entries) in manifests.iter().zip(&manifest_entries) {
                for entry in entries {
                    let writers = batches
                        .iter()
                        .filter(|b| b.inputs().contains(manifest))
                        .filter(|b| b.range().contains(&partition_of(entry)).unwrap())
                        .count();
                    assert_eq!(
                        writers, 1,
                        "{name}: {entry:?} of {}",
                        manifest.manifest_path
                    );
                }
            }
            // No value is in two ranges.
            for entry in manifest_entries.iter().flatten() {
                let owners = batches
                    .iter()
                    .filter(|b| b.range().contains(&partition_of(entry)).unwrap())
                    .count();
                assert_eq!(owners, 1, "{name}: {entry:?}");
            }
        }
    }

    #[test]
    fn plan_reads_floating_point_manifests_without_nan_information_in_every_batch() {
        let float = |lower: f32, upper: f32, contains_nan: Option<bool>| {
            let mut summary = summary_of(&[Some(Datum::float(lower)), Some(Datum::float(upper))]);
            summary.contains_nan = contains_nan;
            summary
        };
        let manifests: Vec<ManifestFile> = [
            ("a", float(1.0, 2.0, Some(false))),
            ("unset", float(3.0, 3.5, None)),
            ("b", float(5.0, 6.0, Some(false))),
        ]
        .into_iter()
        .map(|(path, summary)| manifest(path, Some(vec![summary])))
        .collect();
        let first_fields = HashMap::from([(0, FirstField::Typed(PrimitiveType::Float))]);

        let batches = plan_batches(&manifests, &first_fields, 1, &config(1_000_000, 1)).unwrap();
        let inputs: Vec<Vec<&str>> = batches
            .iter()
            .map(|b| {
                b.inputs()
                    .iter()
                    .map(|m| m.manifest_path.as_str())
                    .collect()
            })
            .collect();
        assert_eq!(inputs, vec![vec!["a", "unset"], vec!["unset", "b"], vec![
            "unset"
        ],]);
        assert_eq!(
            batches
                .iter()
                .map(|b| matches!(b.range(), FirstFieldRange::Nulls))
                .collect::<Vec<_>>(),
            vec![false, false, true]
        );
    }

    #[test]
    fn plan_has_no_batches_when_nothing_needs_rewriting() {
        let manifests = [key("a", 1, 1), key("b", 2, 2)];
        let batches = plan(&manifests, &roomy()).unwrap();
        assert_eq!(layout(&batches), Layout::new());
    }

    #[test]
    fn plan_clusters_by_the_requested_number_of_fields() {
        let two_fields = |first: i32, second: (i32, i32)| {
            manifest(
                "m",
                Some(vec![
                    summary(first, first, false),
                    summary(second.0, second.1, false),
                ]),
            )
        };
        let mut mixed_second = two_fields(4, (1, 9));
        mixed_second.manifest_path = "mixed-second".to_owned();
        let manifests = [mixed_second, key("other", 7, 2)];

        let batches = plan_batches(&manifests, &int_field(), 1, &roomy()).unwrap();
        assert_eq!(
            layout(&batches),
            Layout::new(),
            "the second field's mix is ignored when clustering by the first"
        );
        let batches = plan_batches(&manifests, &int_field(), 2, &roomy()).unwrap();
        assert_eq!(layout(&batches), vec![expect(0, "(-inf,+inf)", &[
            "mixed-second"
        ])]);
    }

    #[test]
    fn plan_respects_entry_and_manifest_limits() {
        let disjoint = |added: u32| {
            (0..5)
                .map(|i| weighted(span(&format!("m{i}"), 2 * i + 1, 2 * i + 2), Some(added)))
                .collect::<Vec<_>>()
        };
        let oversized = vec![
            span("small-1", 1, 2),
            weighted(span("big", 3, 4), Some(1000)),
            span("small-2", 5, 6),
        ];
        let cases: Vec<(
            &str,
            Vec<ManifestFile>,
            BatchedRewriteManifestsConfig,
            Layout,
        )> = vec![
            (
                "entries: the target is 80% of the limit",
                disjoint(20),
                config(100, 100),
                vec![
                    expect(0, "(-inf,7)", &["m0", "m1", "m2"]),
                    expect(0, "[7,+inf)", &["m3", "m4"]),
                ],
            ),
            (
                "entries: reaching the target cuts",
                disjoint(20),
                config(50, 100),
                vec![
                    expect(0, "(-inf,3)", &["m0"]),
                    expect(0, "[3,5)", &["m1"]),
                    expect(0, "[5,7)", &["m2"]),
                    expect(0, "[7,9)", &["m3"]),
                    expect(0, "[9,+inf)", &["m4"]),
                ],
            ),
            ("manifests", disjoint(1), config(1_000_000, 2), vec![
                expect(0, "(-inf,5)", &["m0", "m1"]),
                expect(0, "[5,9)", &["m2", "m3"]),
                expect(0, "[9,+inf)", &["m4"]),
            ]),
            (
                "one manifest over the limit forms its own batch",
                oversized,
                config(50, 100),
                vec![
                    expect(0, "(-inf,3)", &["small-1"]),
                    expect(0, "[3,5)", &["big"]),
                    expect(0, "[5,+inf)", &["small-2"]),
                ],
            ),
        ];
        for (name, manifests, config, expected) in cases {
            let batches = plan(&manifests, &config).unwrap();
            assert_eq!(layout(&batches), expected, "{name}");
        }
    }

    #[test]
    fn plan_attaches_manifests_without_live_entries_to_batches() {
        let in_spec = |spec: i32, manifest: ManifestFile| ManifestFile {
            partition_spec_id: spec,
            ..manifest
        };
        let delete = ManifestFile {
            content: ManifestContentType::Deletes,
            ..span("delete", 1, 9)
        };
        let manifests = [
            span("a", 1, 2),
            no_live("empty-1"),
            no_live("empty-2"),
            span("b", 3, 4),
            no_live("empty-3"),
            in_spec(5, no_live("spec-5-empty-1")),
            // A clean manifest, so spec 5 has nothing else to rewrite.
            in_spec(5, span("spec-5-clean", 7, 7)),
            in_spec(5, no_live("spec-5-empty-2")),
            in_spec(6, no_live("spec-6-empty")),
            delete,
        ];
        let batches = plan(&manifests, &config(1_000_000, 1)).unwrap();
        // Each goes to the batch of its spec with the fewest inputs, the first on ties.
        assert_eq!(layout(&batches), vec![
            expect(0, "(-inf,3)", &["a", "empty-1", "empty-3"]),
            expect(0, "[3,+inf)", &["empty-2", "b"]),
            expect(5, "all", &["spec-5-empty-1", "spec-5-empty-2"]),
            expect(6, "all", &["spec-6-empty"]),
        ]);

        // With nothing else to rewrite, empty manifests alone don't make a plan do work.
        let clean = [
            span("a", 1, 1),
            in_spec(5, span("spec-5-clean", 7, 7)),
            in_spec(5, no_live("spec-5-empty")),
        ];
        assert_eq!(
            layout(&plan(&clean, &config(1_000_000, 1)).unwrap()),
            vec![]
        );
    }

    #[test]
    fn config_validation_names_the_invalid_limit() {
        let valid = config(10, 2);
        valid.validate().unwrap();
        BatchedRewriteManifestsConfig {
            max_read_amplification: Some(1.0),
            ..valid.clone()
        }
        .validate()
        .unwrap();
        let amplification = |max: f64| BatchedRewriteManifestsConfig {
            max_read_amplification: Some(max),
            ..valid.clone()
        };
        let cases = [
            (amplification(0.5), "max_read_amplification"),
            (amplification(f64::NAN), "max_read_amplification"),
            (amplification(f64::INFINITY), "max_read_amplification"),
            (
                BatchedRewriteManifestsConfig {
                    max_batch_entries: 0,
                    ..valid.clone()
                },
                "max_batch_entries",
            ),
            (
                BatchedRewriteManifestsConfig {
                    max_batch_manifests: 0,
                    ..valid.clone()
                },
                "max_batch_manifests",
            ),
            (
                BatchedRewriteManifestsConfig {
                    load_concurrency: 0,
                    ..valid
                },
                "load_concurrency",
            ),
        ];
        for (config, name) in cases {
            assert_eq!(
                config.validate().unwrap_err().to_string(),
                format!("Invalid configuration: {name} must be at least 1")
            );
        }
    }

    #[test]
    fn range_target_is_80_percent_of_the_entry_limit() {
        let target = |max: u64| config(max, 1).limits().target_entries;
        assert_eq!(target(1), 0);
        assert_eq!(target(10), 8);
        assert_eq!(target(u64::MAX), 14_757_395_258_967_641_292);
    }

    #[test]
    fn unique_id_skips_taken_ids() {
        let mut draws = [5, 5, 7, 9].into_iter();
        assert_eq!(
            generate_unique_id(|id| id == 5, || draws.next().unwrap()),
            7
        );
        assert_eq!(draws.next(), Some(9));
    }
}
