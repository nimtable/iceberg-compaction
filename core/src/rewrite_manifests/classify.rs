//! Classification of data manifests from their manifest-list partition summaries.
//!
//! Summaries avoid loading manifest files: each manifest is classified as holding no
//! live entries, a single cluster key, several keys, or an inconclusive summary.

use std::collections::HashMap;

use iceberg::spec::ManifestFile;

/// Outcome of the manifest-list pass of the no-op check.
#[derive(Debug, PartialEq)]
pub(crate) enum SummaryDecision {
    NeedsRewrite,
    NoRewrite,
    /// No summary proved a rewrite is needed, but `manifests` couldn't be classified.
    Inconclusive {
        manifests: u64,
    },
}

/// Decides from summaries alone. A definite `Mixed` or duplicate key wins over
/// inconclusive manifests, since loading can't un-need a rewrite.
pub(crate) fn decide_from_summaries<'a>(
    data_manifests: impl Iterator<Item = &'a ManifestFile>,
    clustered_fields: usize,
) -> SummaryDecision {
    let mut key_snapshots: HashMap<SummaryKey, i64> = HashMap::new();
    let mut inconclusive: u64 = 0;
    for manifest_file in data_manifests {
        match classify_from_summary(manifest_file, clustered_fields) {
            SummaryClass::Mixed => return SummaryDecision::NeedsRewrite,
            SummaryClass::SingleKey(key) => {
                if is_cross_snapshot_duplicate(&mut key_snapshots, key, manifest_file) {
                    return SummaryDecision::NeedsRewrite;
                }
            }
            SummaryClass::NoLiveEntries => {}
            SummaryClass::Inconclusive => inconclusive += 1,
        }
    }
    if inconclusive == 0 {
        SummaryDecision::NoRewrite
    } else {
        SummaryDecision::Inconclusive {
            manifests: inconclusive,
        }
    }
}

/// Records `manifest_file`'s snapshot for `key`; true if `key` was already seen in a
/// manifest added by a different snapshot.
///
/// Same-key manifests from one snapshot are not duplicates: a rewrite rolls a large key
/// over at `commit.manifest.target-size-bytes`, and rewriting that output reproduces it,
/// so treating them as duplicates would make every check report a rewrite.
///
/// Caveat: any commit that rewrites existing manifests stamps them all with its own
/// snapshot id, so real duplicates can look like rollover. Known sources: compaction's
/// `ReplaceFiles` (one manifest per touched input plus the added manifest), Java
/// copy-on-write DELETE/UPDATE/MERGE, and appends that add several manifests. The cost is a
/// skipped consolidation, bounded by one commit's manifests per key: the next write to that
/// key adds another snapshot id and triggers a rewrite.
pub(crate) fn is_cross_snapshot_duplicate<K: std::hash::Hash + Eq>(
    key_snapshots: &mut HashMap<K, i64>,
    key: K,
    manifest_file: &ManifestFile,
) -> bool {
    *key_snapshots
        .entry(key)
        .or_insert(manifest_file.added_snapshot_id)
        != manifest_file.added_snapshot_id
}

/// Cluster key derived from a manifest's partition summary: the spec id plus one
/// value per clustered field (`None` = null).
pub(crate) type SummaryKey = (i32, Vec<Option<Vec<u8>>>);

#[derive(Debug, PartialEq)]
pub(crate) enum SummaryClass {
    /// Entries for more than one key: a rewrite is needed.
    Mixed,
    /// Every entry has this key.
    SingleKey(SummaryKey),
    /// The manifest has no added or existing files; it contributes no key.
    NoLiveEntries,
    /// The summary can't decide (missing, NaN, or malformed); load the manifest.
    Inconclusive,
}

/// Classifies a data manifest from its manifest-list `FieldSummary`s alone, keying on
/// the first `clustered_fields` partition fields (all of them if there are fewer).
///
/// The writer computes summaries over every entry, deleted ones included, so a
/// manifest whose only other-key entries are deleted is reported `Mixed`. That
/// over-triggers a rewrite (which drops deleted entries) but never misses one.
/// Bounds are assumed untruncated; iceberg-rust and Java write exact partition bounds.
pub(crate) fn classify_from_summary(
    manifest_file: &ManifestFile,
    clustered_fields: usize,
) -> SummaryClass {
    if manifest_file.added_files_count == Some(0) && manifest_file.existing_files_count == Some(0) {
        return SummaryClass::NoLiveEntries;
    }
    // iceberg-rust writes `Some(vec![])` for unpartitioned specs, so `None` only
    // comes from legacy or foreign writers.
    let Some(summaries) = &manifest_file.partitions else {
        return SummaryClass::Inconclusive;
    };
    let clustered = &summaries[..summaries.len().min(clustered_fields)];

    let mut values = Vec::with_capacity(clustered.len());
    for summary in clustered {
        if summary.contains_nan == Some(true) {
            return SummaryClass::Inconclusive;
        }
        let value = match (&summary.lower_bound, &summary.upper_bound) {
            (None, None) if summary.contains_null => None,
            (Some(_), Some(_)) if summary.contains_null => return SummaryClass::Mixed,
            (Some(lower), Some(upper)) if lower == upper => Some(lower.to_vec()),
            (Some(_), Some(_)) => return SummaryClass::Mixed,
            // No values at all, or one bound missing: not something we can reason about.
            _ => return SummaryClass::Inconclusive,
        };
        values.push(value);
    }
    SummaryClass::SingleKey((manifest_file.partition_spec_id, values))
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{ByteBuf, FieldSummary, ManifestContentType};

    use super::*;

    fn summary(lower: Option<&[u8]>, upper: Option<&[u8]>, null: bool) -> FieldSummary {
        FieldSummary {
            contains_null: null,
            contains_nan: Some(false),
            lower_bound: lower.map(|b| ByteBuf::from(b.to_vec())),
            upper_bound: upper.map(|b| ByteBuf::from(b.to_vec())),
        }
    }

    fn manifest_with(partitions: Option<Vec<FieldSummary>>) -> ManifestFile {
        ManifestFile {
            manifest_path: "s3://bucket/m.avro".to_owned(),
            manifest_length: 0,
            partition_spec_id: 7,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_files_count: Some(1),
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

    #[test]
    fn classify_from_summary_cases() {
        use SummaryClass::*;
        let day = summary(Some(b"d1"), Some(b"d1"), false);
        let days = summary(Some(b"d1"), Some(b"d2"), false);
        let bucket_range = summary(Some(b"b0"), Some(b"b9"), false);
        let all_null = summary(None, None, true);
        let null_and_value = summary(Some(b"d1"), Some(b"d1"), true);
        let nan = FieldSummary {
            contains_nan: Some(true),
            ..day.clone()
        };
        // `ALL` stands in for a full-partition clustering strategy: any count >= field count.
        const ALL: usize = usize::MAX;
        const FIRST: usize = 1;

        let cases: Vec<(&str, Option<Vec<FieldSummary>>, usize, SummaryClass)> = vec![
            (
                "equal bounds",
                Some(vec![day.clone()]),
                ALL,
                SingleKey((7, vec![Some(b"d1".to_vec())])),
            ),
            ("differing bounds", Some(vec![days.clone()]), ALL, Mixed),
            (
                "all null is one key",
                Some(vec![all_null]),
                ALL,
                SingleKey((7, vec![None])),
            ),
            ("null plus a value", Some(vec![null_and_value]), ALL, Mixed),
            ("NaN", Some(vec![nan]), ALL, Inconclusive),
            ("missing summary", None, ALL, Inconclusive),
            (
                "no values, one bound",
                Some(vec![summary(Some(b"d1"), None, false)]),
                ALL,
                Inconclusive,
            ),
            ("unpartitioned", Some(vec![]), ALL, SingleKey((7, vec![]))),
            (
                "second field mixed",
                Some(vec![day.clone(), bucket_range.clone()]),
                ALL,
                Mixed,
            ),
            (
                "first-field ignores later fields",
                Some(vec![day.clone(), bucket_range.clone()]),
                FIRST,
                SingleKey((7, vec![Some(b"d1".to_vec())])),
            ),
            (
                "first-field still sees its own mix",
                Some(vec![days, day.clone()]),
                FIRST,
                Mixed,
            ),
            (
                "exact field count keys on every field",
                Some(vec![day.clone(), bucket_range.clone()]),
                2,
                Mixed,
            ),
            (
                "count above field count behaves like all fields",
                Some(vec![day.clone(), bucket_range.clone()]),
                3,
                Mixed,
            ),
            (
                "zero fields keys on the spec id alone",
                Some(vec![summary(Some(b"d1"), Some(b"d2"), false), bucket_range]),
                0,
                SingleKey((7, vec![])),
            ),
            ("zero fields still needs a summary", None, 0, Inconclusive),
        ];
        for (name, partitions, clustered_fields, expected) in cases {
            assert_eq!(
                classify_from_summary(&manifest_with(partitions), clustered_fields),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn decide_from_summaries_combines_manifests() {
        use SummaryDecision::*;
        let key = |v: &[u8]| Some(vec![summary(Some(v), Some(v), false)]);
        let mixed = Some(vec![summary(Some(b"d1"), Some(b"d2"), false)]);
        let mut other_spec = manifest_with(key(b"d1"));
        other_spec.partition_spec_id = 8;
        let later = |mut m: ManifestFile| {
            m.added_snapshot_id = 1;
            m
        };

        let cases: Vec<(&str, Vec<ManifestFile>, SummaryDecision)> = vec![
            ("empty", vec![], NoRewrite),
            (
                "distinct keys",
                vec![manifest_with(key(b"d1")), manifest_with(key(b"d2"))],
                NoRewrite,
            ),
            (
                "duplicate key across snapshots",
                vec![manifest_with(key(b"d1")), later(manifest_with(key(b"d1")))],
                NeedsRewrite,
            ),
            // One writer rolled the key over at the target size; rewriting reproduces it.
            (
                "duplicate key from one snapshot",
                vec![manifest_with(key(b"d1")), manifest_with(key(b"d1"))],
                NoRewrite,
            ),
            (
                "same value, different spec",
                vec![manifest_with(key(b"d1")), other_spec],
                NoRewrite,
            ),
            (
                "mixed manifest",
                vec![manifest_with(key(b"d1")), manifest_with(mixed.clone())],
                NeedsRewrite,
            ),
            (
                "inconclusive only",
                vec![manifest_with(key(b"d1")), manifest_with(None)],
                Inconclusive { manifests: 1 },
            ),
            // A definite answer wins: loading can't make a rewrite unnecessary.
            (
                "mixed beats inconclusive",
                vec![manifest_with(None), manifest_with(mixed)],
                NeedsRewrite,
            ),
            (
                "duplicate beats inconclusive",
                vec![
                    manifest_with(None),
                    manifest_with(key(b"d1")),
                    later(manifest_with(key(b"d1"))),
                ],
                NeedsRewrite,
            ),
        ];
        for (name, manifests, expected) in cases {
            assert_eq!(
                decide_from_summaries(manifests.iter(), usize::MAX),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn decide_from_summaries_zero_fields_collapses_each_spec_to_one_key() {
        let two_keys = |v: &[u8]| {
            Some(vec![
                summary(Some(v), Some(v), false),
                summary(Some(b"b0"), Some(b"b9"), false),
            ])
        };
        let mut later = manifest_with(two_keys(b"d2"));
        later.added_snapshot_id = 1;
        let manifests = [manifest_with(two_keys(b"d1")), later];

        assert_eq!(
            decide_from_summaries(manifests.iter(), 0),
            SummaryDecision::NeedsRewrite,
            "different snapshots in one spec are duplicates once no field is keyed"
        );
        assert_eq!(
            decide_from_summaries(manifests.iter(), 1),
            SummaryDecision::NoRewrite,
            "the first field alone separates the two manifests"
        );
    }

    #[test]
    fn classify_from_summary_skips_manifests_without_live_files() {
        let mut manifest = manifest_with(Some(vec![summary(Some(b"d1"), Some(b"d2"), false)]));
        manifest.added_files_count = Some(0);
        assert_eq!(
            classify_from_summary(&manifest, usize::MAX),
            SummaryClass::NoLiveEntries,
            "Deleted-only manifests contribute no key, even with mixed bounds"
        );

        // Unknown counts (e.g. V1) must not be read as zero.
        manifest.added_files_count = None;
        assert_eq!(
            classify_from_summary(&manifest, usize::MAX),
            SummaryClass::Mixed
        );
    }
}
