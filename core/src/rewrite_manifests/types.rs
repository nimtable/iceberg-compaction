//! Configuration, plan and batch types of the batched manifest rewrite.
//!
//! A [`ManifestRewritePlan`] splits the data manifests of one branch into
//! [`ManifestBatch`]es. Plans and batches are owned (`Clone + Send + 'static`), so batches
//! can be rewritten on separate tasks, or later in separate jobs, without borrowing a
//! `Table`.

use std::cmp::Ordering;
use std::mem::discriminant;
use std::time::Duration;

use iceberg::spec::{Datum, Literal, ManifestFile, PrimitiveLiteral, PrimitiveType, Struct};
use iceberg::table::Table;

use crate::{CompactionError, Result};

/// Targets for splitting a manifest rewrite into batches.
///
/// The limits are targets the planner builds each batch around, not hard caps. A manifest
/// that straddles a cut is read by every batch it overlaps, and manifests with unusable
/// first-field bounds or without live entries are added to batches on top of the targets.
/// A single manifest over a target, or a run of manifests with identical lower bounds,
/// which can't be cut apart, forms a batch of its own.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchedRewriteManifestsConfig {
    /// Target for the live entries (added plus existing files) a batch is estimated to
    /// write. Ranges are cut at about 80% of this.
    pub max_batch_entries: u64,
    /// Target for the number of manifests whose first-field lower bound falls in a batch's
    /// range. The batch also reads the manifests straddling in from earlier ranges.
    pub max_batch_manifests: usize,
    /// Maximum manifests loaded at once by each
    /// [`rewrite_manifest_batch`](super::rewrite_manifest_batch) call. Must be at least 1.
    pub load_concurrency: usize,
    /// If set, [`plan_manifest_batches`](super::plan_manifest_batches) returns `None`, so
    /// the caller falls back to a global rewrite, when the batches would read more than
    /// this many times as many manifests as a global rewrite: the inputs of all batches,
    /// counted once per batch, over the distinct inputs. Manifests without live entries
    /// aren't read and don't count. Must be at least 1. `None` never falls back.
    pub max_read_amplification: Option<f64>,
}

/// A plan to rewrite the data manifests of one branch in independent batches.
///
/// A commit built from the plan must delete the batches' inputs and add their rewritten
/// outputs in one snapshot with id [`snapshot_id`](Self::snapshot_id), because outputs are
/// written ahead of time with that id.
#[derive(Debug, Clone)]
pub struct ManifestRewritePlan {
    pub(super) branch: String,
    pub(super) base_snapshot_id: i64,
    pub(super) clustered_fields: usize,
    pub(super) snapshot_id: i64,
    pub(super) batches: Vec<ManifestBatch>,
}

impl ManifestRewritePlan {
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// Snapshot the branch pointed at when the plan was made; every input comes from its
    /// manifest list.
    pub fn base_snapshot_id(&self) -> i64 {
        self.base_snapshot_id
    }

    /// Number of leading partition fields that form the cluster key.
    pub fn clustered_fields(&self) -> usize {
        self.clustered_fields
    }

    /// Id reserved for the snapshot that commits the rewrite. Not yet used by any
    /// snapshot of the table at plan time.
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Batches in index order; batch `i` is `batches()[i]`. Empty if nothing needs rewriting.
    pub fn batches(&self) -> &[ManifestBatch] {
        &self.batches
    }
}

/// The manifests one rewrite task reads and the entries it writes.
///
/// Within one partition spec, batch ranges never overlap, and every batch whose range
/// intersects a manifest's keys reads that manifest. So a task that keeps only the
/// entries its [`range`](Self::range) contains writes every entry of every input
/// exactly once across all batches, even for manifests read by several batches.
#[derive(Debug, Clone)]
pub struct ManifestBatch {
    pub(super) index: usize,
    pub(super) partition_spec_id: i32,
    pub(super) range: FirstFieldRange,
    pub(super) inputs: Vec<ManifestFile>,
}

impl ManifestBatch {
    /// Position in [`ManifestRewritePlan::batches`].
    pub fn index(&self) -> usize {
        self.index
    }

    /// Spec shared by all inputs; a batch never mixes specs.
    pub fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub fn range(&self) -> &FirstFieldRange {
        &self.range
    }

    /// Input manifests exactly as listed in the plan's base snapshot, in manifest-list order.
    /// Manifests without live entries are inputs too, so the commit drops them.
    pub fn inputs(&self) -> &[ManifestFile] {
        &self.inputs
    }
}

/// The manifests written by [`rewrite_manifest_batch`](super::rewrite_manifest_batch) for
/// one batch, ready to be committed by
/// [`commit_manifest_batches`](super::commit_manifest_batches).
///
/// Owned (`Clone + Send + 'static`), so it can be returned from a spawned task.
#[derive(Debug, Clone)]
pub struct RewrittenManifestBatch {
    pub(super) batch_index: usize,
    pub(super) manifests: Vec<ManifestFile>,
    pub(super) inputs_read: usize,
    pub(super) entries_written: u64,
    pub(super) duration: Duration,
}

impl RewrittenManifestBatch {
    /// Index of the rewritten batch in [`ManifestRewritePlan::batches`].
    pub fn batch_index(&self) -> usize {
        self.batch_index
    }

    /// The manifests written, each holding the live entries of one cluster key.
    pub fn manifests(&self) -> &[ManifestFile] {
        &self.manifests
    }

    /// Input manifests loaded, which is all of the batch's inputs.
    pub fn inputs_read(&self) -> usize {
        self.inputs_read
    }

    /// Live entries written across [`manifests`](Self::manifests).
    pub fn entries_written(&self) -> u64 {
        self.entries_written
    }

    /// Time spent loading the inputs and writing the manifests.
    pub fn duration(&self) -> Duration {
        self.duration
    }
}

/// The outcome of [`commit_manifest_batches`](super::commit_manifest_batches).
///
/// `batches_committed + batches_failed + batches_dropped` is the number of batches in the
/// plan. Nothing was committed if `batches_committed` is `0`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CommittedManifestRewrite {
    /// The table after the commit, or the table passed in if nothing was committed.
    pub table: Table,
    /// Batches whose outputs replaced their inputs in the new snapshot.
    pub batches_committed: usize,
    /// Batches whose rewrite returned an error, plus all batches of a group whose written
    /// entry count didn't match its inputs' file counts.
    pub batches_failed: usize,
    /// Batches that were rewritten successfully but not committed because they share an
    /// input manifest, directly or through other batches, with a failed batch.
    pub batches_dropped: usize,
}

/// The entries of a batch's partition spec that a batch is responsible for, as a range of
/// the first partition field's value.
///
/// Bounds compare as the field's partition-result type; `Datum` carries that type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FirstFieldRange {
    /// Non-null values `v` with `lower <= v < upper`; a `None` bound is unbounded.
    Values {
        lower: Option<Datum>,
        upper: Option<Datum>,
    },
    /// Entries whose first field is null.
    Nulls,
    /// Every entry, null or not. Used when the first field's range is unknown or the
    /// spec has no partition fields.
    All,
}

impl FirstFieldRange {
    /// Returns whether an entry with this partition belongs to the range.
    ///
    /// An `Int` entry value is compared with a `Long` bound, and a `Float` value with a
    /// `Double` bound, as numbers: manifests written before an `int` to `long` or `float`
    /// to `double` promotion of the source column still hold the narrower value. Date,
    /// time and timestamp bounds are never widened, since a day count is not a timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Execution`] if the first field is missing or isn't a
    /// primitive of the bounds' type, which means the entry doesn't match the spec the
    /// plan was made for.
    pub fn contains(&self, partition: &Struct) -> Result<bool> {
        let (lower, upper) = match self {
            FirstFieldRange::All => return Ok(true),
            FirstFieldRange::Nulls => {
                return Ok(first_field(partition)?.is_none());
            }
            FirstFieldRange::Values { lower, upper } => (lower, upper),
        };
        let Some(value) = first_field(partition)? else {
            return Ok(false);
        };
        let Literal::Primitive(value) = value else {
            return Err(CompactionError::Execution(format!(
                "First partition field is not a primitive: {value:?}"
            )));
        };
        let above_lower = match lower {
            Some(lower) => compare(value, lower)? != Ordering::Less,
            None => true,
        };
        let below_upper = match upper {
            Some(upper) => compare(value, upper)? == Ordering::Less,
            None => true,
        };
        Ok(above_lower && below_upper)
    }
}

fn first_field(partition: &Struct) -> Result<Option<&Literal>> {
    partition
        .fields()
        .first()
        .map(Option::as_ref)
        .ok_or_else(|| {
            CompactionError::Execution(
                "Entry has no partition fields but the batch has a range".to_owned(),
            )
        })
}

/// Compares an entry's first-field value with a range bound using [`order`], widening
/// the value first if the bound is a `long` or `double` and the value an `int` or `float`.
fn compare(value: &PrimitiveLiteral, bound: &Datum) -> Result<Ordering> {
    let widened = match (value, bound.data_type()) {
        (PrimitiveLiteral::Int(v), PrimitiveType::Long) => Some(Datum::long(i64::from(*v))),
        (PrimitiveLiteral::Float(v), PrimitiveType::Double) => Some(Datum::double(f64::from(v.0))),
        _ => None,
    };
    let value = widened.as_ref().map_or(value, Datum::literal);
    if discriminant(value) != discriminant(bound.literal()) {
        return Err(CompactionError::Execution(format!(
            "First partition field {value:?} doesn't match the range bound type {:?}",
            bound.literal()
        )));
    }
    Ok(order(value, bound.literal()))
}

/// Total order of two literals of the same variant. Planning and
/// [`FirstFieldRange::contains`] both use it, so they agree on which side of a bound a
/// value is.
pub(super) fn order(a: &PrimitiveLiteral, b: &PrimitiveLiteral) -> Ordering {
    a.partial_cmp(b).unwrap_or(Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(v: i32) -> Datum {
        Datum::int(v)
    }

    fn partition(value: Option<Literal>) -> Struct {
        Struct::from_iter([value])
    }

    fn int_partition(v: i32) -> Struct {
        partition(Some(Literal::int(v)))
    }

    #[test]
    fn range_contains_is_half_open_with_separate_null_handling() {
        let bounded = FirstFieldRange::Values {
            lower: Some(int(10)),
            upper: Some(int(20)),
        };
        let unbounded_below = FirstFieldRange::Values {
            lower: None,
            upper: Some(int(20)),
        };
        let unbounded_above = FirstFieldRange::Values {
            lower: Some(int(10)),
            upper: None,
        };
        let unbounded = FirstFieldRange::Values {
            lower: None,
            upper: None,
        };

        // (range, [(value, expected)]) for values 9, 10, 19, 20 and null.
        let cases = [
            ("bounded", &bounded, [false, true, true, false, false]),
            ("unbounded below", &unbounded_below, [
                true, true, true, false, false,
            ]),
            ("unbounded above", &unbounded_above, [
                false, true, true, true, false,
            ]),
            ("unbounded", &unbounded, [true, true, true, true, false]),
            ("nulls", &FirstFieldRange::Nulls, [
                false, false, false, false, true,
            ]),
            ("all", &FirstFieldRange::All, [true; 5]),
        ];
        for (name, range, expected) in cases {
            let actual = [
                range.contains(&int_partition(9)).unwrap(),
                range.contains(&int_partition(10)).unwrap(),
                range.contains(&int_partition(19)).unwrap(),
                range.contains(&int_partition(20)).unwrap(),
                range.contains(&partition(None)).unwrap(),
            ];
            assert_eq!(actual, expected, "{name}");
        }
    }

    #[test]
    fn range_contains_rejects_entries_that_do_not_match_the_spec() {
        let range = FirstFieldRange::Values {
            lower: Some(int(10)),
            upper: None,
        };
        let long_value = partition(Some(Literal::long(10)));
        assert_eq!(
            range.contains(&long_value).unwrap_err().to_string(),
            "Execution failed: First partition field Long(10) doesn't match the range bound \
             type Int(10)"
        );
        assert_eq!(
            range.contains(&Struct::empty()).unwrap_err().to_string(),
            "Execution failed: Entry has no partition fields but the batch has a range"
        );
        assert!(FirstFieldRange::All.contains(&Struct::empty()).unwrap());

        // Narrowing, and widening to a type that isn't a promotion of the value's, are
        // errors: a day count is not a timestamp.
        let timestamp_range = FirstFieldRange::Values {
            lower: Some(Datum::timestamp_micros(10)),
            upper: None,
        };
        assert_eq!(
            timestamp_range
                .contains(&int_partition(10))
                .unwrap_err()
                .to_string(),
            "Execution failed: First partition field Int(10) doesn't match the range bound \
             type Long(10)"
        );
        let float_range = FirstFieldRange::Values {
            lower: Some(Datum::float(1.0)),
            upper: None,
        };
        assert_eq!(
            float_range
                .contains(&partition(Some(Literal::double(1.0))))
                .unwrap_err()
                .to_string(),
            "Execution failed: First partition field Double(1.0) doesn't match the range bound \
             type Float(1.0)"
        );
    }

    #[test]
    fn range_contains_widens_values_written_before_a_type_promotion() {
        let long_range = FirstFieldRange::Values {
            lower: Some(Datum::long(10)),
            upper: Some(Datum::long(20)),
        };
        let double_range = FirstFieldRange::Values {
            lower: Some(Datum::double(1.5)),
            upper: Some(Datum::double(2.5)),
        };
        let double_unbounded_above = FirstFieldRange::Values {
            lower: Some(Datum::double(1.5)),
            upper: None,
        };
        let contains = |range: &FirstFieldRange, value: Literal| {
            range.contains(&partition(Some(value))).unwrap()
        };

        // Int values against Long bounds: 9, 10, 19, 20, as written before and after.
        let ints = [9, 10, 19, 20];
        assert_eq!(ints.map(|v| contains(&long_range, Literal::int(v))), [
            false, true, true, false
        ]);
        assert_eq!(
            ints.map(|v| contains(&long_range, Literal::long(i64::from(v)))),
            [false, true, true, false]
        );
        // Float values against Double bounds: 1.0, 1.5, 2.0, 2.5, NaN, with NaN last.
        let floats = [1.0f32, 1.5, 2.0, 2.5, f32::NAN];
        assert_eq!(
            floats.map(|v| contains(&double_range, Literal::float(v))),
            [false, true, true, false, false]
        );
        assert_eq!(
            floats.map(|v| contains(&double_unbounded_above, Literal::float(v))),
            [false, true, true, true, true]
        );
    }

    #[test]
    fn plan_types_are_owned_and_sendable() {
        fn assert_owned<T: Clone + Send + Sync + std::fmt::Debug + 'static>() {}
        assert_owned::<ManifestRewritePlan>();
        assert_owned::<ManifestBatch>();
        assert_owned::<FirstFieldRange>();
        assert_owned::<BatchedRewriteManifestsConfig>();
        assert_owned::<RewrittenManifestBatch>();
    }
}
