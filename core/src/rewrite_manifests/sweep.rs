//! First-field extents of manifests and the sweep that cuts them into key ranges.
//!
//! The sweep only picks cut values. Callers turn consecutive cuts into half-open ranges
//! `(-inf, c1)`, `[c1, c2)`, ..., `[cn, +inf)`, which partition every possible value.

use std::cmp::{Ordering, Reverse};

use iceberg::spec::{Datum, ManifestFile, PrimitiveLiteral, PrimitiveType};

use super::types::order;

/// Keys a manifest can hold on the first partition field, from its summary.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Extent {
    Known {
        /// Inclusive bounds of the non-null values; `None` if there are none.
        values: Option<(Datum, Datum)>,
        has_null: bool,
    },
    /// The summary gives no usable bounds; the manifest may hold any key.
    Wide,
}

impl Extent {
    /// Returns whether the two extents may hold a common key. The planner uses
    /// [`OverlapIndex`]; this pairwise form is its test oracle.
    #[cfg(test)]
    pub(super) fn overlaps(&self, other: &Extent) -> bool {
        match (self, other) {
            (Extent::Wide, _) | (_, Extent::Wide) => true,
            (
                Extent::Known {
                    values: a,
                    has_null: a_null,
                },
                Extent::Known {
                    values: b,
                    has_null: b_null,
                },
            ) => {
                let values_overlap = match (a, b) {
                    (Some((a_lower, a_upper)), Some((b_lower, b_upper))) => {
                        order(a_lower.literal(), b_upper.literal()) != Ordering::Greater
                            && order(b_lower.literal(), a_upper.literal()) != Ordering::Greater
                    }
                    _ => false,
                };
                values_overlap || (*a_null && *b_null)
            }
        }
    }
}

/// Reads the first partition field's summary of `manifest`, whose values have type
/// `first_field`. Anything that can't be trusted yields [`Extent::Wide`]: a missing
/// summary, NaN, a missing or undecodable bound, or inverted bounds. For a `float` or
/// `double` field only `contains_nan == Some(false)` is trusted, because a writer that
/// leaves it unset may have left NaN out of the bounds.
pub(super) fn decode_extent(manifest: &ManifestFile, first_field: &PrimitiveType) -> Extent {
    let Some(summary) = manifest.partitions.as_ref().and_then(|p| p.first()) else {
        return Extent::Wide;
    };
    let is_floating = matches!(first_field, PrimitiveType::Float | PrimitiveType::Double);
    if summary.contains_nan == Some(true) || (is_floating && summary.contains_nan.is_none()) {
        return Extent::Wide;
    }
    match (&summary.lower_bound, &summary.upper_bound) {
        (None, None) if summary.contains_null => Extent::Known {
            values: None,
            has_null: true,
        },
        (Some(lower), Some(upper)) => {
            let decode = |bytes: &[u8]| Datum::try_from_bytes(bytes, first_field.clone()).ok();
            match (decode(lower), decode(upper)) {
                (Some(lower), Some(upper))
                    if order(lower.literal(), upper.literal()) != Ordering::Greater =>
                {
                    Extent::Known {
                        values: Some((lower, upper)),
                        has_null: summary.contains_null,
                    }
                }
                _ => Extent::Wide,
            }
        }
        _ => Extent::Wide,
    }
}

/// The union of a set of extents, for asking whether another extent overlaps any of them
/// in `O(log n)` instead of comparing against each. Equivalent to
/// `extents.iter().any(|e| e.overlaps(other))`.
#[derive(Debug, Default)]
pub(super) struct OverlapIndex {
    /// Some extent is wide, and so overlaps every extent.
    any_wide: bool,
    has_null: bool,
    /// Sorted, disjoint inclusive value intervals; sorted by both bounds.
    intervals: Vec<(Datum, Datum)>,
    any: bool,
}

impl OverlapIndex {
    pub(super) fn new<'a>(extents: impl IntoIterator<Item = &'a Extent>) -> Self {
        let mut index = Self::default();
        let mut values: Vec<(&Datum, &Datum)> = Vec::new();
        for extent in extents {
            index.any = true;
            match extent {
                Extent::Wide => index.any_wide = true,
                Extent::Known {
                    values: bounds,
                    has_null,
                } => {
                    index.has_null |= has_null;
                    if let Some((lower, upper)) = bounds {
                        values.push((lower, upper));
                    }
                }
            }
        }
        values.sort_by(|a, b| order(a.0.literal(), b.0.literal()));
        for (lower, upper) in values {
            match index.intervals.last_mut() {
                Some((_, last_upper)) if order(lower.literal(), last_upper.literal()).is_le() => {
                    if order(upper.literal(), last_upper.literal()).is_gt() {
                        *last_upper = upper.clone();
                    }
                }
                _ => index.intervals.push((lower.clone(), upper.clone())),
            }
        }
        index
    }

    /// Returns whether `extent` may hold a key that some indexed extent may hold.
    pub(super) fn overlaps(&self, extent: &Extent) -> bool {
        if !self.any {
            return false;
        }
        let Extent::Known { values, has_null } = extent else {
            return true;
        };
        if self.any_wide || (*has_null && self.has_null) {
            return true;
        }
        let Some((lower, upper)) = values else {
            return false;
        };
        // The first interval ending at or after `lower` is the only one that can reach it.
        let first = self
            .intervals
            .partition_point(|(_, end)| order(end.literal(), lower.literal()).is_lt());
        self.intervals
            .get(first)
            .is_some_and(|(start, _)| order(start.literal(), upper.literal()).is_le())
    }
}

/// A manifest's non-null value bounds and how much it weighs against a batch's limits.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ValueExtent {
    pub(super) lower: Datum,
    pub(super) upper: Datum,
    pub(super) weight: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Limits {
    /// A range is cut before its weight reaches this.
    pub(super) target_entries: u64,
    pub(super) max_manifests: usize,
}

/// Picks the values at which to cut the value line into ranges, ascending.
///
/// A manifest is read by every range its `[lower, upper]` intersects, but a range only
/// weighs the share of each manifest it is estimated to write (see
/// [`weight_below_lowers`]), and only counts the manifests starting inside it against the
/// manifest limit. Counting the manifests straddling in from the left in full would not
/// shrink as ranges get smaller, so once they alone filled a limit every lower bound
/// would become a cut, and each range would read them again. Starting at the lowest
/// value, each range is extended over the candidate cuts (the distinct lower bounds) while
/// it stays under both limits. Among those cuts that fill at least half of a limit, the
/// one with the fewest straddling manifests wins, ties going to the largest range; if none
/// fills half, the largest range wins. When even the first candidate breaks a limit it is
/// taken anyway, so progress is guaranteed and an oversized manifest ends up in a batch of
/// its own. No cut is made where all remaining lower bounds are equal.
pub(super) fn choose_cuts(mut extents: Vec<ValueExtent>, limits: Limits) -> Vec<Datum> {
    extents.sort_by(|a, b| {
        order(a.lower.literal(), b.lower.literal())
            .then_with(|| order(a.upper.literal(), b.upper.literal()))
    });
    let n = extents.len();
    let weight_below = weight_below_lowers(&extents);
    let target = limits.target_entries as f64;
    let mut uppers: Vec<&Datum> = extents.iter().map(|e| &e.upper).collect();
    uppers.sort_by(|a, b| order(a.literal(), b.literal()));
    // Index of the first manifest of each run of equal lower bounds, except the first run.
    let group_starts: Vec<usize> = (1..n)
        .filter(|&g| {
            order(extents[g].lower.literal(), extents[g - 1].lower.literal()) != Ordering::Equal
        })
        .collect();
    let straddling = |end: usize| {
        let ended = uppers.partition_point(|upper| {
            order(upper.literal(), extents[end].lower.literal()) == Ordering::Less
        });
        end - ended
    };

    let mut cuts: Vec<Datum> = Vec::new();
    let mut start = 0;
    loop {
        // `start` and `end` are 0, `n` or the first manifest of a run of equal lower bounds,
        // where `weight_below` is the weight below the range's bound.
        let size = |end: usize| (weight_below[end] - weight_below[start], end - start);
        let fits = |end: usize| {
            let (weight, count) = size(end);
            weight < target && count <= limits.max_manifests
        };
        if fits(n) {
            break;
        }
        let first_candidate = group_starts.partition_point(|&g| g <= start);
        let candidates = &group_starts[first_candidate..];
        let Some(&smallest) = candidates.first() else {
            break;
        };
        let within_limits = &candidates[..candidates.partition_point(|&g| fits(g))];
        let end = match within_limits.last() {
            None => smallest,
            Some(&largest) => within_limits
                .iter()
                .copied()
                .filter(|&g| {
                    let (weight, count) = size(g);
                    weight * 2.0 >= target || count.saturating_mul(2) >= limits.max_manifests
                })
                .min_by_key(|&g| (straddling(g), Reverse(g)))
                .unwrap_or(largest),
        };
        cuts.push(extents[end].lower.clone());
        start = end;
    }
    cuts
}

/// How a manifest's weight spreads over the values between its bounds.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Scale {
    /// Integer-valued types: over each value in `[lower, upper]`.
    Discrete,
    /// Floating point: over the interval `[lower, upper]`.
    Continuous,
}

/// The position of `datum` on a numeric scale, for types that have one: integers, dates,
/// times, timestamps and decimals are discrete, floating point is continuous.
fn position(datum: &Datum) -> Option<(Scale, f64)> {
    let (scale, value) = match datum.literal() {
        PrimitiveLiteral::Int(v) => (Scale::Discrete, f64::from(*v)),
        PrimitiveLiteral::Long(v) => (Scale::Discrete, *v as f64),
        PrimitiveLiteral::Int128(v) => (Scale::Discrete, *v as f64),
        PrimitiveLiteral::Float(v) => (Scale::Continuous, f64::from(v.0)),
        PrimitiveLiteral::Double(v) => (Scale::Continuous, v.0),
        _ => return None,
    };
    value.is_finite().then_some((scale, value))
}

/// The estimated weight of the values below each manifest's lower bound, for `extents`
/// sorted by lower bound, followed by the total weight. Only the entries at the first
/// manifest of each run of equal lower bounds, and the last, are meaningful.
///
/// If all bounds have the same numeric scale, a manifest's weight is spread evenly over
/// its values: each value in `[lower, upper]` for integer-valued types, or the interval
/// for floating point, where `lower == upper` is a single point. A range then weighs the
/// share of the manifest it covers, and the shares of a manifest add up to its weight.
/// Otherwise, as for strings, a manifest weighs only on the range where it starts.
pub(super) fn weight_below_lowers(extents: &[ValueExtent]) -> Vec<f64> {
    let n = extents.len();
    let weights: Vec<f64> = extents.iter().map(|e| e.weight as f64).collect();
    let mut below = vec![0.0; n + 1];

    let numeric = extents.first().and_then(|first| {
        let scale = position(&first.lower)?.0;
        let bounds = extents
            .iter()
            .map(|e| match (position(&e.lower)?, position(&e.upper)?) {
                ((lower_scale, lower), (upper_scale, upper))
                    if lower_scale == scale && upper_scale == scale =>
                {
                    Some((lower, upper))
                }
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        Some((scale, bounds))
    });
    let Some((scale, bounds)) = numeric else {
        for (i, weight) in weights.iter().enumerate() {
            below[i + 1] = below[i] + weight;
        }
        return below;
    };

    // Each manifest spreads `density` per unit over `[from, to)`, or is a point at `from`
    // (`density` is `None`) whose weight is all below any greater value. Positions are
    // offset from the lowest bound to keep `x * density` small.
    let origin = bounds[0].0;
    let spreads: Vec<(f64, f64, Option<f64>)> = bounds
        .iter()
        .zip(&weights)
        .map(|(&(lower, upper), &weight)| {
            let from = lower - origin;
            let width = match scale {
                Scale::Discrete => upper - lower + 1.0,
                Scale::Continuous => upper - lower,
            };
            if width > 0.0 {
                (from, from + width, Some(weight / width))
            } else {
                (from, from, None)
            }
        })
        .collect();
    let mut by_end: Vec<usize> = (0..n).filter(|&i| spreads[i].2.is_some()).collect();
    by_end.sort_by(|&a, &b| spreads[a].1.total_cmp(&spreads[b].1));

    // Below `x`: the weight of manifests ending at or before it, plus
    // `x * density - Σ density * from` of those it is inside.
    let (mut finished, mut density, mut offset) = (0.0, 0.0, 0.0);
    let (mut entered, mut exited) = (0, 0);
    for i in 0..n {
        let x = spreads[i].0;
        while entered < n && spreads[entered].0 < x {
            match spreads[entered].2 {
                Some(d) => {
                    density += d;
                    offset += d * spreads[entered].0;
                }
                None => finished += weights[entered],
            }
            entered += 1;
        }
        while exited < by_end.len() && spreads[by_end[exited]].1 <= x {
            let j = by_end[exited];
            let d = spreads[j].2.unwrap_or_default();
            density -= d;
            offset -= d * spreads[j].0;
            finished += weights[j];
            exited += 1;
        }
        let estimate = finished + x * density - offset;
        below[i] = if i == 0 {
            0.0
        } else {
            estimate.max(below[i - 1])
        };
    }
    below[n] = weights.iter().sum::<f64>().max(below[n.saturating_sub(1)]);
    below
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{ByteBuf, FieldSummary, ManifestContentType};

    use super::*;

    fn int(v: i32) -> Datum {
        Datum::int(v)
    }

    fn extent(lower: i32, upper: i32, weight: u64) -> ValueExtent {
        ValueExtent {
            lower: int(lower),
            upper: int(upper),
            weight,
        }
    }

    fn limits(target_entries: u64, max_manifests: usize) -> Limits {
        Limits {
            target_entries,
            max_manifests,
        }
    }

    fn cut_values(extents: Vec<ValueExtent>, limits: Limits) -> Vec<i32> {
        choose_cuts(extents, limits)
            .iter()
            .map(|cut| match cut.literal() {
                iceberg::spec::PrimitiveLiteral::Int(v) => *v,
                other => panic!("unexpected cut {other:?}"),
            })
            .collect()
    }

    fn manifest_with(summaries: Option<Vec<FieldSummary>>) -> ManifestFile {
        ManifestFile {
            manifest_path: "m.avro".to_owned(),
            manifest_length: 0,
            partition_spec_id: 0,
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
            partitions: summaries,
            key_metadata: None,
            first_row_id: None,
        }
    }

    fn summary(lower: Option<&[u8]>, upper: Option<&[u8]>, null: bool) -> FieldSummary {
        FieldSummary {
            contains_null: null,
            contains_nan: Some(false),
            lower_bound: lower.map(|b| ByteBuf::from(b.to_vec())),
            upper_bound: upper.map(|b| ByteBuf::from(b.to_vec())),
        }
    }

    fn known(lower: i32, upper: i32, has_null: bool) -> Extent {
        Extent::Known {
            values: Some((int(lower), int(upper))),
            has_null,
        }
    }

    #[test]
    fn decode_extent_reads_the_first_field_summary() {
        let b = |v: i32| v.to_le_bytes();
        let nan = FieldSummary {
            contains_nan: Some(true),
            ..summary(Some(&b(1)), Some(&b(2)), false)
        };
        let cases: Vec<(&str, Option<Vec<FieldSummary>>, Extent)> = vec![
            (
                "bounds",
                Some(vec![summary(Some(&b(1)), Some(&b(5)), false)]),
                known(1, 5, false),
            ),
            (
                "bounds and null",
                Some(vec![summary(Some(&b(1)), Some(&b(5)), true)]),
                known(1, 5, true),
            ),
            (
                "only the first field is read",
                Some(vec![
                    summary(Some(&b(2)), Some(&b(2)), false),
                    summary(Some(b"x"), Some(b"y"), false),
                ]),
                known(2, 2, false),
            ),
            (
                "all null",
                Some(vec![summary(None, None, true)]),
                Extent::Known {
                    values: None,
                    has_null: true,
                },
            ),
            ("no summaries", None, Extent::Wide),
            ("no fields", Some(vec![]), Extent::Wide),
            ("NaN", Some(vec![nan]), Extent::Wide),
            (
                "no values and no null",
                Some(vec![summary(None, None, false)]),
                Extent::Wide,
            ),
            (
                "one bound",
                Some(vec![summary(Some(&b(1)), None, false)]),
                Extent::Wide,
            ),
            (
                "undecodable bound",
                Some(vec![summary(Some(b"abc"), Some(&b(1)), false)]),
                Extent::Wide,
            ),
            (
                "inverted bounds",
                Some(vec![summary(Some(&b(5)), Some(&b(1)), false)]),
                Extent::Wide,
            ),
        ];
        for (name, summaries, expected) in cases {
            assert_eq!(
                decode_extent(&manifest_with(summaries), &PrimitiveType::Int),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn decode_extent_trusts_floating_point_summaries_only_without_nan() {
        let float = |nan: Option<bool>| FieldSummary {
            contains_nan: nan,
            ..summary(
                Some(&1.0f32.to_le_bytes()),
                Some(&2.0f32.to_le_bytes()),
                false,
            )
        };
        let double = |nan: Option<bool>| FieldSummary {
            contains_nan: nan,
            ..summary(
                Some(&1.0f64.to_le_bytes()),
                Some(&2.0f64.to_le_bytes()),
                false,
            )
        };
        let int_summary = |nan: Option<bool>| FieldSummary {
            contains_nan: nan,
            ..summary(Some(&1i32.to_le_bytes()), Some(&2i32.to_le_bytes()), false)
        };
        let known = |lower: Datum, upper: Datum| Extent::Known {
            values: Some((lower, upper)),
            has_null: false,
        };

        // (type, summary, expected): only an explicit `false` is trusted for floating
        // point; other types never report NaN.
        let cases = [
            (
                PrimitiveType::Float,
                float(Some(false)),
                known(Datum::float(1.0), Datum::float(2.0)),
            ),
            (PrimitiveType::Float, float(None), Extent::Wide),
            (PrimitiveType::Float, float(Some(true)), Extent::Wide),
            (
                PrimitiveType::Double,
                double(Some(false)),
                known(Datum::double(1.0), Datum::double(2.0)),
            ),
            (PrimitiveType::Double, double(None), Extent::Wide),
            (PrimitiveType::Double, double(Some(true)), Extent::Wide),
            (PrimitiveType::Int, int_summary(None), known(int(1), int(2))),
        ];
        for (ty, field_summary, expected) in cases {
            assert_eq!(
                decode_extent(&manifest_with(Some(vec![field_summary.clone()])), &ty),
                expected,
                "{ty:?} {field_summary:?}"
            );
        }
    }

    #[test]
    fn decode_extent_widens_bounds_written_before_a_type_promotion() {
        let four = |v: i32| v.to_le_bytes();
        let promoted_long = summary(Some(&four(-5)), Some(&four(7)), false);
        let promoted_double = FieldSummary {
            lower_bound: Some(ByteBuf::from((-1.5f32).to_le_bytes().to_vec())),
            upper_bound: Some(ByteBuf::from(2.5f32.to_le_bytes().to_vec())),
            ..summary(None, None, false)
        };
        for (ty, field_summary, expected) in [
            (
                PrimitiveType::Long,
                promoted_long,
                (Datum::long(-5), Datum::long(7)),
            ),
            (
                PrimitiveType::Double,
                promoted_double,
                (Datum::double(-1.5), Datum::double(2.5)),
            ),
        ] {
            assert_eq!(
                decode_extent(&manifest_with(Some(vec![field_summary])), &ty),
                Extent::Known {
                    values: Some(expected),
                    has_null: false
                },
                "{ty:?}"
            );
        }
    }

    #[test]
    fn extents_overlap_on_shared_values_or_nulls() {
        let null_only = Extent::Known {
            values: None,
            has_null: true,
        };
        let cases = [
            ("disjoint", known(1, 2, false), known(3, 4, false), false),
            (
                "touching bounds",
                known(1, 3, false),
                known(3, 4, false),
                true,
            ),
            ("contained", known(1, 9, false), known(4, 4, false), true),
            (
                "null does not meet value",
                null_only.clone(),
                known(1, 2, false),
                false,
            ),
            (
                "null meets null",
                null_only.clone(),
                known(1, 2, true),
                true,
            ),
            (
                "wide meets anything",
                Extent::Wide,
                known(1, 2, false),
                true,
            ),
            ("wide meets null", null_only, Extent::Wide, true),
        ];
        for (name, a, b, expected) in cases {
            assert_eq!(a.overlaps(&b), expected, "{name}");
            assert_eq!(b.overlaps(&a), expected, "{name}, reversed");
        }
    }

    #[test]
    fn choose_cuts_prefers_the_cut_with_fewest_straddling_manifests() {
        // Each weighs 10 and the target is 45, so up to four fit and a range is cut once
        // it holds at least three. Cutting at 5 would make the larger range but straddle
        // [4,6]; cutting at 4 straddles nothing.
        let extents = vec![
            extent(1, 1, 10),
            extent(2, 2, 10),
            extent(3, 3, 10),
            extent(4, 6, 10),
            extent(5, 5, 10),
        ];
        assert_eq!(cut_values(extents, limits(45, 100)), vec![4]);
    }

    #[test]
    fn choose_cuts_cases() {
        let ones = |lowers: &[i32]| -> Vec<ValueExtent> {
            lowers.iter().map(|&v| extent(v, v, 1)).collect()
        };
        let cases: Vec<(&str, Vec<ValueExtent>, Limits, Vec<i32>)> = vec![
            (
                "everything fits",
                ones(&[1, 2, 3]),
                limits(100, 100),
                vec![],
            ),
            ("empty", vec![], limits(1, 1), vec![]),
            (
                "manifest count limit",
                ones(&[1, 2, 3, 4, 5, 6, 7]),
                limits(100, 3),
                vec![4, 7],
            ),
            (
                "entry limit is strict: reaching the target cuts",
                ones(&[1, 2, 3, 4]),
                limits(2, 100),
                vec![2, 3, 4],
            ),
            (
                "an oversized manifest is its own range",
                vec![extent(1, 1, 1), extent(2, 2, 1000), extent(3, 3, 1)],
                limits(10, 100),
                vec![2, 3],
            ),
            (
                "equal lower bounds can't be cut",
                vec![extent(7, 7, 100), extent(7, 7, 100), extent(7, 7, 100)],
                limits(10, 2),
                vec![],
            ),
            (
                "unsorted input",
                ones(&[5, 1, 4, 2, 3]),
                limits(100, 2),
                vec![3, 5],
            ),
            (
                // [1,9] weighs 10/9 per value, so (-inf,4) weighs 3.3 of it plus 20; the
                // rest of it and [4,4] fit in [4,+inf).
                "straddling manifest weighs its share on each range",
                vec![
                    extent(1, 9, 10),
                    extent(2, 2, 10),
                    extent(3, 3, 10),
                    extent(4, 4, 10),
                ],
                limits(25, 100),
                vec![4],
            ),
            (
                // [1,100] weighs 1 per value and every ten values add a manifest of 10,
                // so ranges of 20 values weigh about 40. Counting all of [1,100] on every
                // range it reaches would cut at every lower bound.
                "a manifest spanning every range doesn't force a cut at every lower bound",
                std::iter::once(extent(1, 100, 100))
                    .chain((1..=10).map(|i| extent(i * 10, i * 10, 10)))
                    .collect(),
                limits(45, 100),
                vec![20, 40, 60, 80, 100],
            ),
            (
                // Every manifest spans five values, so up to four straddle each cut; only
                // the three starting in a range count against its limit.
                "only manifests starting in a range count against the manifest limit",
                (1..=10).map(|lower| extent(lower, lower + 4, 1)).collect(),
                limits(1000, 3),
                vec![3, 6, 9],
            ),
        ];
        for (name, extents, limits, expected) in cases {
            assert_eq!(cut_values(extents, limits), expected, "{name}");
        }
    }

    #[test]
    fn weight_below_lowers_spreads_numeric_manifests_over_their_values() {
        let value = |lower: Datum, upper: Datum, weight: u64| ValueExtent {
            lower,
            upper,
            weight,
        };
        let cases: Vec<(&str, Vec<ValueExtent>, Vec<f64>)> = vec![
            (
                // [1,4] has 4 values of 10; [3,3] is all below 5.
                "integers",
                vec![extent(1, 4, 40), extent(3, 3, 10), extent(5, 6, 20)],
                vec![0.0, 20.0, 50.0, 70.0],
            ),
            (
                "longs, as for timestamps",
                vec![
                    value(Datum::long(1_000), Datum::long(1_999), 100),
                    value(Datum::long(1_250), Datum::long(1_250), 7),
                ],
                vec![0.0, 25.0, 107.0],
            ),
            (
                // [0,10] spreads 10 per unit; [2.5,2.5] is a point, all below 5.
                "floating point",
                vec![
                    value(Datum::double(0.0), Datum::double(10.0), 100),
                    value(Datum::double(2.5), Datum::double(2.5), 10),
                    value(Datum::double(5.0), Datum::double(5.0), 10),
                ],
                vec![0.0, 25.0, 60.0, 120.0],
            ),
            (
                "strings weigh where they start",
                vec![
                    value(Datum::string("a"), Datum::string("m"), 30),
                    value(Datum::string("c"), Datum::string("c"), 10),
                    value(Datum::string("x"), Datum::string("z"), 5),
                ],
                vec![0.0, 30.0, 40.0, 45.0],
            ),
            ("empty", vec![], vec![0.0]),
        ];
        for (name, extents, expected) in cases {
            assert_eq!(weight_below_lowers(&extents), expected, "{name}");
        }
    }

    #[test]
    fn choose_cuts_weighs_non_numeric_manifests_where_they_start() {
        let string = |lower: &str, upper: &str, weight: u64| ValueExtent {
            lower: Datum::string(lower),
            upper: Datum::string(upper),
            weight,
        };
        // ["a","z"] counts only on the first range, so the rest fits in one.
        let extents = vec![
            string("a", "z", 100),
            string("b", "b", 10),
            string("c", "c", 10),
            string("d", "d", 10),
        ];
        assert_eq!(choose_cuts(extents, limits(45, 100)), vec![Datum::string(
            "b"
        )]);
    }

    #[test]
    fn overlap_index_matches_pairwise_overlap() {
        // Deterministic xorshift, so a failure reproduces.
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let random_extent = |next: &mut dyn FnMut(u64) -> u64| match next(8) {
            0 => Extent::Wide,
            1 => Extent::Known {
                values: None,
                has_null: true,
            },
            kind => {
                let lower = next(40) as i32;
                known(lower, lower + next(6) as i32, kind == 2)
            }
        };
        let mut overlapping = 0;
        for _ in 0..500 {
            let indexed: Vec<Extent> = (0..next(6)).map(|_| random_extent(&mut next)).collect();
            let index = OverlapIndex::new(&indexed);
            for _ in 0..20 {
                let probe = random_extent(&mut next);
                let expected = indexed.iter().any(|e| e.overlaps(&probe));
                assert_eq!(index.overlaps(&probe), expected, "{indexed:?} vs {probe:?}");
                overlapping += usize::from(expected);
            }
        }
        // Both outcomes are exercised.
        assert_eq!(overlapping, 5_337);
    }
}
