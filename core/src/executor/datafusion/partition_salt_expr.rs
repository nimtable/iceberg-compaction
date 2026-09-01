/*
 * Copyright 2025 iceberg-compaction
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use iceberg::arrow::{PartitionValueCalculator, arrow_struct_to_literal};
use iceberg::spec::{Literal, PartitionSpecRef, SchemaRef as IcebergSchemaRef, Struct, StructType};

use crate::error::Result;

/// Per-partition split state: the split factor `k` and a row counter used to
/// deal salts round-robin within the partition.
#[derive(Debug)]
struct SplitLane {
    factor: u64,
    counter: AtomicU64,
}

/// Physical expression producing a bounded salt for oversized partitions.
///
/// Used as a secondary hash key next to [`IcebergPartitionExpr`] in a
/// `RepartitionExec`: the primary key routes each Iceberg partition to one
/// bucket, which caps the effective sort/write parallelism at the number of
/// distinct partitions in the group. For a partition whose input bytes exceed
/// the target file size, this expression widens the routing key to
/// `(partition value, salt)` with `salt < k`, spreading that partition over up
/// to `k` output streams.
///
/// File-count invariance: an unsalted partition of `bytes` input rolls into
/// `ceil(bytes / target)` files anyway. With `k = ceil(bytes / target)` each
/// salt lane carries roughly `bytes / k <= target`, so the total file count
/// stays the same while the sizes become more even. Partition purity of output
/// files is unaffected: each output stream writes through a partition-aware
/// fanout writer, so any row-to-stream routing is correct (round-robin
/// distribution relied on the same property).
///
/// The salt is dealt round-robin per partition from an internal counter, so
/// `evaluate` is intentionally not a pure function of the batch: two
/// evaluations of the same rows may yield different salts. `RepartitionExec`
/// evaluates its hash expressions exactly once per input batch, and any
/// resulting distribution is correct (see above), so this only affects
/// routing balance, never output content.
///
/// Rows whose partition value has no entry in the split table (smaller
/// partitions, or values unseen at plan time) get salt `0`, preserving the
/// one-bucket-per-partition behavior.
pub struct PartitionSaltExpr {
    table_schema: IcebergSchemaRef,
    partition_spec: PartitionSpecRef,
    calculator: PartitionValueCalculator,
    partition_type: StructType,
    split_lanes: HashMap<Struct, SplitLane>,
}

impl PartitionSaltExpr {
    /// Creates a salt expression over batches whose layout matches
    /// `table_schema`, splitting the partitions listed in `split_factors`
    /// (partition value -> number of salt lanes, each expected to be >= 2).
    ///
    /// # Errors
    ///
    /// Returns an error if the spec is unpartitioned or a partition transform
    /// or source column projection cannot be constructed.
    pub fn try_new(
        table_schema: IcebergSchemaRef,
        partition_spec: PartitionSpecRef,
        split_factors: HashMap<Struct, u32>,
    ) -> Result<Self> {
        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema)?;
        let partition_type = calculator.partition_type().clone();
        let split_lanes = split_factors
            .into_iter()
            .map(|(partition, factor)| {
                (partition, SplitLane {
                    factor: u64::from(factor.max(1)),
                    counter: AtomicU64::new(0),
                })
            })
            .collect();
        Ok(Self {
            table_schema,
            partition_spec,
            calculator,
            partition_type,
            split_lanes,
        })
    }

    fn salts_for(&self, partition_values: Vec<Option<Literal>>) -> UInt64Array {
        UInt64Array::from_iter_values(partition_values.into_iter().map(|value| match value {
            Some(Literal::Struct(partition)) => {
                self.split_lanes.get(&partition).map_or(0, |lane| {
                    lane.counter.fetch_add(1, Ordering::Relaxed) % lane.factor
                })
            }
            // The partition expression is non-nullable in practice; treat
            // anything unexpected as "do not split".
            _ => 0,
        }))
    }
}

impl PhysicalExpr for PartitionSaltExpr {
    fn data_type(&self, _input_schema: &ArrowSchema) -> DFResult<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &ArrowSchema) -> DFResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let partition_array = self
            .calculator
            .calculate(batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partition_values = arrow_struct_to_literal(&partition_array, &self.partition_type)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(ColumnarValue::Array(Arc::new(
            self.salts_for(partition_values),
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "iceberg_partition_salt(spec_id={}, salted_partitions={})",
            self.partition_spec.spec_id(),
            self.split_lanes.len()
        )
    }
}

impl Display for PartitionSaltExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartitionSaltExpr(spec_id={}, salted_partitions={})",
            self.partition_spec.spec_id(),
            self.split_lanes.len()
        )
    }
}

impl fmt::Debug for PartitionSaltExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionSaltExpr")
            .field("partition_spec", &self.partition_spec)
            .field("split_lanes", &self.split_lanes)
            .finish_non_exhaustive()
    }
}

impl PartialEq for PartitionSaltExpr {
    fn eq(&self, other: &Self) -> bool {
        self.partition_spec == other.partition_spec
            && self.table_schema.as_ref() == other.table_schema.as_ref()
            && self.split_lanes.len() == other.split_lanes.len()
            && self.split_lanes.iter().all(|(partition, lane)| {
                other
                    .split_lanes
                    .get(partition)
                    .is_some_and(|other_lane| other_lane.factor == lane.factor)
            })
    }
}

impl Eq for PartitionSaltExpr {}

impl Hash for PartitionSaltExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.partition_spec.spec_id().hash(state);
        self.table_schema.schema_id().hash(state);
        self.split_lanes.len().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::{
        NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Transform, Type,
    };

    use super::*;

    fn table_schema() -> IcebergSchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "p",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                ])
                .build()
                .unwrap(),
        )
    }

    fn identity_spec(schema: &Schema) -> PartitionSpecRef {
        Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("p", "p", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        )
    }

    fn partition_value(value: i32) -> Struct {
        Struct::from_iter(vec![Some(Literal::Primitive(PrimitiveLiteral::Int(value)))])
    }

    fn batch_with_partitions(values: &[i32]) -> RecordBatch {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("p", DataType::Int32, false),
        ]));
        let ids = (0..values.len() as i32).collect::<Int32Array>();
        RecordBatch::try_new(arrow_schema, vec![
            Arc::new(ids),
            Arc::new(Int32Array::from(values.to_vec())),
        ])
        .unwrap()
    }

    /// Salts of a split partition stay below the split factor and cycle
    /// through every lane; unsplit partitions always get salt 0.
    #[test]
    fn test_salts_bounded_and_exhaustive() {
        let schema = table_schema();
        let spec = identity_spec(&schema);
        let expr =
            PartitionSaltExpr::try_new(schema, spec, HashMap::from([(partition_value(1), 3u32)]))
                .unwrap();

        // 32 rows of the hot partition interleaved with the cold partition.
        let values = (0..64)
            .map(|i| if i % 2 == 0 { 1 } else { 2 })
            .collect::<Vec<_>>();
        let ColumnarValue::Array(array) = expr.evaluate(&batch_with_partitions(&values)).unwrap()
        else {
            panic!("expected array result");
        };
        let salts = array.as_any().downcast_ref::<UInt64Array>().unwrap();

        let mut hot_seen = std::collections::HashSet::new();
        for (i, value) in values.iter().enumerate() {
            let salt = salts.value(i);
            if *value == 1 {
                assert!(salt < 3, "salt {salt} out of range");
                hot_seen.insert(salt);
            } else {
                assert_eq!(salt, 0, "unsplit partition must not be salted");
            }
        }
        assert_eq!(hot_seen.len(), 3, "all salt lanes should be used");
    }

    /// The internal counter continues across batches, so consecutive batches
    /// keep spreading rows instead of restarting every lane at 0.
    #[test]
    fn test_counter_spans_batches() {
        let schema = table_schema();
        let spec = identity_spec(&schema);
        let expr =
            PartitionSaltExpr::try_new(schema, spec, HashMap::from([(partition_value(7), 2u32)]))
                .unwrap();

        let mut seen = std::collections::HashSet::new();
        for _ in 0..2 {
            let ColumnarValue::Array(array) = expr.evaluate(&batch_with_partitions(&[7])).unwrap()
            else {
                panic!("expected array result");
            };
            let salts = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            assert_eq!(salts.len(), 1);
            seen.insert(salts.value(0));
        }
        assert_eq!(seen, std::collections::HashSet::from([0, 1]));
    }

    /// Equality identifies the expression by (schema, spec, split factors),
    /// ignoring counter state, as required by `DynEq` for plan comparison.
    #[test]
    fn test_eq_ignores_counter_state() {
        let schema = table_schema();
        let spec = identity_spec(&schema);
        let factors = HashMap::from([(partition_value(1), 4u32)]);
        let a = PartitionSaltExpr::try_new(schema.clone(), spec.clone(), factors.clone()).unwrap();
        let b = PartitionSaltExpr::try_new(schema.clone(), spec.clone(), factors).unwrap();
        // Advance one counter; the expressions must still compare equal.
        a.evaluate(&batch_with_partitions(&[1, 1])).unwrap();
        assert_eq!(a, b);

        let c =
            PartitionSaltExpr::try_new(schema, spec, HashMap::from([(partition_value(1), 5u32)]))
                .unwrap();
        assert_ne!(a, c);
    }
}
