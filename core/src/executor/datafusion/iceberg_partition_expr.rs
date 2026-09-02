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

use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use iceberg::arrow::PartitionValueCalculator;
use iceberg::spec::{PartitionSpecRef, SchemaRef as IcebergSchemaRef};

use crate::error::Result;

/// Physical expression evaluating to the Iceberg partition value (the struct
/// of transformed partition fields under the given partition spec) for each
/// input row.
///
/// It is used as the hash key of a `RepartitionExec` so that all rows
/// belonging to one Iceberg partition are routed to exactly one output
/// stream. Compared to round-robin distribution, this keeps the number of
/// open fanout writers at one per distinct partition in total (instead of
/// `output streams x partitions`) and avoids slicing every partition's output
/// into one file per stream.
pub struct IcebergPartitionExpr {
    table_schema: IcebergSchemaRef,
    partition_spec: PartitionSpecRef,
    calculator: PartitionValueCalculator,
    partition_arrow_type: DataType,
}

impl IcebergPartitionExpr {
    /// Creates an expression computing partition values of `partition_spec`
    /// over batches whose layout matches `table_schema`.
    ///
    /// # Errors
    ///
    /// Returns an error if the spec is unpartitioned or a partition transform
    /// or source column projection cannot be constructed.
    pub fn try_new(
        table_schema: IcebergSchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<Self> {
        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema)?;
        let partition_arrow_type = calculator.partition_arrow_type().clone();
        Ok(Self {
            table_schema,
            partition_spec,
            calculator,
            partition_arrow_type,
        })
    }
}

impl PhysicalExpr for IcebergPartitionExpr {
    fn data_type(&self, _input_schema: &ArrowSchema) -> DFResult<DataType> {
        Ok(self.partition_arrow_type.clone())
    }

    fn nullable(&self, _input_schema: &ArrowSchema) -> DFResult<bool> {
        // Every row has a partition value struct; individual partition fields
        // may be null inside the struct.
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let array = self
            .calculator
            .calculate(batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(ColumnarValue::Array(array))
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
            "iceberg_partition_value(spec_id={})",
            self.partition_spec.spec_id()
        )
    }
}

impl Display for IcebergPartitionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IcebergPartitionExpr(spec_id={})",
            self.partition_spec.spec_id()
        )
    }
}

impl fmt::Debug for IcebergPartitionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergPartitionExpr")
            .field("partition_spec", &self.partition_spec)
            .finish_non_exhaustive()
    }
}

impl PartialEq for IcebergPartitionExpr {
    fn eq(&self, other: &Self) -> bool {
        self.partition_spec == other.partition_spec
            && self.table_schema.as_ref() == other.table_schema.as_ref()
    }
}

impl Eq for IcebergPartitionExpr {}

impl Hash for IcebergPartitionExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.partition_spec.spec_id().hash(state);
        self.table_schema.schema_id().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Int32Array, StringArray, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type};

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
                        "category",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        )
    }

    fn identity_bucket_spec(schema: &Schema) -> PartitionSpecRef {
        Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("category", "category", Transform::Identity)
                .unwrap()
                .add_partition_field("id", "id_bucket", Transform::Bucket(4))
                .unwrap()
                .build()
                .unwrap(),
        )
    }

    fn test_batch() -> RecordBatch {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
        ])
        .unwrap()
    }

    /// The expression evaluates to a struct of transformed partition values:
    /// identity keeps the source value, bucket maps equal inputs to equal
    /// bucket ids. Rows with equal partition values must produce equal structs
    /// (that is what the hash repartitioning keys on).
    #[test]
    fn test_evaluate_partition_values() {
        let schema = table_schema();
        let spec = identity_bucket_spec(&schema);
        let expr = IcebergPartitionExpr::try_new(schema, spec).unwrap();

        let batch = test_batch();
        assert_eq!(
            expr.data_type(&batch.schema()).unwrap(),
            expr.partition_arrow_type
        );

        let ColumnarValue::Array(array) = expr.evaluate(&batch).unwrap() else {
            panic!("expected array result");
        };
        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.len(), 4);
        assert_eq!(struct_array.num_columns(), 2);

        let categories = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(categories.value(0), "a");
        assert_eq!(categories.value(1), "b");

        let buckets = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..4 {
            let bucket = buckets.value(i);
            assert!((0..4).contains(&bucket), "bucket id out of range: {bucket}");
        }
    }

    /// Unpartitioned specs cannot be used as a repartitioning key.
    #[test]
    fn test_unpartitioned_spec_rejected() {
        let schema = table_schema();
        let spec = Arc::new(
            PartitionSpec::builder(schema.as_ref().clone())
                .with_spec_id(0)
                .build()
                .unwrap(),
        );
        assert!(IcebergPartitionExpr::try_new(schema, spec).is_err());
    }

    /// Equality and hash identify the expression by (schema, partition spec),
    /// as required by `DynEq`/`DynHash` for plan comparison.
    #[test]
    fn test_eq_by_schema_and_spec() {
        let schema = table_schema();
        let spec = identity_bucket_spec(&schema);
        let a = IcebergPartitionExpr::try_new(schema.clone(), spec.clone()).unwrap();
        let b = IcebergPartitionExpr::try_new(schema.clone(), spec).unwrap();
        assert_eq!(a, b);

        let other_spec = Arc::new(
            PartitionSpec::builder(schema.as_ref().clone())
                .with_spec_id(2)
                .add_partition_field("id", "id", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        let c = IcebergPartitionExpr::try_new(schema, other_spec).unwrap();
        assert_ne!(a, c);
    }
}
