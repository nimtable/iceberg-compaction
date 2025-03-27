use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::vec;

use async_stream::try_stream;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef as ArrowSchemaRef};
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg_datafusion::physical_plan::expr_to_predicate::convert_filters_to_predicate;
use iceberg_datafusion::to_datafusion_error;

#[derive(Debug)]
pub(crate) struct IcebergFileTaskScan {
    file_scan_tasks: Vec<FileScanTask>,
    plan_properties: PlanProperties,
    projection: Option<Vec<String>>,
    predicates: Option<Predicate>,
    file_io: FileIO,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
}

impl IcebergFileTaskScan {
    pub(crate) fn new(
        file_scan_tasks: Vec<FileScanTask>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        file_io: &FileIO,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let plan_properties = Self::compute_properties(output_schema.clone());
        let projection = get_column_names(schema.clone(), projection);
        let predicates = convert_filters_to_predicate(filters);

        Self {
            file_scan_tasks,
            plan_properties,
            projection,
            predicates,
            file_io: file_io.clone(),
            need_seq_num,
            need_file_path_and_pos,
        }
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for IcebergFileTaskScan {
    fn name(&self) -> &str {
        "IcebergFileTaskScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let fut = get_batch_stream(
            self.file_io.clone(),
            self.file_scan_tasks.clone(),
            self.need_seq_num,
            self.need_file_path_and_pos,
        );
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

async fn get_batch_stream(
    file_io: FileIO,
    file_scan_tasks: Vec<FileScanTask>,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let stream = try_stream! {
        for task in file_scan_tasks {
            let file_path = task.data_file_path.clone();
            let data_file_content = task.data_file_content;
            let sequence_number = task.sequence_number;
            let task_stream = futures::stream::iter(vec![Ok(task)]).boxed();
            let arrow_reader_builder = ArrowReaderBuilder::new(file_io.clone());
            let mut batch_stream = arrow_reader_builder.build()
                .read(task_stream)
                .await
                .map_err(to_datafusion_error)?;
            let mut index_start = 0;
            while let Some(batch) = batch_stream.next().await {
                let mut batch = batch.map_err(to_datafusion_error)?;
                let batch = match data_file_content {
                    iceberg::spec::DataContentType::Data => {
                        if need_seq_num{
                            batch = add_seq_num_into_batch(batch, sequence_number)?;
                        }
                        if need_file_path_and_pos {
                            batch = add_file_path_pos_into_batch(batch, &file_path, index_start)?;
                            index_start += batch.num_rows() as i64;
                        }
                        batch
                    }
                    iceberg::spec::DataContentType::PositionDeletes => {
                        batch
                    },
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        add_seq_num_into_batch(batch, sequence_number)?
                    },
                };
                yield batch;
            }
        }
    };
    Ok(Box::pin(stream))
}

fn add_seq_num_into_batch(batch: RecordBatch, seq_num: i64) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let seq_num_field = Arc::new(Field::new(
        "seq_num",
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    let mut new_fields = schema.fields().to_vec();
    new_fields.push(seq_num_field);
    let new_schema = Arc::new(Schema::new(new_fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![seq_num; batch.num_rows()])));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))
}

fn add_file_path_pos_into_batch(
    batch: RecordBatch,
    file_path: &str,
    index_start: i64,
) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let file_path_field = Arc::new(Field::new(
        "file_path",
        datafusion::arrow::datatypes::DataType::Utf8,
        false,
    ));
    let pos_field = Arc::new(Field::new(
        "pos",
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    let mut new_fields = schema.fields().to_vec();
    new_fields.push(file_path_field);
    new_fields.push(pos_field);
    let new_schema = Arc::new(Schema::new(new_fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![
        file_path;
        batch.num_rows()
    ])));
    columns.push(Arc::new(Int64Array::from_iter(
        (index_start..(index_start + batch.num_rows() as i64)).collect::<Vec<i64>>(),
    )));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))
}

impl DisplayAs for IcebergFileTaskScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "IcebergTableScan projection:[{}] predicate:[{}]",
            self.projection
                .clone()
                .map_or(String::new(), |v| v.join(",")),
            self.predicates
                .clone()
                .map_or(String::from(""), |p| format!("{}", p))
        )
    }
}

pub fn get_column_names(
    schema: ArrowSchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<String>> {
    projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    })
}
