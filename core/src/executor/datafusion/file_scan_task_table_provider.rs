use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;

use super::iceberg_file_task_scan::IcebergFileTaskScan;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct IcebergFileScanTaskTableProvider {
    file_scan_tasks: Vec<FileScanTask>,
    schema: ArrowSchemaRef,
    file_io: FileIO,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    batch_parallelism: usize,
}
impl IcebergFileScanTaskTableProvider {
    pub fn new(
        file_scan_tasks: Vec<FileScanTask>,
        schema: ArrowSchemaRef,
        file_io: &FileIO,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Self {
        Self {
            file_scan_tasks,
            schema,
            file_io: file_io.clone(),
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        }
    }
}
#[async_trait]
impl TableProvider for IcebergFileScanTaskTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IcebergFileTaskScan::new(
            self.file_scan_tasks.clone(),
            self.schema.clone(),
            projection,
            filters,
            &self.file_io,
            self.need_seq_num,
            self.need_file_path_and_pos,
            self.batch_parallelism,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError>
    {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
