use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;

use crate::config::CompactionConfig;
use crate::error::CompactionError;
use iceberg::spec::{DataFile, Schema};

pub mod mock;
pub use mock::MockExecutor;
pub mod datafusion;
pub use datafusion::DataFusionExecutor;

#[async_trait]
pub trait CompactionExecutor: Send + Sync + 'static {
    async fn rewrite_files(
        file_io: FileIO,
        schema: Arc<Schema>,
        input_file_scan_tasks: AllFileScanTasks,
        config: Arc<CompactionConfig>,
        dir_path: String,
    ) -> Result<Vec<DataFile>, CompactionError>;
}

pub struct AllFileScanTasks {
    pub data_files: Vec<FileScanTask>,
    pub position_delete_files: Vec<FileScanTask>,
    pub equality_delete_files: Vec<FileScanTask>,
}
