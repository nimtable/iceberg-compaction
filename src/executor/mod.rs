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
pub mod util;

#[async_trait]
pub trait CompactionExecutor: Send + Sync + 'static {
    async fn compact(
        &self,
        file_id: FileIO,
        schema: Schema,
        input_file_scan_tasks: AllFileScanTasks,
        config: Arc<CompactionConfig>,
        dir_path: String,
    ) -> Result<Vec<DataFile>, CompactionError>;
}

pub struct AllFileScanTasks {
    data_files: Vec<FileScanTask>,
    position_delete_files: Vec<FileScanTask>,
    equality_delete_files: Vec<FileScanTask>,
}
