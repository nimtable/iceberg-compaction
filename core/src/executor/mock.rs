use super::*;
use async_trait::async_trait;

pub struct MockExecutor;

#[async_trait]
impl CompactionExecutor for MockExecutor {
    async fn rewrite_files(
        _file_io: FileIO,
        _schema: Arc<Schema>,
        _input_file_scan_tasks: AllFileScanTasks,
        _config: Arc<CompactionConfig>,
        _dir_path: String,
    ) -> Result<Vec<DataFile>, CompactionError> {
        Ok(vec![])
    }
}
