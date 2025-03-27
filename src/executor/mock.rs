use super::*;
use async_trait::async_trait;

pub struct MockExecutor;

#[async_trait]
impl CompactionExecutor for MockExecutor {
    async fn compact(
        &self,
        _file_id: FileIO,
        _schema: Schema,
        _input_file_scan_tasks: AllFileScanTasks,
        _config: Arc<CompactionConfig>,
        _dir_path: String,
    ) -> Result<Vec<DataFile>, CompactionError> {
        Ok(vec![])
    }
}
