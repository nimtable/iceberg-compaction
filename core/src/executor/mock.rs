use super::*;
use async_trait::async_trait;

pub struct MockExecutor;

#[async_trait]
impl CompactionExecutor for MockExecutor {
    async fn rewrite_files(
        _file_io: FileIO,
        _schema: Arc<Schema>,
        _input_file_scan_tasks: InputFileScanTasks,
        _config: Arc<CompactionConfig>,
        _dir_path: String,
        _partition_spec: Arc<PartitionSpec>,
    ) -> Result<CompactionResult, CompactionError> {
        Ok(CompactionResult::default())
    }
}
