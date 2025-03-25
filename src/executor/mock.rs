use super::*;
use async_trait::async_trait;

pub struct MockExecutor;

#[async_trait]
impl CompactionExecutor for MockExecutor {
    async fn compact(
        &self,
        _table: Table,
        input_files: Vec<DataFile>,
        _config: Arc<CompactionConfig>,
    ) -> Result<Vec<DataFile>, CompactionError> {
        Ok(input_files)
    }

    async fn compact_table(
        &self,
        _table: Table,
        _config: Arc<CompactionConfig>,
    ) -> Result<Vec<DataFile>, CompactionError> {
        Ok(vec![])
    }
}
