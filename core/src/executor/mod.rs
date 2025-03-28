use std::sync::Arc;

use async_trait::async_trait;

use crate::config::CompactionConfig;
use crate::error::CompactionError;
use iceberg::spec::DataFile;
use iceberg::table::Table;

pub mod mock;
pub use mock::MockExecutor;

pub mod datafusion;
pub use datafusion::DataFusionExecutor;

#[async_trait]
pub trait CompactionExecutor: Send + Sync + 'static {
    async fn compact(
        &self,
        table: Table,
        input_files: Vec<DataFile>,
        config: Arc<CompactionConfig>,
    ) -> Result<Vec<DataFile>, CompactionError>;
}
