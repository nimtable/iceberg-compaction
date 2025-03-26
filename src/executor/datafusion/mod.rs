use async_trait::async_trait;
use iceberg::TableIdent;
use std::sync::Arc;

// use datafusion::prelude::*;
use super::*;

pub struct DataFusionExecutor {
    // ctx: SessionContext,
}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn compact(
        &self,
        _table: &TableIdent,
        _input_files: Vec<DataFile>,
        _config: Arc<CompactionConfig>,
    ) -> Result<Vec<DataFile>, CompactionError> {
        unimplemented!("DataFusionExecutor::compact")
    }
}

impl DataFusionExecutor {
    pub fn new() -> Self {
        Self {}
    }
}
