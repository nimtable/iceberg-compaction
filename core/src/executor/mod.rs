use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;

use crate::parser::proto::RewriteFilesResponseProtoEncoder;
use crate::{config::CompactionConfig, parser::proto::PbRewriteFilesRequestDecoder};
use iceberg::spec::{DataFile, Schema};

pub mod mock;
pub use mock::MockExecutor;
pub mod datafusion;
use crate::error::Result;
pub use datafusion::DataFusionExecutor;
use ic_codegen::compactor::RewriteFilesRequest as PbRewriteFilesRequest;
use ic_codegen::compactor::RewriteFilesResponse as PbRewriteFilesResponse;

#[async_trait]
pub trait CompactionExecutor: Send + Sync + 'static {
    async fn rewrite_files(request: RewriteFilesRequest) -> Result<RewriteFilesResponse>;

    async fn rewrite_file_proto(request: PbRewriteFilesRequest) -> Result<PbRewriteFilesResponse> {
        let request = PbRewriteFilesRequestDecoder::new(request).decode()?;
        let response = Self::rewrite_files(request).await?;
        let response = RewriteFilesResponseProtoEncoder::new(response).encode();
        Ok(response)
    }
}

pub struct RewriteFilesRequest {
    pub file_io: FileIO,
    pub schema: Arc<Schema>,
    pub input_file_scan_tasks: InputFileScanTasks,
    pub config: Arc<CompactionConfig>,
    pub dir_path: String,
}

pub struct InputFileScanTasks {
    pub data_files: Vec<FileScanTask>,
    pub position_delete_files: Vec<FileScanTask>,
    pub equality_delete_files: Vec<FileScanTask>,
}

impl InputFileScanTasks {
    pub fn input_files_count(&self) -> u32 {
        self.data_files.len() as u32
            + self.position_delete_files.len() as u32
            + self.equality_delete_files.len() as u32
    }
}

#[derive(Debug, Clone, Default)]
pub struct RewriteFilesResponse {
    pub data_files: Vec<DataFile>,
    pub stat: RewriteFilesStat,
}

#[derive(Debug, Clone, Default)]
pub struct RewriteFilesStat {
    pub rewritten_files_count: u32,
    pub added_files_count: u32,
    pub rewritten_bytes: u64,
    pub failed_data_files_count: u32,
}
