use async_trait::async_trait;

use super::{CompactionExecutor, RewriteFilesRequest, RewriteFilesResponse};
use crate::error::Result;

pub struct MockExecutor;

#[async_trait]
impl CompactionExecutor for MockExecutor {
    async fn rewrite_files(_request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        Ok(RewriteFilesResponse::default())
    }
}
