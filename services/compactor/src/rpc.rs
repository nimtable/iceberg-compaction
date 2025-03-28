use ic_prost::compactor::compactor_service_server::CompactorService;
use ic_prost::compactor::{RewriteFilesRequest, RewriteFilesResponse};

#[derive(Default)]
pub struct CompactorServiceImpl;

#[async_trait::async_trait]
impl CompactorService for CompactorServiceImpl {
    async fn rewrite_files(
        &self,
        _request: tonic::Request<RewriteFilesRequest>,
    ) -> std::result::Result<tonic::Response<RewriteFilesResponse>, tonic::Status> {
        //TODO: compact the input files with executor

        Ok(tonic::Response::new(RewriteFilesResponse {}))
    }
}
