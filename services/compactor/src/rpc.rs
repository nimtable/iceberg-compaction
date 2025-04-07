use ic_codegen::compactor::compactor_service_server::CompactorService;
use ic_codegen::compactor::{EchoRequest, EchoResponse, RewriteFilesRequest, RewriteFilesResponse};

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

    async fn echo(
        &self,
        request: tonic::Request<EchoRequest>,
    ) -> std::result::Result<tonic::Response<EchoResponse>, tonic::Status> {
        tracing::info!("Echo request: {:?}", request);
        Ok(tonic::Response::new(EchoResponse {
            message: format!("Echo: {}", request.into_inner().message),
        }))
    }
}
