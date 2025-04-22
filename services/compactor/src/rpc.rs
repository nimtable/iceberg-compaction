use ic_codegen::compactor::compactor_service_server::CompactorService;
use ic_codegen::compactor::{
    EchoRequest, EchoResponse, RewriteFilesRequest as PbRewriteFilesRequest,
    RewriteFilesResponse as PbRewriteFilesResponse,
};
use ic_core::CompactionExecutor;
use ic_core::executor::DataFusionExecutor;

#[derive(Default)]
pub struct CompactorServiceImpl;

#[async_trait::async_trait]
impl CompactorService for CompactorServiceImpl {
    async fn rewrite_files(
        &self,
        request: tonic::Request<PbRewriteFilesRequest>,
    ) -> std::result::Result<tonic::Response<PbRewriteFilesResponse>, tonic::Status> {
        let request = request.into_inner();
        let response = DataFusionExecutor::rewrite_file_proto(request)
            .await
            .map_err(|e| {
                tracing::error!("Error processing request: {:?}", e);
                tonic::Status::internal(format!("Internal error: {}", e))
            })?;
        Ok(tonic::Response::new(response))
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
