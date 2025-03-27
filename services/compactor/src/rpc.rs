use futures::Stream;
use ic_prost::compactor::compactor_service_server::CompactorService;
use ic_prost::compactor::{RewriteFilesRequest, RewriteFilesResponse};

#[derive(Default)]
pub struct CompactorServiceImpl;

#[async_trait::async_trait]
impl CompactorService for CompactorServiceImpl {
    type RewriteFilesStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<RewriteFilesResponse, tonic::Status>> + Send>>;

    async fn rewrite_files(
        &self,
        _request: tonic::Request<tonic::Streaming<RewriteFilesRequest>>,
    ) -> std::result::Result<tonic::Response<Self::RewriteFilesStream>, tonic::Status> {
        //TODO: compact the input files with executor

        let empty_output_stream = futures::stream::empty();
        Ok(tonic::Response::new(Box::pin(empty_output_stream)))
    }
}
