use crate::rpc::CompactorServiceImpl;
use ic_codegen::compactor::compactor_service_server::CompactorServiceServer;
use std::net::SocketAddr;
use tokio::task::JoinHandle;
use tonic::transport::Server;

pub async fn grpc_compactor_serve(
    listen_addr: SocketAddr,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    let compactor_srv = CompactorServiceImpl::default();

    let server = Server::builder()
        .add_service(CompactorServiceServer::new(compactor_srv))
        .serve(listen_addr);

    tokio::spawn(server)
}
