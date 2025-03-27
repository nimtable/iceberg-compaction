use crate::rpc::CompactorServiceImpl;
use ic_prost::compactor::compactor_service_server::CompactorServiceServer;
use std::net::SocketAddr;

pub async fn compactor_serve(listen_addr: SocketAddr) {
    let compactor_srv = CompactorServiceImpl::default();

    let server = tonic::transport::Server::builder()
        .add_service(CompactorServiceServer::new(compactor_srv))
        .serve(listen_addr);

    let _server_handle = tokio::spawn(server);
}
