use crate::rpc::CompactorServiceImpl;
use ic_prost::compactor::compactor_service_server::CompactorServiceServer;
use std::net::SocketAddr;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use tower_http::cors::{Any, CorsLayer};

pub async fn grpc_compactor_serve(
    listen_addr: SocketAddr,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    let compactor_srv = CompactorServiceImpl::default();

    let server = Server::builder()
        .add_service(CompactorServiceServer::new(compactor_srv))
        .serve(listen_addr);

    tokio::spawn(server)
}

pub async fn http_compactor_serve(
    listen_addr: SocketAddr,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    let compactor_service_impl = CompactorServiceImpl::default();
    let service = CompactorServiceServer::new(compactor_service_impl);

    let server = Server::builder()
        .accept_http1(true)
        // This will apply the gRPC-Web translation layer
        .layer(CorsLayer::new().allow_origin(Any))
        .layer(GrpcWebLayer::new())
        .add_service(service)
        .serve(listen_addr);

    tokio::spawn(server)
}
