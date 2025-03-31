use ic_service_compactor::server::{grpc_compactor_serve, http_compactor_serve};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry().with(fmt::layer()).init();

    // read ip and port from env
    let listen_addr = {
        let default_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let default_port = 7777;

        let ip_str = std::env::var("ADDR_IP").unwrap_or_else(|_| default_ip.to_string());
        let ip: IpAddr = ip_str.parse().unwrap_or(default_ip);

        let port_str = std::env::var("ADDR_PORT").unwrap_or_else(|_| default_port.to_string());
        let port: u16 = port_str.parse().unwrap_or(default_port);

        SocketAddr::new(ip, port)
    };

    let server_type = std::env::var("SERVER_TYPE").unwrap_or_else(|_| "grpc".to_string());

    let join_handle = match server_type.as_str() {
        "grpc" => {
            tracing::info!("Starting gRPC server...");
            grpc_compactor_serve(listen_addr).await
        }
        "http" => {
            tracing::info!("Starting HTTP server...");
            http_compactor_serve(listen_addr).await
        }
        _ => panic!("Unknown server type: {}", server_type),
    };

    tracing::info!("Start {} server successful", server_type);

    // join_handle
    match join_handle.await {
        Ok(_) => {
            tracing::info!("Server stopped gracefully");
        }
        Err(e) => {
            tracing::error!("Server stopped with error: {}", e);
        }
    }
}
