use ic_service_compactor::{config::Config, server::grpc_compactor_serve};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use std::{env, net::SocketAddr, path::PathBuf};

#[tokio::main]
async fn main() {
    let config_path = find_config_file();

    let config = Config::from_file(config_path).unwrap();
    unsafe {
        env::set_var("RUST_LOG", &config.logging.level);
    }
    tracing_subscriber::registry().with(fmt::layer()).init();

    // read ip and port from env
    let listen_addr = SocketAddr::new(config.server.host, config.server.port);
    let join_handle = grpc_compactor_serve(listen_addr).await;
    tracing::info!("Start server successful {:?}", listen_addr);

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

fn find_config_file() -> PathBuf {
    let current_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let possible_paths = [
        current_dir.join("config.yaml"),
        current_dir.join("services/compactor/config.yaml"),
        PathBuf::from("/app/config.yaml"),
    ];

    for path in possible_paths.iter() {
        if path.exists() {
            return path.clone();
        }
    }

    current_dir.join("config.yaml")
}
