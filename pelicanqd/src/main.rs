mod api;
mod config;

use std::sync::{Arc, Mutex};

use pelicanq_core::queue::QueueManager;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cfg = config::Config::from_env();
    let queue_manager = QueueManager::open(&cfg.data_dir, cfg.max_bytes)
        .expect("failed to open queue storage");
    let state = Arc::new(Mutex::new(queue_manager));

    let app = api::build_router(state);

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
        .await
        .expect("failed to bind address");

    tracing::info!("listening on {}", cfg.listen_addr);

    axum::serve(listener, app)
        .await
        .expect("server error");
}
