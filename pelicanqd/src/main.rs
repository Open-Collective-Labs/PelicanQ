mod api;
mod cluster_config;
mod config;
mod grpc;
mod mqtt;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use pelicanq_core::queue::QueueManager;

use crate::grpc::pb::admin_service_server::AdminServiceServer;
use crate::grpc::pb::queue_service_server::QueueServiceServer;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cfg = config::Config::from_env();
    let cluster = cfg.cluster;

    let state = if let Some(ref cluster_cfg) = cluster {
        let self_raft_addr = cluster_cfg
            .members
            .iter()
            .find(|m| m.id == cluster_cfg.self_id)
            .expect("self_id must be in cluster members")
            .raft_addr
            .clone();

        // Detect fresh cluster: no existing Raft state directory.
        let raft_path = cfg.data_dir.join("raft");
        let is_fresh = !raft_path.exists()
            || raft_path
                .read_dir()
                .map(|mut it| it.next().is_none())
                .unwrap_or(true);

        let (raft, store) = pelicanq_raft::start_node(
            cluster_cfg.self_id,
            &self_raft_addr,
            &cfg.data_dir,
        )
        .await
        .expect("failed to start Raft node");

        // Bootstrap convention: the lowest-ID node initializes the
        // cluster on first start. Other nodes just wait to be contacted.
        let lowest_id = cluster_cfg.members.iter().map(|m| m.id).min().unwrap();
        if is_fresh && cluster_cfg.self_id == lowest_id {
            let members: BTreeMap<u64, pelicanq_raft::Node> = cluster_cfg
                .members
                .iter()
                .map(|m| {
                    (
                        m.id,
                        pelicanq_raft::Node {
                            raft_addr: m.raft_addr.clone(),
                            client_addr: m.client_addr.clone(),
                        },
                    )
                })
                .collect();

            tracing::info!(
                "bootstrapping cluster: node {} initializing with {} members",
                lowest_id,
                members.len()
            );

            if let Err(e) = raft.initialize(members).await {
                // Continue as a follower; another node may have raced,
                // or the cluster is already initialized.
                tracing::warn!("cluster initialization for node {lowest_id} failed: {e}");
            }
        }

        tracing::info!(
            "cluster mode enabled: node {} on {}",
            cluster_cfg.self_id,
            cfg.listen_addr
        );

        Arc::new(api::AppState {
            engine: api::AppEngine::Flock(pelicanq_raft::FlockHandle::new(raft, store)),
            cluster,
        })
    } else {
        let queue_manager = QueueManager::open(&cfg.data_dir, cfg.max_bytes)
            .expect("failed to open queue storage");
        tracing::info!("solo mode: listening on {}", cfg.listen_addr);

        Arc::new(api::AppState {
            engine: api::AppEngine::Solo(Arc::new(Mutex::new(queue_manager))),
            cluster,
        })
    };

    let app = api::build_router(state.clone());

    let http_listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
        .await
        .expect("failed to bind HTTP address");

    tracing::info!("HTTP server listening on {}", cfg.listen_addr);

    let grpc_addr = cfg.grpc_addr.clone();
    let queue_svc = grpc::queue_service::QueueServiceImpl::new(state.clone());
    let admin_svc = grpc::admin_service::AdminServiceImpl::new(state.clone());

    let grpc_router = tonic::transport::Server::builder()
        .add_service(QueueServiceServer::new(queue_svc))
        .add_service(AdminServiceServer::new(admin_svc));

    tracing::info!("gRPC server listening on {}", grpc_addr);

    let http = axum::serve(http_listener, app);
    let grpc = grpc_router.serve(grpc_addr.parse().expect("invalid gRPC address"));

    tokio::select! {
        result = http => { result.expect("HTTP server error"); }
        result = grpc => { result.expect("gRPC server error"); }
        result = async {
            if let Some(ref mqtt_addr) = cfg.mqtt_addr {
                mqtt::handler::listen(state.clone(), mqtt_addr.clone()).await;
            }
            Ok::<_, std::convert::Infallible>(())
        } => { result.unwrap(); }
    }
}
