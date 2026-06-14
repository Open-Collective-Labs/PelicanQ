use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use base64::Engine;
use pelicanq_core::error::PelicanError;
use pelicanq_core::message::DeliveryTag;
use pelicanq_core::message::Message;
use pelicanq_core::queue::QueueManager;
use pelicanq_core::PublishOutcome;
use pelicanq_raft::QueueOperation;
use pelicanq_raft::QueueOperationResponse;
use pelicanq_raft::WriteResult;
use serde::{Deserialize, Serialize};

use crate::cluster_config::ClusterConfig;

/// Execution engine: Solo (direct QueueManager, Phases 1-2) or Flock (Raft, Phase 3+).
pub enum AppEngine {
    /// Solo mode — direct, non-replicated queue operations.
    Solo(Arc<Mutex<QueueManager>>),
    /// Clustered mode — all mutations go through Raft.
    Flock(pelicanq_raft::FlockHandle),
}

/// Shared application state held by all route handlers.
pub struct AppState {
    pub engine: AppEngine,
    pub cluster: Option<ClusterConfig>,
}

pub type SharedState = Arc<AppState>;

#[derive(Serialize)]
struct HealthResponse {
    status: String,
}

#[derive(Serialize)]
struct QueueListResponse {
    queues: Vec<QueueEntry>,
}

#[derive(Serialize)]
struct QueueEntry {
    name: String,
    depth: usize,
}

#[derive(Deserialize)]
struct PublishRequest {
    payload_base64: String,
    #[serde(default)]
    headers: HashMap<String, String>,
}

#[derive(Deserialize)]
struct AckNackRequest {
    delivery_tag: DeliveryTag,
}

fn map_error(e: PelicanError) -> (StatusCode, Json<serde_json::Value>) {
    let body = serde_json::json!({"error": e.to_string()});
    match &e {
        PelicanError::QueueNotFound { .. } => (StatusCode::NOT_FOUND, Json(body)),
        PelicanError::QueueAlreadyExists { .. } => (StatusCode::CONFLICT, Json(body)),
        PelicanError::InvalidDeliveryTag { .. } => (StatusCode::BAD_REQUEST, Json(body)),
        PelicanError::StorageLimitExceeded { .. } => {
            (StatusCode::PAYLOAD_TOO_LARGE, Json(body))
        }
        PelicanError::MessageDeadLettered { .. } => (StatusCode::OK, Json(body)),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, Json(body)),
    }
}

fn not_leader_response(leader_node: Option<pelicanq_raft::Node>) -> Response {
    let mut resp = (
        StatusCode::MISDIRECTED_REQUEST,
        Json(serde_json::json!({"error": "not leader"})),
    )
        .into_response();
    if let Some(node) = leader_node {
        if !node.client_addr.is_empty() {
            resp.headers_mut().insert(
                HeaderName::from_static("x-pelican-leader-addr"),
                HeaderValue::from_str(&node.client_addr).unwrap(),
            );
        }
    }
    resp
}

fn flock_error_response(msg: String) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error": msg})),
    )
        .into_response()
}

async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok".to_string(),
        }),
    )
}

async fn declare_queue(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Response {
    match &state.engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            match mgr.declare_queue(&name) {
                Ok(()) => (StatusCode::CREATED, Json(serde_json::Value::Null)).into_response(),
                Err(e) => map_error(e).into_response(),
            }
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::DeclareQueue {
                name,
                policy: Default::default(),
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::DeclareQueue(Ok(()))) => {
                    (StatusCode::CREATED, Json(serde_json::Value::Null)).into_response()
                }
                WriteResult::Ok(QueueOperationResponse::DeclareQueue(Err(e))) => {
                    map_error(e).into_response()
                }
                WriteResult::NotLeader { leader_node } => not_leader_response(leader_node),
                WriteResult::Error(msg) => flock_error_response(msg),
                _ => flock_error_response("unexpected response type".into()),
            }
        }
    }
}

async fn list_queues(State(state): State<SharedState>) -> Response {
    match &state.engine {
        AppEngine::Solo(qm_arc) => {
            let mgr = qm_arc.lock().unwrap();
            let names = mgr.list_queues();
            let mut queues = Vec::with_capacity(names.len());
            for name in names {
                let depth = mgr.depth(&name).unwrap_or(0);
                queues.push(QueueEntry { name, depth });
            }
            (StatusCode::OK, Json(QueueListResponse { queues })).into_response()
        }
        AppEngine::Flock(flock) => {
            flock
                .with_qm(|mgr| {
                    let names = mgr.list_queues();
                    let mut queues = Vec::with_capacity(names.len());
                    for name in names {
                        let depth = mgr.depth(&name).unwrap_or(0);
                        queues.push(QueueEntry { name, depth });
                    }
                    (StatusCode::OK, Json(QueueListResponse { queues }))
                })
                .await
                .into_response()
        }
    }
}

async fn publish(
    State(state): State<SharedState>,
    Path(name): Path<String>,
    Json(req): Json<PublishRequest>,
) -> Response {
    let payload = match base64::engine::general_purpose::STANDARD.decode(&req.payload_base64) {
        Ok(bytes) => bytes,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("invalid base64: {e}")})),
            )
                .into_response();
        }
    };

    let message = Message::new(payload, req.headers);
    let msg_id = message.id;

    match &state.engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            match mgr.publish(&name, message) {
                Ok(PublishOutcome::Stored(_)) => {
                    let resp = serde_json::json!({"id": msg_id.to_string()});
                    (StatusCode::CREATED, Json(resp)).into_response()
                }
                Ok(PublishOutcome::Deduplicated) => {
                    (StatusCode::OK, Json(serde_json::json!({"status": "deduplicated"})))
                        .into_response()
                }
                Err(e) => map_error(e).into_response(),
            }
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::Publish {
                queue: name,
                message,
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::Publish(Ok(PublishOutcome::Stored(_)))) => {
                    let resp = serde_json::json!({"id": msg_id.to_string()});
                    (StatusCode::CREATED, Json(resp)).into_response()
                }
                WriteResult::Ok(QueueOperationResponse::Publish(Ok(
                    PublishOutcome::Deduplicated,
                ))) => {
                    (StatusCode::OK, Json(serde_json::json!({"status": "deduplicated"})))
                        .into_response()
                }
                WriteResult::Ok(QueueOperationResponse::Publish(Err(e))) => {
                    map_error(e).into_response()
                }
                WriteResult::NotLeader { leader_node } => not_leader_response(leader_node),
                WriteResult::Error(msg) => flock_error_response(msg),
                _ => flock_error_response("unexpected response type".into()),
            }
        }
    }
}

async fn consume(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Response {
    match &state.engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            match mgr.consume(&name) {
                Ok(Some((tag, msg))) => {
                    let payload_b64 =
                        base64::engine::general_purpose::STANDARD.encode(&msg.payload);
                    let resp = serde_json::json!({
                        "delivery_tag": tag,
                        "id": msg.id.to_string(),
                        "payload_base64": payload_b64,
                        "headers": msg.headers,
                        "timestamp": msg.timestamp,
                    });
                    (StatusCode::OK, Json(resp)).into_response()
                }
                Ok(None) => {
                    (StatusCode::NO_CONTENT, Json(serde_json::Value::Null)).into_response()
                }
                Err(e) => map_error(e).into_response(),
            }
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::Consume { queue: name };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::Consume(Ok(Some((tag, msg))))) => {
                    let payload_b64 =
                        base64::engine::general_purpose::STANDARD.encode(&msg.payload);
                    let resp = serde_json::json!({
                        "delivery_tag": tag,
                        "id": msg.id.to_string(),
                        "payload_base64": payload_b64,
                        "headers": msg.headers,
                        "timestamp": msg.timestamp,
                    });
                    (StatusCode::OK, Json(resp)).into_response()
                }
                WriteResult::Ok(QueueOperationResponse::Consume(Ok(None))) => {
                    (StatusCode::NO_CONTENT, Json(serde_json::Value::Null)).into_response()
                }
                WriteResult::Ok(QueueOperationResponse::Consume(Err(e))) => {
                    map_error(e).into_response()
                }
                WriteResult::NotLeader { leader_node } => not_leader_response(leader_node),
                WriteResult::Error(msg) => flock_error_response(msg),
                _ => flock_error_response("unexpected response type".into()),
            }
        }
    }
}

async fn ack(
    State(state): State<SharedState>,
    Path(name): Path<String>,
    Json(req): Json<AckNackRequest>,
) -> Response {
    match &state.engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            match mgr.ack(&name, req.delivery_tag) {
                Ok(()) => (StatusCode::OK, Json(serde_json::Value::Null)).into_response(),
                Err(e) => map_error(e).into_response(),
            }
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::Ack {
                queue: name,
                tag: req.delivery_tag,
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::Ack(Ok(()))) => {
                    (StatusCode::OK, Json(serde_json::Value::Null)).into_response()
                }
                WriteResult::Ok(QueueOperationResponse::Ack(Err(e))) => {
                    map_error(e).into_response()
                }
                WriteResult::NotLeader { leader_node } => not_leader_response(leader_node),
                WriteResult::Error(msg) => flock_error_response(msg),
                _ => flock_error_response("unexpected response type".into()),
            }
        }
    }
}

async fn nack(
    State(state): State<SharedState>,
    Path(name): Path<String>,
    Json(req): Json<AckNackRequest>,
) -> Response {
    match &state.engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            match mgr.nack(&name, req.delivery_tag) {
                Ok(()) => (StatusCode::OK, Json(serde_json::Value::Null)).into_response(),
                Err(e) => map_error(e).into_response(),
            }
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::Nack {
                queue: name,
                tag: req.delivery_tag,
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::Nack(Ok(()))) => {
                    (StatusCode::OK, Json(serde_json::Value::Null)).into_response()
                }
                WriteResult::Ok(QueueOperationResponse::Nack(Err(e))) => {
                    map_error(e).into_response()
                }
                WriteResult::NotLeader { leader_node } => not_leader_response(leader_node),
                WriteResult::Error(msg) => flock_error_response(msg),
                _ => flock_error_response("unexpected response type".into()),
            }
        }
    }
}

async fn cluster_status(State(state): State<SharedState>) -> Response {
    match &state.cluster {
        Some(cfg) => {
            let resp = if matches!(state.engine, AppEngine::Flock(_)) {
                serde_json::json!({
                    "self_id": cfg.self_id,
                    "members": cfg.members,
                    "reads_may_lag": true,
                })
            } else {
                serde_json::json!({
                    "self_id": cfg.self_id,
                    "members": cfg.members,
                })
            };
            (StatusCode::OK, Json(resp)).into_response()
        }
        None => (StatusCode::NOT_FOUND, Json(serde_json::Value::Null)).into_response(),
    }
}

/// Builds the router. In Solo mode (cluster config is `None`), the
/// `/cluster/status` route is not registered.
pub fn build_router(state: SharedState) -> Router {
    let mut router = Router::new()
        .route("/health", axum::routing::get(health))
        .route("/queues", axum::routing::get(list_queues))
        .route("/queues/:name", axum::routing::post(declare_queue))
        .route("/queues/:name/publish", axum::routing::post(publish))
        .route("/queues/:name/consume", axum::routing::post(consume))
        .route("/queues/:name/ack", axum::routing::post(ack))
        .route("/queues/:name/nack", axum::routing::post(nack));

    if state.cluster.is_some() {
        router = router.route("/cluster/status", axum::routing::get(cluster_status));
    }

    router.with_state(state)
}
