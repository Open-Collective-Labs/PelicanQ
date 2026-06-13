use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Json, Router};
use base64::Engine;
use pelicanq_core::error::PelicanError;
use pelicanq_core::message::DeliveryTag;
use pelicanq_core::message::Message;
use pelicanq_core::queue::QueueManager;
use pelicanq_core::PublishOutcome;
use serde::{Deserialize, Serialize};

pub type AppState = Arc<Mutex<QueueManager>>;

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
        PelicanError::QueueNotFound(_) => {
            (StatusCode::NOT_FOUND, Json(body))
        }
        PelicanError::QueueAlreadyExists(_) => {
            (StatusCode::CONFLICT, Json(body))
        }
        PelicanError::InvalidDeliveryTag(_) => {
            (StatusCode::BAD_REQUEST, Json(body))
        }
        PelicanError::StorageLimitExceeded { .. } => {
            (StatusCode::PAYLOAD_TOO_LARGE, Json(body))
        }
        PelicanError::MessageDeadLettered { .. } => {
            (StatusCode::OK, Json(body))
        }
        _ => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
        }
    }
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(HealthResponse {
        status: "ok".to_string(),
    }))
}

async fn declare_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut mgr = state.lock().unwrap();
    match mgr.declare_queue(&name) {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::Value::Null)),
        Err(e) => map_error(e),
    }
}

async fn list_queues(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mgr = state.lock().unwrap();
    let names = mgr.list_queues();
    let mut queues = Vec::with_capacity(names.len());
    for name in names {
        let depth = mgr.depth(&name).unwrap_or(0);
        queues.push(QueueEntry { name, depth });
    }
    (StatusCode::OK, Json(QueueListResponse { queues }))
}

async fn publish(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<PublishRequest>,
) -> impl IntoResponse {
    let payload = match base64::engine::general_purpose::STANDARD
        .decode(&req.payload_base64)
    {
        Ok(bytes) => bytes,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("invalid base64: {}", e)})),
            );
        }
    };

    let message = Message::new(payload, req.headers);
    let msg_id = message.id;

    let mut mgr = state.lock().unwrap();
    match mgr.publish(&name, message) {
        Ok(PublishOutcome::Stored(_)) => {
            let resp = serde_json::json!({"id": msg_id.to_string()});
            (StatusCode::CREATED, Json(resp))
        }
        Ok(PublishOutcome::Deduplicated) => {
            (StatusCode::OK, Json(serde_json::json!({"status": "deduplicated"})))
        }
        Err(e) => map_error(e),
    }
}

async fn consume(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut mgr = state.lock().unwrap();
    match mgr.consume(&name) {
        Ok(Some((tag, msg))) => {
            let payload_b64 = base64::engine::general_purpose::STANDARD
                .encode(&msg.payload);
            let resp = serde_json::json!({
                "delivery_tag": tag,
                "id": msg.id.to_string(),
                "payload_base64": payload_b64,
                "headers": msg.headers,
                "timestamp": msg.timestamp,
            });
            (StatusCode::OK, Json(resp))
        }
        Ok(None) => (StatusCode::NO_CONTENT, Json(serde_json::Value::Null)),
        Err(e) => map_error(e),
    }
}

async fn ack(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<AckNackRequest>,
) -> impl IntoResponse {
    let mut mgr = state.lock().unwrap();
    match mgr.ack(&name, req.delivery_tag) {
        Ok(()) => (StatusCode::OK, Json(serde_json::Value::Null)),
        Err(e) => map_error(e),
    }
}

async fn nack(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<AckNackRequest>,
) -> impl IntoResponse {
    let mut mgr = state.lock().unwrap();
    match mgr.nack(&name, req.delivery_tag) {
        Ok(()) => (StatusCode::OK, Json(serde_json::Value::Null)),
        Err(e) => map_error(e),
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", axum::routing::get(health))
        .route("/queues", axum::routing::get(list_queues))
        .route("/queues/:name", axum::routing::post(declare_queue))
        .route("/queues/:name/publish", axum::routing::post(publish))
        .route("/queues/:name/consume", axum::routing::post(consume))
        .route("/queues/:name/ack", axum::routing::post(ack))
        .route("/queues/:name/nack", axum::routing::post(nack))
        .with_state(state)
}
