use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use pelicanq_core::queue::QueueManager;
use tower::ServiceExt;

fn test_state() -> pelicanqd::api::AppState {
    let dir = tempfile::tempdir().unwrap();
    let mgr = QueueManager::open(dir.path(), None).unwrap();
    Arc::new(Mutex::new(mgr))
}

#[tokio::test]
async fn test_health() {
    let app = pelicanqd::api::build_router(test_state());
    let response = app
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn test_declare_queue_201() {
    let app = pelicanqd::api::build_router(test_state());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queues/orders")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_declare_queue_twice_409() {
    let app = pelicanqd::api::build_router(test_state());
    let req = || {
        Request::builder()
            .method("POST")
            .uri("/queues/orders")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap()
    };
    let r1 = app.clone().oneshot(req()).await.unwrap();
    assert_eq!(r1.status(), StatusCode::CREATED);
    let r2 = app.oneshot(req()).await.unwrap();
    assert_eq!(r2.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_publish_and_consume() {
    let app = pelicanqd::api::build_router(test_state());

    // Declare queue
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queues/orders")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);

    // Publish
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queues/orders/publish")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"payload_base64":"aGVsbG8=","headers":{"key":"val"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(r.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let published_id = json["id"].as_str().unwrap().to_string();

    // List queues - depth should be 1
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/queues")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let body = axum::body::to_bytes(r.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let queues = json["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0]["name"], "orders");
    assert_eq!(queues[0]["depth"], 1);

    // Consume
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queues/orders/consume")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let body = axum::body::to_bytes(r.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["id"], published_id);
    assert_eq!(json["payload_base64"], "aGVsbG8=");
    assert_eq!(json["headers"]["key"], "val");
    assert!(json["delivery_tag"].as_u64().is_some());
    assert!(json["timestamp"].as_i64().is_some());

    let delivery_tag = json["delivery_tag"].as_u64().unwrap();

    // Ack
    let ack_body = serde_json::json!({"delivery_tag": delivery_tag}).to_string();
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queues/orders/ack")
                .header("content-type", "application/json")
                .body(Body::from(ack_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // List queues - depth should be 0
    let r = app
        .oneshot(
            Request::builder()
                .uri("/queues")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(r.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["queues"][0]["depth"], 0);
}

#[tokio::test]
async fn test_consume_missing_queue_404() {
    let app = pelicanqd::api::build_router(test_state());
    let r = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queues/missing/consume")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_consume_empty_queue_204() {
    let app = pelicanqd::api::build_router(test_state());

    // Declare queue but don't publish
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queues/empty")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);

    // Consume on empty queue
    let r = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queues/empty/consume")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
}
