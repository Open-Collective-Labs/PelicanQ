use std::sync::{Arc, Mutex};
use std::time::Duration;

use pelicanq_core::queue::QueueManager;
use pelicanqd::api::{AppEngine, AppState};
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Server};
use tower::ServiceExt;

use pelicanqd::grpc::pb::admin_service_client::AdminServiceClient;
use pelicanqd::grpc::pb::queue_service_client::QueueServiceClient;
use pelicanqd::grpc::pb::{
    self as pb, ConsumeRequest, ConsumeStreamAck, DeclareQueueRequest, HealthRequest,
    ListQueuesRequest, PublishRequest,
};

fn test_state() -> pelicanqd::api::SharedState {
    let dir = tempfile::tempdir().unwrap();
    let mgr = QueueManager::open(dir.path(), None).unwrap();
    Arc::new(AppState {
        engine: AppEngine::Solo(Arc::new(Mutex::new(mgr))),
        cluster: None,
    })
}

async fn start_grpc_server(
    state: pelicanqd::api::SharedState,
) -> (tokio::task::JoinHandle<()>, u16) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();

    let queue_svc = pelicanqd::grpc::queue_service::QueueServiceImpl::new(state.clone());
    let admin_svc = pelicanqd::grpc::admin_service::AdminServiceImpl::new(state);

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(
                pelicanqd::grpc::pb::queue_service_server::QueueServiceServer::new(queue_svc),
            )
            .add_service(
                pelicanqd::grpc::pb::admin_service_server::AdminServiceServer::new(admin_svc),
            )
            .serve_with_incoming(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    (handle, port)
}

async fn grpc_channel(port: u16) -> Channel {
    Channel::from_shared(format!("http://127.0.0.1:{}", port))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_grpc_declare_queue_created() {
    let state = test_state();
    let (_handle, port) = start_grpc_server(state).await;
    let mut client = QueueServiceClient::new(grpc_channel(port).await);

    let resp = client
        .declare_queue(DeclareQueueRequest {
            name: "test_queue".to_string(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.created);
}

#[tokio::test]
async fn test_grpc_declare_queue_twice() {
    let state = test_state();
    let (_handle, port) = start_grpc_server(state).await;
    let mut client = QueueServiceClient::new(grpc_channel(port).await);

    let req = DeclareQueueRequest {
        name: "dup_queue".to_string(),
        ..Default::default()
    };

    let resp1 = client.declare_queue(req.clone()).await.unwrap().into_inner();
    assert!(resp1.created);

    let resp2 = client.declare_queue(req).await.unwrap().into_inner();
    assert!(!resp2.created);
}

#[tokio::test]
async fn test_grpc_publish_and_consume() {
    let state = test_state();
    let (_handle, port) = start_grpc_server(state).await;
    let mut client = QueueServiceClient::new(grpc_channel(port).await);

    client
        .declare_queue(DeclareQueueRequest {
            name: "q".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    client
        .publish(PublishRequest {
            queue: "q".to_string(),
            message: Some(pb::Message {
                id: String::new(),
                payload: b"hello".to_vec(),
                headers: Default::default(),
                timestamp: 0,
                priority: 0,
                deliver_at: None,
                dedup_key: None,
                delivery_attempts: 0,
            }),
        })
        .await
        .unwrap();

    let consumed = client
        .consume(ConsumeRequest {
            queue: "q".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(consumed.message.is_some());
    let msg = consumed.message.unwrap();
    assert_eq!(msg.message.unwrap().payload, b"hello");
}

#[tokio::test]
async fn test_grpc_consume_empty() {
    let state = test_state();
    let (_handle, port) = start_grpc_server(state).await;
    let mut client = QueueServiceClient::new(grpc_channel(port).await);

    client
        .declare_queue(DeclareQueueRequest {
            name: "empty_q".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    let resp = client
        .consume(ConsumeRequest {
            queue: "empty_q".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.message.is_none());
}

#[tokio::test]
async fn test_grpc_list_queues() {
    let state = test_state();
    let (_handle, port) = start_grpc_server(state).await;
    let mut client = QueueServiceClient::new(grpc_channel(port).await);

    client
        .declare_queue(DeclareQueueRequest {
            name: "a".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .declare_queue(DeclareQueueRequest {
            name: "b".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    let resp = client
        .list_queues(ListQueuesRequest {})
        .await
        .unwrap()
        .into_inner();
    let names: Vec<&str> = resp.queues.iter().map(|q| q.name.as_str()).collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"b"));
}

#[tokio::test]
async fn test_grpc_health() {
    let state = test_state();
    let (_handle, port) = start_grpc_server(state).await;
    let mut client = AdminServiceClient::new(grpc_channel(port).await);

    let resp = client
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, "ok");
}

fn make_msg(payload: &[u8]) -> pb::Message {
    pb::Message {
        id: String::new(),
        payload: payload.to_vec(),
        headers: Default::default(),
        timestamp: 0,
        priority: 0,
        deliver_at: None,
        dedup_key: None,
        delivery_attempts: 0,
    }
}

#[tokio::test]
async fn test_grpc_consume_stream() {
    let state = test_state();
    let (_handle, port) = start_grpc_server(state).await;
    let mut client = QueueServiceClient::new(grpc_channel(port).await);

    client
        .declare_queue(DeclareQueueRequest {
            name: "stream_q".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    client
        .publish(PublishRequest {
            queue: "stream_q".to_string(),
            message: Some(make_msg(b"stream_msg")),
        })
        .await
        .unwrap();

    let (tx, rx) = tokio::sync::mpsc::channel(8);

    let response = client
        .consume_stream(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap();
    let mut response_stream = response.into_inner();

    tx.send(ConsumeStreamAck {
        queue: Some("stream_q".to_string()),
        delivery_tag: None,
        nack_delivery_tag: None,
    })
    .await
    .unwrap();

    let msg = response_stream.next().await;
    assert!(msg.is_some());
    let consumed = msg.unwrap().unwrap();
    assert_eq!(consumed.message.unwrap().payload, b"stream_msg");

    drop(tx);
}

#[tokio::test]
async fn test_grpc_queue_not_found() {
    let state = test_state();
    let (_handle, port) = start_grpc_server(state).await;
    let mut client = QueueServiceClient::new(grpc_channel(port).await);

    let err = client
        .consume(ConsumeRequest {
            queue: "nonexistent".to_string(),
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_grpc_cross_protocol() {
    let state = test_state();
    let (_handle, port) = start_grpc_server(state.clone()).await;

    let mut grpc_client = QueueServiceClient::new(grpc_channel(port).await);
    grpc_client
        .declare_queue(DeclareQueueRequest {
            name: "cross_q".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    let app = pelicanqd::api::build_router(state.clone());
    let payload_b64 = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(b"cross_proto_msg")
    };
    let body = serde_json::json!({
        "payload_base64": payload_b64,
        "headers": {},
    });
    let http_resp = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/queues/cross_q/publish")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::to_string(&body).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(http_resp.status(), axum::http::StatusCode::CREATED);

    let consumed = grpc_client
        .consume(ConsumeRequest {
            queue: "cross_q".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(consumed.message.is_some());
    let msg = consumed.message.unwrap();
    assert_eq!(msg.message.unwrap().payload, b"cross_proto_msg");
}
