use std::io::Read;
use std::process::{Child, Command};
use std::sync::Mutex;
use std::time::Duration;

use pelicanq::{ClientMessage, PelicanClient, QueueOptions};

/// Serializes daemon-started tests so they don't fight over ports.
static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn find_daemon_bin() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_pelicanqd") {
        return std::path::PathBuf::from(path);
    }
    for candidate in &[
        "target/debug/pelicanqd",
        "../target/debug/pelicanqd",
        "../../target/debug/pelicanqd",
    ] {
        let p = std::path::PathBuf::from(candidate);
        if p.exists() {
            return p;
        }
    }
    panic!("pelicanqd binary not found. Build it first with:\n  cargo build -p pelicanqd");
}

fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

struct Daemon {
    child: Child,
    _data_dir: tempfile::TempDir,
    grpc_port: u16,
}

impl Daemon {
    fn start() -> Self {
        let bin = find_daemon_bin();
        let grpc_port = find_free_port();
        let data_dir = tempfile::tempdir().unwrap();

        let mut child = Command::new(&bin)
            .env("PELICANQ_DATA_DIR", data_dir.path())
            .env(
                "PELICANQ_LISTEN_ADDR",
                format!("127.0.0.1:{}", find_free_port()),
            )
            .env("PELICANQ_GRPC_ADDR", format!("127.0.0.1:{}", grpc_port))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("failed to start pelicanqd");

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", grpc_port)).is_ok() {
                break;
            }
            if let Some(status) = child.try_wait().expect("failed to poll pelicanqd") {
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr);
                }
                panic!("pelicanqd exited before gRPC was ready: {status}; stderr: {stderr}");
            }
            if std::time::Instant::now() >= deadline {
                panic!("pelicanqd did not listen on gRPC port {grpc_port} within 5 seconds");
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        Self {
            child,
            _data_dir: data_dir,
            grpc_port,
        }
    }

    fn grpc_addr(&self) -> String {
        format!("http://127.0.0.1:{}", self.grpc_port)
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn test_sdk_publish_consume_ack() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let daemon = Daemon::start();

    let mut client = PelicanClient::connect(daemon.grpc_addr()).await.unwrap();
    client.health().await.unwrap();

    let created = client
        .declare_queue("test_sdk_q", QueueOptions::default())
        .await
        .unwrap();
    assert!(created);

    let msg = ClientMessage::new(b"hello sdk").with_header("x-source", "rust-sdk");
    let result = client.publish("test_sdk_q", msg).await.unwrap();
    assert!(!result.id.is_empty());
    assert!(!result.deduplicated);

    let delivery = client.consume("test_sdk_q").await.unwrap();
    assert!(delivery.is_some());
    let d = delivery.unwrap();
    assert_eq!(d.message.payload, b"hello sdk");
    assert_eq!(d.message.headers.get("x-source").unwrap(), "rust-sdk");

    client.ack("test_sdk_q", d.delivery_tag).await.unwrap();

    let queues = client.list_queues().await.unwrap();
    let names: Vec<&str> = queues.iter().map(|q| q.name.as_str()).collect();
    assert!(names.contains(&"test_sdk_q"));
}

#[tokio::test]
async fn test_sdk_declare_twice() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let daemon = Daemon::start();

    let mut client = PelicanClient::connect(daemon.grpc_addr()).await.unwrap();

    let first = client
        .declare_queue("dup_q", QueueOptions::default())
        .await
        .unwrap();
    assert!(first);

    let second = client
        .declare_queue("dup_q", QueueOptions::default())
        .await
        .unwrap();
    assert!(!second);
}

#[tokio::test]
async fn test_sdk_consume_empty() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let daemon = Daemon::start();

    let mut client = PelicanClient::connect(daemon.grpc_addr()).await.unwrap();

    client
        .declare_queue("empty_q", QueueOptions::default())
        .await
        .unwrap();

    let delivery = client.consume("empty_q").await.unwrap();
    assert!(delivery.is_none());
}

#[tokio::test]
async fn test_sdk_publish_batch() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let daemon = Daemon::start();

    let mut client = PelicanClient::connect(daemon.grpc_addr()).await.unwrap();

    client
        .declare_queue("batch_q", QueueOptions::default())
        .await
        .unwrap();

    let msgs = vec![
        ClientMessage::new(b"msg1"),
        ClientMessage::new(b"msg2"),
        ClientMessage::new(b"msg3"),
    ];
    let results = client.publish_batch("batch_q", msgs).await.unwrap();
    assert_eq!(results.len(), 3);

    let deliveries = client.consume_batch("batch_q", 10).await.unwrap();
    assert_eq!(deliveries.len(), 3);
    assert_eq!(deliveries[0].message.payload, b"msg1");
    assert_eq!(deliveries[1].message.payload, b"msg2");
    assert_eq!(deliveries[2].message.payload, b"msg3");
}
