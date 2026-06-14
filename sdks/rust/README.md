# PelicanQ Rust SDK

A minimal native Rust client for [PelicanQ](https://github.com/anomalyco/pelicanq).

## Quickstart

Add to your `Cargo.toml`:

```toml
[dependencies]
pelicanq = { path = "sdks/rust" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Connect, declare a queue, publish, consume, and ack:

```rust
use pelicanq::{ClientMessage, PelicanClient, QueueOptions};

#[tokio::main]
async fn main() {
    let mut client = PelicanClient::connect("http://127.0.0.1:7072")
        .await
        .expect("failed to connect");

    // Declare a queue (idempotent)
    let created = client
        .declare_queue("my_queue", QueueOptions::default())
        .await
        .unwrap();
    println!("Queue created: {created}");

    // Publish a message
    let msg = ClientMessage::new(b"Hello, PelicanQ!")
        .with_priority(5)
        .with_header("content-type", "text/plain");
    let result = client.publish("my_queue", msg).await.unwrap();
    println!("Published: id={}", result.id);

    // Consume the message
    let delivery = client.consume("my_queue").await.unwrap().unwrap();
    println!(
        "Received: payload={:?} tag={}",
        String::from_utf8_lossy(&delivery.message.payload),
        delivery.delivery_tag,
    );

    // Acknowledge
    client.ack("my_queue", delivery.delivery_tag).await.unwrap();
    println!("Done!");
}
```

## API

| Method | Description |
|--------|-------------|
| `connect(addr)` | Connect to a PelicanQ gRPC endpoint |
| `declare_queue(name, opts)` | Create a queue (idempotent) |
| `publish(queue, message)` | Publish a single message |
| `publish_batch(queue, messages)` | Publish multiple messages |
| `consume(queue)` | Consume one message |
| `consume_batch(queue, max)` | Consume up to `max` messages |
| `ack(queue, delivery_tag)` | Acknowledge a message |
| `nack(queue, delivery_tag)` | Nack (requeue or dead-letter) |
| `list_queues()` | List all queues |
| `health()` | Check daemon health |

## Requirements

- Rust 1.75+
- `protoc` installed and available in `PATH`
- A running PelicanQ daemon (see project root)
