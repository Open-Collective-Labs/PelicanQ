# Rust SDK

The Rust SDK is the reference client implementation. It wraps the gRPC generated clients.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
pelicanq = { git = "https://github.com/Open-Collective-Labs/PelicanQ", path = "sdks/rust" }
```

## Client API

```rust
use pelicanq::{PelicanClient, ClientMessage, QueueOptions};

let mut client = PelicanClient::connect("http://127.0.0.1:7072").await?;
```

| Method | Description |
|--------|-------------|
| `connect(addr)` | Connect to a PelicanQ gRPC endpoint |
| `declare_queue(name, opts)` | Create a queue (idempotent) |
| `publish(queue, msg)` | Publish a single message |
| `publish_batch(queue, msgs)` | Publish multiple messages |
| `consume(queue)` | Consume one message |
| `consume_batch(queue, max)` | Consume up to `max` messages |
| `consume_stream(queue)` | Open a bidirectional streaming consume |
| `ack(queue, delivery_tag)` | Acknowledge a message |
| `nack(queue, delivery_tag)` | Nack (requeue or dead-letter) |
| `list_queues()` | List all queues with depth |
| `health()` | Check daemon health |

## Types

- `ClientMessage` — Builder for publish payloads
- `Delivery` — Consumed message with delivery tag
- `PublishResult` — Publish response (id, deduplicated)
- `QueueOptions` — Queue declaration parameters
- `QueueInfo` — Queue metadata (name, depth)
- `PelicanClientError` — Error type

## Example

```rust
use pelicanq::{PelicanClient, ClientMessage, QueueOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = PelicanClient::connect("http://127.0.0.1:7072").await?;

    client.declare_queue("myqueue", QueueOptions::default()).await?;

    let msg = ClientMessage::new(b"Hello!").with_priority(5);
    let result = client.publish("myqueue", msg).await?;
    println!("Published: {}", result.id);

    let delivery = client.consume("myqueue").await?;
    if let Some(d) = delivery {
        println!("Got: {:?}", d.message.payload());
        client.ack("myqueue", d.delivery_tag).await?;
    }

    Ok(())
}
```
