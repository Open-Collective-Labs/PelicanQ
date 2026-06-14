# Quickstart

## Prerequisites

- Rust toolchain (install via [rustup](https://rustup.rs/))
- Git

## Build and Run

```bash
# Clone the repository
git clone https://github.com/Open-Collective-Labs/PelicanQ.git
cd PelicanQ

# Build the daemon
cargo build --release

# Run in Solo mode (single node)
PELICANQ_DATA_DIR=./data ./target/release/pelicanqd
```

## Publish and Consume

### Via HTTP

```bash
# Declare a queue
curl -X POST http://127.0.0.1:7070/queues/myqueue

# Publish a message
curl -X POST http://127.0.0.1:7070/queues/myqueue/publish \
  -H 'content-type: application/json' \
  -d '{"payload_base64":"SGVsbG8gUGVsaWNhblE="}'

# Consume a message
curl -X POST http://127.0.0.1:7070/queues/myqueue/consume

# Ack the message (replace <delivery_tag> with the actual tag)
curl -X POST http://127.0.0.1:7070/queues/myqueue/ack \
  -H 'content-type: application/json' \
  -d '{"delivery_tag": <delivery_tag>}'
```

### Via Rust SDK

```rust
use pelicanq::PelicanClient;

let mut client = PelicanClient::connect("http://127.0.0.1:7072").await?;

client.declare_queue("myqueue", QueueOptions::default()).await?;

let msg = ClientMessage::new(b"Hello PelicanQ!").with_priority(5);
let result = client.publish("myqueue", msg).await?;
println!("Published: id={}", result.id);

let delivery = client.consume("myqueue").await?;
if let Some(d) = delivery {
    println!("Got: {:?}", d.message.payload());
    client.ack("myqueue", d.delivery_tag).await?;
}
```

## Protocol Ports

| Protocol | Default Address | Purpose |
|----------|----------------|---------|
| HTTP/REST | `127.0.0.1:7070` | REST API for queue operations |
| gRPC | `127.0.0.1:7072` | gRPC API (used by official SDKs) |
| MQTT 3.1.1 | `127.0.0.1:1883` | MQTT pub/sub |
| Raft inter-node | Per config | Internal cluster communication |

## Next Steps

- Read the [Guides](../guides/publish-consume.md) for detailed usage
- Set up a [Flock Cluster](../deployment/flock.md) for HA
- Explore the [Architecture](../architecture/overview.md)
