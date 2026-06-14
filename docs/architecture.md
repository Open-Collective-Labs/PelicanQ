# Architecture

## Storage Model

Each queue is backed by a sled-backed append-only log. Messages are stored as serialized records keyed by a monotonically increasing offset. The in-memory index tracks which offsets are ready for delivery.

## Delivery Semantics

- Messages are delivered in FIFO order within a single queue.
- After delivery, a message enters the in-flight state.
- The consumer must send an ack to confirm processing, or a nack to request requeue.
- On crash, all in-flight messages are requeued on restart (at-least-once delivery).

## Retention

- **TTL**: Per-message time-to-live. Expired messages are skipped on consume and purged on compaction.
- **Size limit**: When a queue exceeds its max size, oldest messages are evicted.
- **Age limit**: Messages older than the configured age are purged.
- **Watermarks**: Soft/hard disk limits trigger compaction or rejection of new publishes.

## Daemon Process

The daemon (`pelicanqd`) runs two servers concurrently on separate ports:

```
┌─────────────────────────────────────────────────────┐
│                   pelicanqd                          │
│                                                     │
│  ┌──────────────────────┐  ┌──────────────────────┐ │
│  │    HTTP Server        │  │    gRPC Server        │ │
│  │    (axum 0.7)         │  │    (tonic 0.11)       │ │
│  │    Port: 7070         │  │    Port: 7072         │ │
│  │                      │  │                      │ │
│  │  POST /queues/:name  │  │  QueueService         │ │
│  │  POST .../publish    │  │    DeclareQueue       │ │
│  │  POST .../consume    │  │    Publish            │ │
│  │  POST .../ack        │  │    Consume            │ │
│  │  POST .../nack       │  │    ConsumeStream      │ │
│  │  GET /health         │  │    Ack / Nack         │ │
│  │  GET /cluster/status │  │    ListQueues         │ │
│  │                      │  │  AdminService         │ │
│  │                      │  │    Health             │ │
│  │                      │  │    ClusterStatus      │ │
│  └──────────┬───────────┘  └───────────┬──────────┘ │
│             │                          │            │
│             └──────────┬───────────────┘            │
│                        │                            │
│               ┌────────▼────────┐                   │
│               │   AppEngine     │                   │
│               │  (Solo/Flock)   │                   │
│               └────────┬────────┘                   │
│                        │                            │
│               ┌────────▼────────┐                   │
│               │  QueueManager   │                   │
│               │   (sled store)  │                   │
│               └─────────────────┘                   │
└─────────────────────────────────────────────────────┘
```

Both servers share the same `Arc<AppState>` containing the `AppEngine`. Mutating operations go through the same engine regardless of protocol.

## gRPC Layer

The gRPC server uses `tonic` with `prost` code generation:

- Proto files in `proto/pelicanq/v1/` are the **canonical contract**.
- `pelicanqd/build.rs` runs `tonic_build` to generate Rust types and service traits.
- The generated code lives in the `OUT_DIR` and is included via `tonic::include_proto!("pelicanq.v1")`.
- Service implementations (`grpc/queue_service.rs`, `grpc/admin_service.rs`) implement the generated traits.

### Error Mapping

PelicanError variants are mapped to gRPC status codes:

| PelicanError | gRPC Status |
|---|---|
| QueueNotFound | NotFound |
| QueueAlreadyExists | AlreadyExists (not used — DeclareQueue returns `created: false` instead) |
| StorageLimitExceeded | ResourceExhausted |
| InvalidDeliveryTag | InvalidArgument |
| MessageDeadLettered | FailedPrecondition |
| (other) | Internal |

In Flock (Raft) mode, `NotLeader` is mapped to `Status::unavailable` with the leader's address in the error message.

### ConsumeStream

The `ConsumeStream` RPC is a bidirectional streaming endpoint:

1. Client sends a `ConsumeStreamAck` with a `queue` field to identify the stream.
2. Server polls `QueueManager::consume()` every 100ms.
3. When a message is available, it sends a `ConsumedMessage` over the response stream.
4. Client sends acks/nacks back via `ConsumeStreamAck` with `delivery_tag` or `nack_delivery_tag`.
5. The stream ends when the client disconnects.

In Flock mode, each poll goes through Raft (`client_write(Consume)`) for consistency.

## Clustering

See the [Clustering](clustering.md) document for full details on Raft-based Flock mode.

## Rust SDK

The reference SDK (`sdks/rust/`) wraps the gRPC generated clients:

```
┌──────────────┐     gRPC      ┌──────────────┐
│  Rust SDK    │◄─────────────►│  pelicanqd   │
│  PelicanClient│              │  (daemon)    │
└──────────────┘              └──────────────┘
```

The SDK re-exports proto-generated types through a clean Rust API with `ClientMessage`, `Delivery`, `QueueOptions`, and `PelicanClientError` types. See `sdks/rust/README.md` for usage.
