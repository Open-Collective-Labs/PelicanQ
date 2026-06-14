# PelicanQ — Feature Specification

## Deployment Tiers

### Solo
Single-node, embedded sled storage, HTTP + gRPC API. Suitable for development and low-throughput production.

### Flock (Cluster)
Multi-node Raft consensus, replication, leader election, failover. Suitable for high-availability production.

## Core Features

| Feature | Status | Notes |
|---|---|---|
| FIFO Queues | ✅ | Strict ordering within a queue |
| At-least-once delivery | ✅ | Crash-safe ack/nack with in-flight tracking |
| TTL & Retention | ✅ | Per-message TTL, queue-level size/age limits, disk watermarks |
| DLQ ("The Nest") | ✅ | Dead-letter queue for undeliverable messages |
| Delayed/Scheduled Messages | ✅ | Messages delivered at a future timestamp |
| Priority Queues | ✅ | 0–9 priority, FIFO within same priority |
| Deduplication | ✅ | Idempotent publication via dedup key |
| Batch Operations | ✅ | Batch publish and consume |
| Raft Clustering | ✅ | Multi-node consensus, leader election, failover |
| Sled-backed Raft Log | ✅ | Durable Raft log at `<data_dir>/raft/` |
| Cluster Bootstrap | ✅ | Lowest-ID node auto-initializes on first start |
| gRPC Protocol | ✅ | Full gRPC API alongside HTTP — same engine, same data |
| gRPC Streaming Consume | ✅ | Bidirectional streaming with ack/nack feedback |
| Rust SDK | ✅ | Reference SDK with full API surface |
| MQTT 3.1.1 Listener | ✅ | QoS 0/1, topic ↔ queue mapping, auto-declare |

## Protocol Ports

| Protocol | Env Var | Default | Routes / Services |
|---|---|---|---|
| HTTP/REST | `PELICANQ_LISTEN_ADDR` | `127.0.0.1:7070` | `/health`, `/queues`, `/queues/:name`, `/queues/:name/publish`, `/queues/:name/consume`, `/queues/:name/ack`, `/queues/:name/nack`, `/cluster/status` |
| gRPC | `PELICANQ_GRPC_ADDR` | `127.0.0.1:7072` | `QueueService` (9 RPCs), `AdminService` (2 RPCs) |
| MQTT 3.1.1 | `PELICANQ_MQTT_ADDR` | `127.0.0.1:1883` | QoS 0/1 publish, subscribe, auto-declare |
| Raft inter-node | Member config | Per-node | Internal Raft RPC traffic |

## API Surface

### HTTP/REST

All queue operations use path parameters:

- `POST /queues/:name` — Declare queue (idempotent, returns 201 or 409)
- `GET /queues` — List queues with depth
- `POST /queues/:name/publish` — Publish message (base64 payload)
- `POST /queues/:name/consume` — Consume next message
- `POST /queues/:name/ack` — Ack by delivery tag
- `POST /queues/:name/nack` — Nack by delivery tag
- `GET /health` — Health check
- `GET /cluster/status` — Cluster status (Flock only)

### gRPC

Service definitions in `proto/pelicanq/v1/`:

- **QueueService** — DeclareQueue, ListQueues, Publish, PublishBatch, Consume, ConsumeBatch, ConsumeStream, Ack, Nack
- **AdminService** — Health, ClusterStatus

The Rust SDK (`sdks/rust/`) is the reference client implementation.

## Planned

- AMQP 0-9-1 wire protocol
- WebSocket streaming
- OAuth2 / OIDC authentication
- Role-based access control (RBAC)
- Multi-tenancy with namespace isolation
- Encryption at rest
- Audit logging
- Web dashboard
- Prometheus / OpenTelemetry metrics
- Kubernetes operator + Helm chart
