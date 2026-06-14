# Roadmap

## Completed

| Area | Feature | Status |
|---|---|---|
| Core | Message & Queue primitives | ✅ |
| Core | Sled persistence | ✅ |
| Core | Crash-safe ack/nack | ✅ |
| Core | Retention & watermarks | ✅ |
| Daemon | HTTP API + `pelicanqd` binary | ✅ |
| Reliability | DLQ ("The Nest") | ✅ |
| Reliability | Delayed / scheduled messages | ✅ |
| Reliability | Priority queues | ✅ |
| Reliability | Deduplication | ✅ |
| Reliability | Batch publish / consume | ✅ |
| Clustering | Raft consensus, replication, failover | ✅ |
| Clustering | Sled-backed Raft log | ✅ |
| Clustering | Cluster bootstrap | ✅ |
| Protocols | gRPC server (9 RPCs + streaming) | ✅ |
| SDKs | Rust SDK (reference) | ✅ |

## In Progress

| Area | Feature | Status |
|---|---|---|
| SDKs | Go SDK | ❌ |
| SDKs | Python SDK | ❌ |
| SDKs | Node.js SDK | ❌ |
| Protocols | AMQP-compatible wire protocol | ❌ |

## Planned

| Area | Feature |
|---|---|
| Enterprise | OAuth2 / OIDC authentication |
| Enterprise | Role-based access control (RBAC) |
| Enterprise | Multi-tenancy with namespace isolation |
| Enterprise | Encryption at rest |
| Enterprise | Audit logging |
| Operations | Web dashboard |
| Operations | Prometheus / OpenTelemetry metrics |
| Operations | Kubernetes operator + Helm chart |
| Clustering | Dynamic membership changes |
| Protocols | WebSocket streaming |
