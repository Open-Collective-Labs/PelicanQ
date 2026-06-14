# Roadmap

## Completed

- Core engine with FIFO ordering
- Sled persistence
- At-least-once delivery (ack/nack, crash recovery)
- Retention policies (TTL, max count, max delivery attempts)
- Dead-letter queues
- Delayed/scheduled messages
- Priority queues (0-9)
- Message deduplication
- Batch publish/consume
- HTTP/REST API
- gRPC API (11 RPCs including streaming consume)
- MQTT 3.1.1 listener (QoS 0/1)
- Raft clustering (openraft)
- Rust SDK (reference)
- Go SDK (code complete, needs verification)
- Python SDK (code complete, needs verification)
- Node.js SDK (code complete, build blocked on grpc-tools)
- Java SDK (code complete, needs Maven + protoc)

## In Progress

- SDK build verification and CI integration
- AMQP 0-9-1 wire protocol
- CLI tool (`pelicanctl`)

## Planned

- OAuth2 / OIDC authentication
- Role-based access control (RBAC)
- Multi-tenancy with namespace isolation
- Encryption at rest
- Audit logging
- Web dashboard
- Prometheus / OpenTelemetry metrics
- Kubernetes operator + Helm chart
- Dynamic Raft membership changes
- WebSocket streaming
- Cross-queue DLQ routing
- Consumer groups
- Publish/subscribe exchange model
