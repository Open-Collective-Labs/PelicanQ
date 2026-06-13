# PelicanQ — Feature Specification

## Deployment Tiers

### Solo
Single-node, embedded sled storage, HTTP API. Suitable for development and low-throughput production.

### Flock (Cluster)
Multi-node Raft consensus, replication, leader election, failover. Suitable for high-availability production.

## Core Features

- **FIFO Queues** — strict ordering within a queue
- **At-least-once delivery** — crash-safe ack/nack with in-flight tracking
- **TTL & Retention** — per-message TTL, queue-level size/age limits, disk watermarks
- **DLQ ("The Nest")** — dead-letter queue for undeliverable messages
- **Delayed/Scheduled** — messages delivered at a future timestamp
- **Priority Queues** — messages delivered in priority order
- **Deduplication** — idempotent publication via message ID
- **Batch Operations** — batch publish and consume

## API

- **HTTP/REST** — publish, consume, ack, nack, list queues, health
- **gRPC** — primary contract, protobuf-defined
- **AMQP-compatible** — wire-level compatibility
- **WebSocket** — streaming consume

## Enterprise

- OAuth2 / OIDC authentication
- Role-based access control (RBAC)
- Multi-tenancy with namespace isolation
- Encryption at rest
- Audit logging
- Web dashboard
- Prometheus / OpenTelemetry metrics
- Federation between clusters
- Kubernetes operator + Helm chart
