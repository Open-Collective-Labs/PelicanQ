# Changelog

## v0.1.0 (2026-06-14)

### Phase 5 — SDKs

- Go SDK: Client, types, proto generation, 7 unit tests
- Python SDK: Client, types, proto stubs, 10 unit tests
- Node.js SDK: Client, types (dynamic proto loading via `@grpc/proto-loader`)
- Java SDK: Sync + async client, Types.java, pom.xml, 14 unit tests
- Examples for all 4 SDKs (publish-consume)
- Added `go_package` option to proto files

### Phase 4 — MQTT & gRPC

- MQTT 3.1.1 listener (QoS 0/1, topic→queue mapping, auto-declare)
- Raft clustering (openraft-based consensus, leader election, failover)
- gRPC server (tonic) with 11 RPCs including bidirectional streaming consume
- Rust SDK (reference client implementation)
- Proto contracts (`proto/pelicanq/v1/`) as canonical API surface

### Phase 2 — Core Features

- Delivery attempts & dead-letter queue
- Priority queues (0-9, FIFO within same priority)
- Delayed/scheduled messages
- Message deduplication

### Phase 1 — Foundation

- Core engine with FIFO ordering
- Sled-based persistence
- At-least-once delivery (ack/nack, in-flight tracking)
- TTL & retention policies
- HTTP/REST API
- Crash recovery (inflight requeue on restart)
