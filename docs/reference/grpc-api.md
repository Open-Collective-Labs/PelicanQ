# gRPC API Reference

Endpoint: `127.0.0.1:7072`

Services are defined in `proto/pelicanq/v1/`.

## QueueService

| RPC | Type | Description |
|-----|------|-------------|
| `DeclareQueue` | Unary | Create a queue (idempotent) |
| `ListQueues` | Unary | List all queues with depth |
| `Publish` | Unary | Publish a single message |
| `PublishBatch` | Unary | Publish multiple messages |
| `Consume` | Unary | Consume one message |
| `ConsumeBatch` | Unary | Consume up to N messages |
| `ConsumeStream` | Bidirectional streaming | Streaming consume with ack/nack |
| `Ack` | Unary | Acknowledge a message |
| `Nack` | Unary | Nack a message |

## AdminService

| RPC | Type | Description |
|-----|------|-------------|
| `Health` | Unary | Health check |
| `ClusterStatus` | Unary | Cluster status (Flock only) |

## Error Codes

| Condition | gRPC Status |
|-----------|-------------|
| Queue not found | `NotFound` |
| Storage limit | `ResourceExhausted` |
| Invalid delivery tag | `InvalidArgument` |
| Message dead-lettered | `FailedPrecondition` |
| Not leader (Flock) | `Unavailable` (with leader address) |
