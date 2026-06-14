# gRPC Streaming Consume

PelicanQ supports bidirectional streaming consume via the `ConsumeStream` RPC.

## Protocol

1. Client opens a bidirectional stream.
2. Client sends an initial `ConsumeStreamAck` with the `queue` field to identify the stream.
3. Server polls `QueueManager::consume()` every 100ms.
4. When a message is available, the server sends a `ConsumedMessage` over the response stream.
5. Client sends acks/nacks back via `ConsumeStreamAck` (`delivery_tag` or `nack_delivery_tag`).
6. Stream ends when the client disconnects.

## Raft Integration

In Flock mode, each poll goes through `client_write(Consume)` for Raft consistency, ensuring exactly one node delivers each message.
