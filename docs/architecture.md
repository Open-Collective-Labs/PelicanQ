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
