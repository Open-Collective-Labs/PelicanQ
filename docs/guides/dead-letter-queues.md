# Dead-Letter Queues

Messages that exceed their maximum delivery attempts are routed to a dead-letter queue (DLQ).

## How It Works

1. Each nack increments the message's `delivery_attempts` counter.
2. When `delivery_attempts >= max_delivery_attempts`, the message is moved to the DLQ instead of being requeued.
3. DLQ messages persist across restarts and can be inspected or reprocessed.

## Configuration

Set the max delivery attempts when declaring a queue:

```rust
let opts = QueueOptions {
    max_delivery_attempts: Some(3),  // After 3 nacks, message is dead-lettered
    ..Default::default()
};
client.declare_queue("myqueue", opts).await?;
```

## DLQ Storage

The DLQ is stored as a per-queue subtree within the same sled database. Each queue's DLQ is separate — there is no cross-queue DLQ routing yet.

## Monitoring

```rust
// Get dead letter count (HTTP-only at this stage)
// POST http://127.0.0.1:7070/queues/myqueue/status
```
