# Priority Queues

PelicanQ supports 10 priority levels (0–9). Higher priority messages are delivered before lower priority ones.

## Usage

```rust
let msg = ClientMessage::new(b"urgent").with_priority(9);
client.publish("myqueue", msg).await?;
```

## Priority Levels

| Level | Meaning |
|-------|---------|
| 0 | Lowest (default) |
| 1-3 | Low |
| 4-6 | Normal |
| 7-8 | High |
| 9 | Highest |

## Ordering Guarantees

- Messages with higher priority are always delivered before lower priority.
- Within the same priority level, FIFO ordering is preserved.
- Priority is clamped to 0–9 at the client and server side.
