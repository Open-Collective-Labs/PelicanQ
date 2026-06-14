# Delivery Semantics

PelicanQ provides **at-least-once** delivery with explicit ack/nack.

## Lifecycle

```
publish → [queue] → consume → [inflight] → ack → (removed)
                              → nack → [queue] (retry)
                              → crash → [queue] (recovered on restart)
```

1. **Publish**: Message is appended to the main queue tree with a monotonically increasing ID.
2. **Consume**: Message is atomically moved from the main tree to the inflight tree. The consumer receives the message payload and a delivery tag.
3. **Ack**: Message is removed from the inflight tree.
4. **Nack**: Message's `delivery_attempts` counter is incremented. If it exceeds `max_delivery_attempts`, the message is routed to the DLQ. Otherwise, it's returned to the main tree.
5. **Crash**: On restart, all inflight messages are moved back to the main tree.

## Guarantees

- **At-least-once**: A message may be delivered more than once (if the consumer crashes after processing but before acking).
- **No at-most-once**: There is no fire-and-forget mode for queue operations (MQTT QoS 0 is fire-and-forget).
- **FIFO**: Within a single queue, messages are delivered in FIFO order (at the same priority level).
