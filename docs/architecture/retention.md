# Retention & Watermarks

## Retention Policies

Each queue supports configurable retention parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `max_age_secs` | Maximum age of a message (TTL) | 86400 (24 hours) |
| `max_messages` | Maximum number of messages in the queue | 10000 |
| `max_delivery_attempts` | Max times a message can be nacked before DLQ | 3 |

## Storage Watermarks

PelicanQ enforces disk usage limits to prevent unbounded growth:

| Level | Threshold | Behavior |
|-------|-----------|----------|
| Warn | 75% | Logged |
| Throttle | 90% | New publishes may be delayed |
| Reject | 95% | New publishes return `StorageLimitExceeded` |

## Compaction

Sled performs background compaction. Additionally, `apply_retention()` purges expired messages from all trees.
