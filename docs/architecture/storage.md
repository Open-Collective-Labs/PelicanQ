# Storage Model

PelicanQ uses [sled](https://github.com/spacejam/sled), an embedded database written in Rust, for all persistent storage.

## Queue Storage

Each queue is backed by multiple sled trees within a single database:

| Tree | Purpose |
|------|---------|
| `queue:<name>:msgs` | Main message tree (ready for delivery) |
| `queue:<name>:inflight` | In-flight messages (delivered but not yet acked) |
| `queue:<name>:scheduled` | Scheduled messages (future `deliver_at`) |
| `queue:<name>:dlq` | Dead-letter messages |
| `queue:<name>:dedup` | Deduplication key index |

## Key Encoding

- **Main tree**: 8-byte big-endian monotonically increasing ID.
- **Priority tree**: 9-byte key — `[9 - priority, 8-byte ID]`. Higher priority = smaller key = delivered first.
- **Scheduled tree**: 16-byte key — `[8-byte deliver_at, 8-byte ID]`.
- **Inflight tree**: 8-byte delivery tag (matches message ID).

## Crash Recovery

On startup, `recover_inflight()` moves all entries from the inflight tree back to the main tree, ensuring at-least-once delivery semantics.
