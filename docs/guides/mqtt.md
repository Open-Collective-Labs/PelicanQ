# MQTT Protocol Support

PelicanQ exposes an MQTT 3.1.1 listener alongside its native HTTP and gRPC APIs. All three protocols share the same engine and data.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `PELICANQ_MQTT_ADDR` | `127.0.0.1:1883` | MQTT listen address. Set empty to disable. |

## Topic to Queue Mapping

MQTT topics map 1:1 to PelicanQ queue names. Publishing to `orders` writes to queue `orders`; subscribing to `orders` polls queue `orders`.

## QoS Levels

| Level | Publish | Subscribe |
|-------|---------|-----------|
| QoS 0 | Fire-and-forget | Message delivered without PUBACK |
| QoS 1 | Engine write + PUBACK | Message delivered; PUBACK from client triggers engine ack |
| QoS 2 | Treated as QoS 1 | Not supported (SUBACK returns 0x80) |

## Auto-Declare

Queues are auto-declared on first publish or subscribe.

## Limitations

- No wildcard subscriptions (`+`, `#`)
- No authentication
- No retained messages or LWT
- Rich PelicanQ metadata (priority, headers, scheduling) not accessible via MQTT

## Examples

```bash
# Mosquitto CLI
mosquitto_pub -h 127.0.0.1 -p 1883 -t "orders" -m 'hello'
mosquitto_sub -h 127.0.0.1 -p 1883 -t "orders"
```

See the [MQTT feature spec](../architecture/overview.md) for more details.
