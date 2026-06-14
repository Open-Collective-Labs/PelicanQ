# MQTT Protocol Support

PelicanQ exposes an **MQTT 3.1.1** listener alongside its native HTTP REST and gRPC APIs. All three protocols share the same engine and data — messages published via MQTT are immediately available via HTTP/gRPC and vice versa.

## Configuration

| Env Var | Default | Description |
|---|---|---|
| `PELICANQ_MQTT_ADDR` | `127.0.0.1:1883` | MQTT listen address. Set to empty string to disable. |

## Protocol Mapping

### Topic → Queue

MQTT topics map **1:1** to PelicanQ queue names. There is no hierarchy or topic tree.

- Publishing to topic `orders` writes to queue `orders`.
- Subscribing to topic `orders` polls queue `orders`.

### Wildcards

MQTT wildcards `+` and `#` are **not supported**. A SUBSCRIBE with a wildcard filter returns a SUBACK with failure code `0x80`.

### QoS

| Level | Publish (client → broker) | Subscribe (broker → client) |
|---|---|---|
| QoS 0 | Fire-and-forget; no PUBACK | Message delivered without PUBACK from client |
| QoS 1 | Engine write + PUBACK | Message delivered with packet ID; waits for client PUBACK before calling `QueueManager::ack` |
| QoS 2 | Treated as QoS 1 | SUBACK returns failure code `0x80` |

### Auto-Declare

If a queue does not exist when a client publishes or subscribes to it, PelicanQ automatically declares it. This matches the behavior of most MQTT brokers.

## Limitations

- No authentication (simple CONNECT accepted for any client).
- No retained messages (`retain` flag is ignored).
- No Last Will and Testament (LWT).
- No topic hierarchy or wildcard subscriptions.
- Rich message metadata (priority, headers, dedup keys, scheduling) is not accessible via MQTT — payload is raw bytes and all other fields use defaults.

## Examples

### Mosquitto CLI

```bash
# Publish
mosquitto_pub -h 127.0.0.1 -p 1883 -t "orders" -m '{"id":"123"}'

# Subscribe
mosquitto_sub -h 127.0.0.1 -p 1883 -t "orders"
```

### Python (paho-mqtt)

```python
import paho.mqtt.client as mqtt

def on_message(client, userdata, msg):
    print(f"{msg.topic}: {msg.payload}")

client = mqtt.Client()
client.on_message = on_message
client.connect("127.0.0.1", 1883)
client.subscribe("orders")
client.publish("orders", b"hello")
client.loop_forever()
```

### Node.js (mqtt.js)

```js
const mqtt = require('mqtt')
const client = mqtt.connect('mqtt://127.0.0.1:1883')

client.on('connect', () => {
  client.subscribe('orders')
  client.publish('orders', 'hello')
})

client.on('message', (topic, message) => {
  console.log(topic, message.toString())
})
```
