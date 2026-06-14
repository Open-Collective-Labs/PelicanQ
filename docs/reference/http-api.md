# HTTP API Reference

Base URL: `http://127.0.0.1:7070`

## Queue Operations

### Declare Queue

```
POST /queues/:name
```

Creates a queue if it doesn't exist. Idempotent.

**Response**: `201 Created` (new) or `409 Conflict` (already exists)

---

### List Queues

```
GET /queues
```

Returns all queues with their current depth.

**Response**:
```json
[
  {"name": "myqueue", "depth": 42, "scheduled_depth": 0}
]
```

---

### Publish

```
POST /queues/:name/publish
```

**Request**:
```json
{
  "payload_base64": "SGVsbG8=",
  "headers": {"content-type": "text/plain"},
  "priority": 5,
  "deliver_at": null,
  "dedup_key": null
}
```

**Response**:
```json
{"id": "abc123", "deduplicated": false}
```

---

### Consume

```
POST /queues/:name/consume
```

**Response**:
```json
{
  "delivery_tag": 1,
  "payload_base64": "SGVsbG8=",
  "headers": {},
  "id": "abc123",
  "timestamp": 1718000000
}
```

Returns `null` if no messages are available.

---

### Ack

```
POST /queues/:name/ack
```

**Request**:
```json
{"delivery_tag": 1}
```

---

### Nack

```
POST /queues/:name/nack
```

**Request**:
```json
{"delivery_tag": 1}
```

## Health

```
GET /health
```

**Response**: `ok`

## Cluster Status (Flock only)

```
GET /cluster/status
```

**Response**:
```json
{
  "self_id": 1,
  "members": [...],
  "reads_may_lag": true
}
```
