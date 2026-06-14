use std::collections::HashMap;

/// Builder for a message to publish.
#[derive(Debug, Clone)]
pub struct ClientMessage {
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub priority: u8,
    pub deliver_at: Option<i64>,
    pub dedup_key: Option<String>,
}

impl ClientMessage {
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            payload: payload.into(),
            headers: HashMap::new(),
            priority: 0,
            deliver_at: None,
            dedup_key: None,
        }
    }

    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority.min(9);
        self
    }

    pub fn with_deliver_at(mut self, deliver_at_ms: i64) -> Self {
        self.deliver_at = Some(deliver_at_ms);
        self
    }

    pub fn with_dedup_key(mut self, key: impl Into<String>) -> Self {
        self.dedup_key = Some(key.into());
        self
    }

    pub fn with_header(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// Result of a publish call.
#[derive(Debug, Clone)]
pub struct PublishResult {
    pub id: String,
    pub deduplicated: bool,
}

/// A consumed message with its delivery tag.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub delivery_tag: u64,
    pub message: ClientMessage,
    pub id: String,
    pub timestamp: i64,
    pub delivery_attempts: u32,
}

/// Queue declaration options.
#[derive(Debug, Clone, Default)]
pub struct QueueOptions {
    pub max_age_secs: Option<u64>,
    pub max_messages: Option<u64>,
    pub max_delivery_attempts: Option<u32>,
    pub dead_letter_queue: Option<String>,
    pub dedup_window_secs: Option<u64>,
}

/// Information about a queue.
#[derive(Debug, Clone)]
pub struct QueueInfo {
    pub name: String,
    pub depth: u64,
    pub scheduled_depth: u64,
}

// ---------------------------------------------------------------------------
// Conversions between SDK types and proto-generated types
// ---------------------------------------------------------------------------

use crate::pb;

impl From<ClientMessage> for pb::Message {
    fn from(msg: ClientMessage) -> Self {
        pb::Message {
            id: String::new(),
            payload: msg.payload,
            headers: msg.headers,
            timestamp: 0,
            priority: msg.priority as u32,
            deliver_at: msg.deliver_at,
            dedup_key: msg.dedup_key,
            delivery_attempts: 0,
        }
    }
}

impl From<pb::Message> for ClientMessage {
    fn from(msg: pb::Message) -> Self {
        ClientMessage {
            payload: msg.payload,
            headers: msg.headers,
            priority: msg.priority as u8,
            deliver_at: msg.deliver_at,
            dedup_key: msg.dedup_key,
        }
    }
}

impl From<pb::ConsumedMessage> for Delivery {
    fn from(cm: pb::ConsumedMessage) -> Self {
        let (msg, id, timestamp, delivery_attempts) =
            if let Some(inner) = cm.message {
                let id = inner.id.clone();
                let ts = inner.timestamp;
                let da = inner.delivery_attempts;
                (ClientMessage::from(inner), id, ts, da)
            } else {
                (ClientMessage::new(Vec::new()), String::new(), 0, 0)
            };
        Delivery {
            delivery_tag: cm.delivery_tag,
            message: msg,
            id,
            timestamp,
            delivery_attempts,
        }
    }
}

impl From<pb::QueueInfo> for QueueInfo {
    fn from(qi: pb::QueueInfo) -> Self {
        QueueInfo {
            name: qi.name,
            depth: qi.depth,
            scheduled_depth: qi.scheduled_depth,
        }
    }
}

impl From<pb::PublishResponse> for PublishResult {
    fn from(pr: pb::PublishResponse) -> Self {
        PublishResult {
            id: pr.id,
            deduplicated: pr.deduplicated,
        }
    }
}
