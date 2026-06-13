use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Opaque handle identifying an in-flight message for ack/nack.
/// Serializes transparently as its inner u64 for JSON compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeliveryTag(pub u64);

impl std::fmt::Display for DeliveryTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for DeliveryTag {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<DeliveryTag> for u64 {
    fn from(t: DeliveryTag) -> Self {
        t.0
    }
}

/// A single message stored in a queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    /// Unix timestamp in milliseconds, set at creation time.
    pub timestamp: i64,
    /// Number of times this message has been delivered and nacked.
    pub delivery_attempts: u32,
    /// Delivery priority, 0 (lowest, default) to 9 (highest). Higher values are
    /// delivered before lower values; within the same priority, FIFO order applies.
    pub priority: u8,
}

impl Message {
    /// Creates a new message with a fresh UUID and current timestamp.
    pub fn new(payload: Vec<u8>, headers: HashMap<String, String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            payload,
            headers,
            timestamp: Self::now_ms(),
            delivery_attempts: 0,
            priority: 0,
        }
    }

    /// Sets the priority (0-9). Values above 9 are clamped to 9.
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority.min(9);
        self
    }

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_message_has_non_nil_uuid() {
        let msg = Message::new(b"data".to_vec(), HashMap::new());
        assert_ne!(msg.id, Uuid::nil());
    }

    #[test]
    fn test_new_message_has_positive_timestamp() {
        let msg = Message::new(b"data".to_vec(), HashMap::new());
        assert!(msg.timestamp > 0);
    }

    #[test]
    fn test_new_message_zero_delivery_attempts() {
        let msg = Message::new(b"data".to_vec(), HashMap::new());
        assert_eq!(msg.delivery_attempts, 0);
    }

    #[test]
    fn test_new_message_default_priority_zero() {
        let msg = Message::new(b"data".to_vec(), HashMap::new());
        assert_eq!(msg.priority, 0);
    }

    #[test]
    fn test_with_priority_clamps_above_9() {
        let msg = Message::new(b"data".to_vec(), HashMap::new())
            .with_priority(15);
        assert_eq!(msg.priority, 9);
    }

    #[test]
    fn test_with_priority_accepts_valid_values() {
        let msg = Message::new(b"data".to_vec(), HashMap::new())
            .with_priority(5);
        assert_eq!(msg.priority, 5);
    }

    #[test]
    fn test_delivery_tag_round_trip() {
        let tag = DeliveryTag(42);
        let bytes = bincode::serialize(&tag).unwrap();
        let deserialized: DeliveryTag = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized, DeliveryTag(42));

        let tag_from: u64 = tag.into();
        assert_eq!(tag_from, 42);

        let tag_from_u64: DeliveryTag = 42u64.into();
        assert_eq!(tag_from_u64, DeliveryTag(42));
    }

    #[test]
    fn test_delivery_tag_json_transparent() {
        let tag = DeliveryTag(99);
        let json = serde_json::to_string(&tag).unwrap();
        assert_eq!(json, "99");

        let deserialized: DeliveryTag = serde_json::from_str("99").unwrap();
        assert_eq!(deserialized, DeliveryTag(99));
    }
}
