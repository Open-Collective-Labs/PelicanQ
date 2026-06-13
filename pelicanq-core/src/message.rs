use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
        }
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
}
