use serde::{Deserialize, Serialize};

/// Per-queue retention policy. All fields optional; None means "no limit".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Maximum age of a message in seconds before it's eligible for removal.
    pub max_age_secs: Option<u64>,
    /// Maximum number of messages allowed in the queue.
    pub max_messages: Option<u64>,
    /// Maximum delivery attempts before routing to the dead-letter queue.
    pub max_delivery_attempts: Option<u32>,
}

impl Default for RetentionPolicy {
    /// No limits.
    fn default() -> Self {
        Self {
            max_age_secs: None,
            max_messages: None,
            max_delivery_attempts: None,
        }
    }
}

impl RetentionPolicy {
    pub fn new(
        max_age_secs: Option<u64>,
        max_messages: Option<u64>,
        max_delivery_attempts: Option<u32>,
    ) -> Self {
        Self {
            max_age_secs,
            max_messages,
            max_delivery_attempts,
        }
    }
}

/// Global storage watermark thresholds, as percentages of disk used (0-100).
pub struct StorageWatermarks {
    pub warn_pct: u8,
    pub throttle_pct: u8,
    pub reject_pct: u8,
}

impl Default for StorageWatermarks {
    /// warn=75, throttle=90, reject=95
    fn default() -> Self {
        Self {
            warn_pct: 75,
            throttle_pct: 90,
            reject_pct: 95,
        }
    }
}
