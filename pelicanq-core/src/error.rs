use serde::{Deserialize, Serialize};

use crate::message::DeliveryTag;

#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum PelicanError {
    #[error("queue not found: {queue}")]
    QueueNotFound { queue: String },

    #[error("queue already exists: {queue}")]
    QueueAlreadyExists { queue: String },

    #[error("storage error: {message}")]
    Storage { message: String },

    #[error("serialization error: {message}")]
    Serialization { message: String },

    #[error("invalid delivery tag: {tag}")]
    InvalidDeliveryTag { tag: DeliveryTag },

    #[error("storage watermark exceeded: disk usage at {used_pct}%, limit is {limit_pct}%")]
    StorageLimitExceeded { used_pct: u8, limit_pct: u8 },

    #[error("message dead-lettered: queue={queue}, dlq={dlq}")]
    MessageDeadLettered { queue: String, dlq: String },
}
