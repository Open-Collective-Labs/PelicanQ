#[derive(Debug, thiserror::Error)]
pub enum PelicanError {
    #[error("queue not found: {0}")]
    QueueNotFound(String),

    #[error("queue already exists: {0}")]
    QueueAlreadyExists(String),

    #[error("storage error: {0}")]
    Storage(#[from] sled::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("invalid delivery tag: {0}")]
    InvalidDeliveryTag(u64),

    #[error("storage watermark exceeded: disk usage at {used_pct}%, limit is {limit_pct}%")]
    StorageLimitExceeded { used_pct: u8, limit_pct: u8 },
}
