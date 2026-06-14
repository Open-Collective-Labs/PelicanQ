use pelicanq_core::error::PelicanError;
use pelicanq_core::message::{DeliveryTag, Message};
use pelicanq_core::queue::PublishOutcome;
use pelicanq_core::retention::RetentionPolicy;
use serde::{Deserialize, Serialize};

/// All mutating operations that go through Raft. Mirrors QueueManager's
/// mutating methods from Phases 1-2.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueOperation {
    DeclareQueue {
        name: String,
        policy: RetentionPolicy,
    },
    Publish {
        queue: String,
        message: Message,
    },
    PublishBatch {
        queue: String,
        messages: Vec<Message>,
    },
    Consume {
        queue: String,
    },
    ConsumeBatch {
        queue: String,
        max: usize,
    },
    Ack {
        queue: String,
        tag: DeliveryTag,
    },
    Nack {
        queue: String,
        tag: DeliveryTag,
    },
    ApplyRetention {
        queue: String,
    },
    PromoteScheduled {
        queue: String,
    },
}

/// Response variants, one per `QueueOperation` variant, mirroring
/// `QueueManager`'s return types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueOperationResponse {
    DeclareQueue(Result<(), PelicanError>),
    Publish(Result<PublishOutcome, PelicanError>),
    PublishBatch(Result<Vec<PublishOutcome>, PelicanError>),
    Consume(Result<Option<(DeliveryTag, Message)>, PelicanError>),
    ConsumeBatch(Result<Vec<(DeliveryTag, Message)>, PelicanError>),
    Ack(Result<(), PelicanError>),
    Nack(Result<(), PelicanError>),
    ApplyRetention(Result<usize, PelicanError>),
    PromoteScheduled(Result<usize, PelicanError>),
}
