use tonic::transport::Channel;

use crate::error::PelicanClientError;
use crate::pb::admin_service_client::AdminServiceClient;
use crate::pb::queue_service_client::QueueServiceClient;
use crate::pb::{self};
use crate::types::{
    ClientMessage, Delivery, PublishResult, QueueInfo, QueueOptions,
};

/// A client for interacting with a PelicanQ daemon over gRPC.
#[derive(Clone)]
pub struct PelicanClient {
    channel: Channel,
}

impl PelicanClient {
    /// Connect to a PelicanQ daemon at the given gRPC address,
    /// e.g. `"http://127.0.0.1:7072"`.
    pub async fn connect(addr: impl Into<String>) -> Result<Self, PelicanClientError> {
        let addr = addr.into();
        let channel = Channel::from_shared(addr)
            .map_err(|e| PelicanClientError::Server(format!("invalid address: {e}")))?
            .connect()
            .await?;
        Ok(Self { channel })
    }

    /// Creates a client from an existing tonic `Channel`.
    pub fn from_channel(channel: Channel) -> Self {
        Self { channel }
    }

    /// Declares a queue. Idempotent — returns `true` if newly created,
    /// `false` if already existed.
    pub async fn declare_queue(
        &mut self,
        name: &str,
        opts: QueueOptions,
    ) -> Result<bool, PelicanClientError> {
        let mut client = QueueServiceClient::new(self.channel.clone());
        let resp = client
            .declare_queue(pb::DeclareQueueRequest {
                name: name.to_string(),
                max_age_secs: opts.max_age_secs,
                max_messages: opts.max_messages,
                max_delivery_attempts: opts.max_delivery_attempts,
                dead_letter_queue: opts.dead_letter_queue,
                dedup_window_secs: opts.dedup_window_secs,
            })
            .await?;
        Ok(resp.into_inner().created)
    }

    /// Publishes a single message to a queue.
    pub async fn publish(
        &mut self,
        queue: &str,
        message: ClientMessage,
    ) -> Result<PublishResult, PelicanClientError> {
        let mut client = QueueServiceClient::new(self.channel.clone());
        let resp = client
            .publish(pb::PublishRequest {
                queue: queue.to_string(),
                message: Some(pb::Message::from(message)),
            })
            .await?;
        Ok(PublishResult::from(resp.into_inner()))
    }

    /// Publishes multiple messages in a single batch call.
    pub async fn publish_batch(
        &mut self,
        queue: &str,
        messages: Vec<ClientMessage>,
    ) -> Result<Vec<PublishResult>, PelicanClientError> {
        let mut client = QueueServiceClient::new(self.channel.clone());
        let proto_messages: Vec<pb::Message> =
            messages.into_iter().map(pb::Message::from).collect();
        let resp = client
            .publish_batch(pb::PublishBatchRequest {
                queue: queue.to_string(),
                messages: proto_messages,
            })
            .await?;
        let results: Vec<PublishResult> = resp
            .into_inner()
            .results
            .into_iter()
            .map(PublishResult::from)
            .collect();
        Ok(results)
    }

    /// Consumes a single message from a queue. Returns `None` if the queue is
    /// empty.
    pub async fn consume(
        &mut self,
        queue: &str,
    ) -> Result<Option<Delivery>, PelicanClientError> {
        let mut client = QueueServiceClient::new(self.channel.clone());
        let resp = client
            .consume(pb::ConsumeRequest {
                queue: queue.to_string(),
            })
            .await?;
        Ok(resp.into_inner().message.map(|cm| Delivery::from(cm)))
    }

    /// Consumes up to `max` messages from a queue in a single batch call.
    pub async fn consume_batch(
        &mut self,
        queue: &str,
        max: u32,
    ) -> Result<Vec<Delivery>, PelicanClientError> {
        let mut client = QueueServiceClient::new(self.channel.clone());
        let resp = client
            .consume_batch(pb::ConsumeBatchRequest {
                queue: queue.to_string(),
                max,
            })
            .await?;
        Ok(resp
            .into_inner()
            .messages
            .into_iter()
            .map(Delivery::from)
            .collect())
    }

    /// Acknowledges a message, removing it from the in-flight store.
    pub async fn ack(
        &mut self,
        queue: &str,
        delivery_tag: u64,
    ) -> Result<(), PelicanClientError> {
        let mut client = QueueServiceClient::new(self.channel.clone());
        client
            .ack(pb::AckRequest {
                queue: queue.to_string(),
                delivery_tag,
            })
            .await?;
        Ok(())
    }

    /// Negatively acknowledges a message, returning it to the queue (or
    /// dead-lettering it if delivery attempts are exhausted).
    pub async fn nack(
        &mut self,
        queue: &str,
        delivery_tag: u64,
    ) -> Result<(), PelicanClientError> {
        let mut client = QueueServiceClient::new(self.channel.clone());
        client
            .nack(pb::NackRequest {
                queue: queue.to_string(),
                delivery_tag,
            })
            .await?;
        Ok(())
    }

    /// Lists all queues and their depths.
    pub async fn list_queues(
        &mut self,
    ) -> Result<Vec<QueueInfo>, PelicanClientError> {
        let mut client = QueueServiceClient::new(self.channel.clone());
        let resp = client.list_queues(pb::ListQueuesRequest {}).await?;
        Ok(resp
            .into_inner()
            .queues
            .into_iter()
            .map(QueueInfo::from)
            .collect())
    }

    /// Checks the daemon health. Returns `Ok(())` if healthy.
    pub async fn health(&mut self) -> Result<(), PelicanClientError> {
        let mut client = AdminServiceClient::new(self.channel.clone());
        let resp = client.health(pb::HealthRequest {}).await?;
        let status = resp.into_inner().status;
        if status == "ok" {
            Ok(())
        } else {
            Err(PelicanClientError::Server(format!(
                "unhealthy: {status}"
            )))
        }
    }
}
