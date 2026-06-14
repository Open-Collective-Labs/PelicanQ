use std::pin::Pin;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use pelicanq_core::error::PelicanError;
use pelicanq_core::message::Message as CoreMessage;
use pelicanq_core::queue::PublishOutcome;
use pelicanq_raft::QueueOperation;
use pelicanq_raft::QueueOperationResponse;
use pelicanq_raft::WriteResult;

use crate::api::{AppEngine, SharedState};
use crate::grpc::pb::queue_service_server::QueueService;
use crate::grpc::pb::{
    self as proto, AckRequest, AckResponse, ConsumeBatchRequest, ConsumeBatchResponse,
    ConsumeRequest, ConsumeResponse, ConsumeStreamAck, DeclareQueueRequest,
    DeclareQueueResponse, ListQueuesRequest, ListQueuesResponse, NackRequest, NackResponse,
    PublishBatchRequest, PublishBatchResponse, PublishRequest, PublishResponse,
};

fn pelican_error_to_status(e: PelicanError) -> Status {
    match &e {
        PelicanError::QueueNotFound { .. } => Status::not_found(e.to_string()),
        PelicanError::QueueAlreadyExists { .. } => Status::already_exists(e.to_string()),
        PelicanError::StorageLimitExceeded { .. } => Status::resource_exhausted(e.to_string()),
        PelicanError::InvalidDeliveryTag { .. } => Status::invalid_argument(e.to_string()),
        PelicanError::MessageDeadLettered { .. } => Status::failed_precondition(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

fn write_result_to_status(result: &WriteResult) -> Result<(), Status> {
    match result {
        WriteResult::Ok(_) => Ok(()),
        WriteResult::NotLeader { leader_node } => {
            let msg = match leader_node {
                Some(node) => format!("not leader, leader at {}", node.client_addr),
                None => "not leader".to_string(),
            };
            Err(Status::unavailable(msg))
        }
        WriteResult::Error(msg) => Err(Status::internal(msg.clone())),
    }
}

fn core_message_to_proto(msg: &CoreMessage) -> proto::Message {
    proto::Message {
        id: msg.id.to_string(),
        payload: msg.payload.clone(),
        headers: msg.headers.clone(),
        timestamp: msg.timestamp,
        priority: msg.priority as u32,
        deliver_at: msg.deliver_at,
        dedup_key: msg.dedup_key.clone(),
        delivery_attempts: msg.delivery_attempts,
    }
}

fn proto_message_to_core(msg: &proto::Message) -> CoreMessage {
    CoreMessage {
        id: msg.id.parse().unwrap_or_else(|_| uuid::Uuid::new_v4()),
        payload: msg.payload.clone(),
        headers: msg.headers.clone(),
        timestamp: msg.timestamp,
        delivery_attempts: msg.delivery_attempts,
        priority: msg.priority as u8,
        deliver_at: msg.deliver_at,
        dedup_key: msg.dedup_key.clone(),
    }
}

async fn consume_one(
    engine: &AppEngine,
    queue: &str,
) -> Option<(pelicanq_core::message::DeliveryTag, CoreMessage)> {
    match engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            mgr.consume(queue).ok()?
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::Consume {
                queue: queue.to_string(),
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::Consume(Ok(opt))) => opt,
                WriteResult::Ok(QueueOperationResponse::Consume(Err(_))) => None,
                _ => None,
            }
        }
    }
}

async fn handle_ack(
    engine: &AppEngine,
    queue: &str,
    delivery_tag: u64,
) -> Result<(), Status> {
    match engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            mgr.ack(queue, delivery_tag.into())
                .map_err(pelican_error_to_status)
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::Ack {
                queue: queue.to_string(),
                tag: delivery_tag.into(),
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::Ack(Ok(()))) => Ok(()),
                WriteResult::Ok(QueueOperationResponse::Ack(Err(e))) => {
                    Err(pelican_error_to_status(e))
                }
                result => {
                    write_result_to_status(&result)?;
                    Err(Status::internal("unexpected ack response"))
                }
            }
        }
    }
}

async fn handle_nack(
    engine: &AppEngine,
    queue: &str,
    delivery_tag: u64,
) -> Result<(), Status> {
    match engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            mgr.nack(queue, delivery_tag.into())
                .map_err(pelican_error_to_status)
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::Nack {
                queue: queue.to_string(),
                tag: delivery_tag.into(),
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::Nack(Ok(()))) => Ok(()),
                WriteResult::Ok(QueueOperationResponse::Nack(Err(e))) => {
                    Err(pelican_error_to_status(e))
                }
                result => {
                    write_result_to_status(&result)?;
                    Err(Status::internal("unexpected nack response"))
                }
            }
        }
    }
}

pub struct QueueServiceImpl {
    state: SharedState,
}

impl QueueServiceImpl {
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl QueueService for QueueServiceImpl {
    type ConsumeStreamStream =
        Pin<Box<dyn Stream<Item = Result<proto::ConsumedMessage, Status>> + Send>>;

    async fn declare_queue(
        &self,
        request: Request<DeclareQueueRequest>,
    ) -> Result<Response<DeclareQueueResponse>, Status> {
        let req = request.into_inner();

        match &self.state.engine {
            AppEngine::Solo(qm_arc) => {
                let mut mgr = qm_arc.lock().unwrap();
                match mgr.declare_queue(&req.name) {
                    Ok(()) => Ok(Response::new(DeclareQueueResponse { created: true })),
                    Err(PelicanError::QueueAlreadyExists { .. }) => {
                        Ok(Response::new(DeclareQueueResponse { created: false }))
                    }
                    Err(e) => Err(pelican_error_to_status(e)),
                }
            }
            AppEngine::Flock(flock) => {
                let op = QueueOperation::DeclareQueue {
                    name: req.name,
                    policy: Default::default(),
                };
                match flock.client_write(op).await {
                    WriteResult::Ok(QueueOperationResponse::DeclareQueue(Ok(()))) => {
                        Ok(Response::new(DeclareQueueResponse { created: true }))
                    }
                    WriteResult::Ok(QueueOperationResponse::DeclareQueue(Err(
                        PelicanError::QueueAlreadyExists { .. },
                    ))) => Ok(Response::new(DeclareQueueResponse { created: false })),
                    WriteResult::Ok(QueueOperationResponse::DeclareQueue(Err(e))) => {
                        Err(pelican_error_to_status(e))
                    }
                    result => {
                        write_result_to_status(&result)?;
                        Err(Status::internal("unexpected response type"))
                    }
                }
            }
        }
    }

    async fn list_queues(
        &self,
        _request: Request<ListQueuesRequest>,
    ) -> Result<Response<ListQueuesResponse>, Status> {
        let queues = match &self.state.engine {
            AppEngine::Solo(qm_arc) => {
                let mgr = qm_arc.lock().unwrap();
                let names = mgr.list_queues();
                let mut queues = Vec::with_capacity(names.len());
                for name in names {
                    let depth = mgr.depth(&name).unwrap_or(0) as u64;
                    queues.push(proto::QueueInfo {
                        name,
                        depth,
                        scheduled_depth: 0,
                    });
                }
                queues
            }
            AppEngine::Flock(flock) => {
                flock
                    .with_qm(|mgr| {
                        let names = mgr.list_queues();
                        let mut queues = Vec::with_capacity(names.len());
                        for name in names {
                            let depth = mgr.depth(&name).unwrap_or(0) as u64;
                            queues.push(proto::QueueInfo {
                                name,
                                depth,
                                scheduled_depth: 0,
                            });
                        }
                        queues
                    })
                    .await
            }
        };

        Ok(Response::new(ListQueuesResponse { queues }))
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let req = request.into_inner();
        let msg = proto_message_to_core(&req.message.expect("message required"));

        let response = match &self.state.engine {
            AppEngine::Solo(qm_arc) => {
                let mut mgr = qm_arc.lock().unwrap();
                match mgr.publish(&req.queue, msg) {
                    Ok(PublishOutcome::Stored(id)) => PublishResponse {
                        id: id.to_string(),
                        deduplicated: false,
                    },
                    Ok(PublishOutcome::Deduplicated) => PublishResponse {
                        id: String::new(),
                        deduplicated: true,
                    },
                    Err(e) => return Err(pelican_error_to_status(e)),
                }
            }
            AppEngine::Flock(flock) => {
                let op = QueueOperation::Publish {
                    queue: req.queue,
                    message: msg,
                };
                match flock.client_write(op).await {
                    WriteResult::Ok(QueueOperationResponse::Publish(Ok(PublishOutcome::Stored(
                        id,
                    )))) => PublishResponse {
                        id: id.to_string(),
                        deduplicated: false,
                    },
                    WriteResult::Ok(QueueOperationResponse::Publish(Ok(
                        PublishOutcome::Deduplicated,
                    ))) => PublishResponse {
                        id: String::new(),
                        deduplicated: true,
                    },
                    WriteResult::Ok(QueueOperationResponse::Publish(Err(e))) => {
                        return Err(pelican_error_to_status(e))
                    }
                    result => {
                        write_result_to_status(&result)?;
                        return Err(Status::internal("unexpected response type"));
                    }
                }
            }
        };

        Ok(Response::new(response))
    }

    async fn publish_batch(
        &self,
        request: Request<PublishBatchRequest>,
    ) -> Result<Response<PublishBatchResponse>, Status> {
        let req = request.into_inner();
        let messages: Vec<CoreMessage> = req
            .messages
            .iter()
            .map(proto_message_to_core)
            .collect();

        let results = match &self.state.engine {
            AppEngine::Solo(qm_arc) => {
                let mut mgr = qm_arc.lock().unwrap();
                let mut results = Vec::with_capacity(messages.len());
                for msg in messages {
                    match mgr.publish(&req.queue, msg) {
                        Ok(PublishOutcome::Stored(id)) => results.push(PublishResponse {
                            id: id.to_string(),
                            deduplicated: false,
                        }),
                        Ok(PublishOutcome::Deduplicated) => results.push(PublishResponse {
                            id: String::new(),
                            deduplicated: true,
                        }),
                        Err(e) => return Err(pelican_error_to_status(e)),
                    }
                }
                results
            }
            AppEngine::Flock(flock) => {
                let op = QueueOperation::PublishBatch {
                    queue: req.queue,
                    messages,
                };
                match flock.client_write(op).await {
                    WriteResult::Ok(QueueOperationResponse::PublishBatch(Ok(outcomes))) => {
                        outcomes
                            .into_iter()
                            .map(|outcome| match outcome {
                                PublishOutcome::Stored(id) => PublishResponse {
                                    id: id.to_string(),
                                    deduplicated: false,
                                },
                                PublishOutcome::Deduplicated => PublishResponse {
                                    id: String::new(),
                                    deduplicated: true,
                                },
                            })
                            .collect()
                    }
                    WriteResult::Ok(QueueOperationResponse::PublishBatch(Err(e))) => {
                        return Err(pelican_error_to_status(e))
                    }
                    result => {
                        write_result_to_status(&result)?;
                        return Err(Status::internal("unexpected response type"));
                    }
                }
            }
        };

        Ok(Response::new(PublishBatchResponse { results }))
    }

    async fn consume(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<ConsumeResponse>, Status> {
        let req = request.into_inner();

        match &self.state.engine {
            AppEngine::Solo(qm_arc) => {
                let mut mgr = qm_arc.lock().unwrap();
                match mgr.consume(&req.queue) {
                    Ok(Some((tag, msg))) => Ok(Response::new(ConsumeResponse {
                        message: Some(proto::ConsumedMessage {
                            delivery_tag: tag.into(),
                            message: Some(core_message_to_proto(&msg)),
                        }),
                    })),
                    Ok(None) => Ok(Response::new(ConsumeResponse { message: None })),
                    Err(e) => Err(pelican_error_to_status(e)),
                }
            }
            AppEngine::Flock(flock) => {
                let op = QueueOperation::Consume {
                    queue: req.queue,
                };
                match flock.client_write(op).await {
                    WriteResult::Ok(QueueOperationResponse::Consume(Ok(Some((tag, msg))))) => {
                        Ok(Response::new(ConsumeResponse {
                            message: Some(proto::ConsumedMessage {
                                delivery_tag: tag.into(),
                                message: Some(core_message_to_proto(&msg)),
                            }),
                        }))
                    }
                    WriteResult::Ok(QueueOperationResponse::Consume(Ok(None))) => {
                        Ok(Response::new(ConsumeResponse { message: None }))
                    }
                    WriteResult::Ok(QueueOperationResponse::Consume(Err(e))) => {
                        Err(pelican_error_to_status(e))
                    }
                    result => {
                        write_result_to_status(&result)?;
                        Err(Status::internal("unexpected response type"))
                    }
                }
            }
        }
    }

    async fn consume_batch(
        &self,
        request: Request<ConsumeBatchRequest>,
    ) -> Result<Response<ConsumeBatchResponse>, Status> {
        let req = request.into_inner();
        let max = req.max as usize;

        let messages = match &self.state.engine {
            AppEngine::Solo(qm_arc) => {
                let mut mgr = qm_arc.lock().unwrap();
                let mut messages = Vec::new();
                for _ in 0..max {
                    match mgr.consume(&req.queue) {
                        Ok(Some((tag, msg))) => {
                            messages.push(proto::ConsumedMessage {
                                delivery_tag: tag.into(),
                                message: Some(core_message_to_proto(&msg)),
                            });
                        }
                        Ok(None) => break,
                        Err(e) => return Err(pelican_error_to_status(e)),
                    }
                }
                messages
            }
            AppEngine::Flock(flock) => {
                let op = QueueOperation::ConsumeBatch {
                    queue: req.queue,
                    max,
                };
                match flock.client_write(op).await {
                    WriteResult::Ok(QueueOperationResponse::ConsumeBatch(Ok(items))) => items
                        .into_iter()
                        .map(|(tag, msg)| proto::ConsumedMessage {
                            delivery_tag: tag.into(),
                            message: Some(core_message_to_proto(&msg)),
                        })
                        .collect(),
                    WriteResult::Ok(QueueOperationResponse::ConsumeBatch(Err(e))) => {
                        return Err(pelican_error_to_status(e))
                    }
                    result => {
                        write_result_to_status(&result)?;
                        return Err(Status::internal("unexpected response type"));
                    }
                }
            }
        };

        Ok(Response::new(ConsumeBatchResponse { messages }))
    }

    async fn consume_stream(
        &self,
        request: Request<tonic::Streaming<ConsumeStreamAck>>,
    ) -> Result<Response<Self::ConsumeStreamStream>, Status> {
        let mut inbound_stream = request.into_inner();
        let state = self.state.clone();
        let (tx, rx) = mpsc::channel::<Result<proto::ConsumedMessage, Status>>(64);
        let mut queue: Option<String> = None;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    ack_opt = inbound_stream.next() => {
                        match ack_opt {
                            Some(Ok(ack)) => {
                                if queue.is_none() {
                                    if let Some(ref q) = ack.queue {
                                        if !q.is_empty() {
                                            queue = Some(q.clone());
                                            continue;
                                        }
                                    }
                                    let _ = tx.send(Err(Status::invalid_argument(
                                        "first ConsumeStreamAck must include a non-empty queue",
                                    ))).await;
                                    return;
                                }

                                let q = queue.as_ref().unwrap();
                                if let Some(tag) = ack.delivery_tag {
                                    let result = handle_ack(&state.engine, q, tag).await;
                                    if let Err(e) = result {
                                        let _ = tx.send(Err(e)).await;
                                        return;
                                    }
                                } else if let Some(tag) = ack.nack_delivery_tag {
                                    let result = handle_nack(&state.engine, q, tag).await;
                                    if let Err(e) = result {
                                        let _ = tx.send(Err(e)).await;
                                        return;
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                                return;
                            }
                            None => {
                                return;
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        let q = match queue {
                            Some(ref q) => q.clone(),
                            None => continue,
                        };

                        let consumed = consume_one(&state.engine, &q).await;

                        if let Some((tag, msg)) = consumed {
                            let cm = proto::ConsumedMessage {
                                delivery_tag: tag.into(),
                                message: Some(core_message_to_proto(&msg)),
                            };
                            if tx.send(Ok(cm)).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn ack(
        &self,
        request: Request<AckRequest>,
    ) -> Result<Response<AckResponse>, Status> {
        let req = request.into_inner();

        match &self.state.engine {
            AppEngine::Solo(qm_arc) => {
                let mut mgr = qm_arc.lock().unwrap();
                mgr.ack(&req.queue, req.delivery_tag.into())
                    .map_err(pelican_error_to_status)?;
                Ok(Response::new(AckResponse {}))
            }
            AppEngine::Flock(flock) => {
                let op = QueueOperation::Ack {
                    queue: req.queue,
                    tag: req.delivery_tag.into(),
                };
                match flock.client_write(op).await {
                    WriteResult::Ok(QueueOperationResponse::Ack(Ok(()))) => {
                        Ok(Response::new(AckResponse {}))
                    }
                    WriteResult::Ok(QueueOperationResponse::Ack(Err(e))) => {
                        Err(pelican_error_to_status(e))
                    }
                    result => {
                        write_result_to_status(&result)?;
                        Err(Status::internal("unexpected response type"))
                    }
                }
            }
        }
    }

    async fn nack(
        &self,
        request: Request<NackRequest>,
    ) -> Result<Response<NackResponse>, Status> {
        let req = request.into_inner();

        match &self.state.engine {
            AppEngine::Solo(qm_arc) => {
                let mut mgr = qm_arc.lock().unwrap();
                mgr.nack(&req.queue, req.delivery_tag.into())
                    .map_err(pelican_error_to_status)?;
                Ok(Response::new(NackResponse {}))
            }
            AppEngine::Flock(flock) => {
                let op = QueueOperation::Nack {
                    queue: req.queue,
                    tag: req.delivery_tag.into(),
                };
                match flock.client_write(op).await {
                    WriteResult::Ok(QueueOperationResponse::Nack(Ok(()))) => {
                        Ok(Response::new(NackResponse {}))
                    }
                    WriteResult::Ok(QueueOperationResponse::Nack(Err(e))) => {
                        Err(pelican_error_to_status(e))
                    }
                    result => {
                        write_result_to_status(&result)?;
                        Err(Status::internal("unexpected response type"))
                    }
                }
            }
        }
    }
}
