use std::collections::HashMap;
use std::time::Duration;

use futures::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, info, warn};

use pelicanq_core::error::PelicanError;
use pelicanq_core::message::{DeliveryTag, Message};
use pelicanq_core::retention::RetentionPolicy;
use pelicanq_raft::{QueueOperation, QueueOperationResponse, WriteResult};
use rumqttc::mqttbytes::v4::{
    Codec, ConnAck, ConnectReturnCode, Packet, Publish, PubAck, SubAck,
    SubscribeReasonCode, UnsubAck,
};
use rumqttc::mqttbytes::QoS;

use crate::api::{AppEngine, SharedState};

// ---------- public entry point ----------

pub async fn listen(state: SharedState, addr: String) {
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind MQTT address");
    info!("MQTT listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let state = state.clone();
                tokio::spawn(handle_connection(stream, state));
            }
            Err(e) => {
                warn!("MQTT accept error: {e}");
            }
        }
    }
}

// ---------- outgoing message ----------

enum MqttOutgoing {
    Incoming {
        queue: String,
        tag: DeliveryTag,
        payload: Vec<u8>,
    },
}

// ---------- engine helpers ----------

fn has_wildcard(filter: &str) -> bool {
    filter.contains('+') || filter.contains('#')
}

async fn declare_queue(engine: &AppEngine, queue: &str) -> bool {
    match engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            mgr.declare_queue(queue).is_ok()
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::DeclareQueue {
                name: queue.to_string(),
                policy: RetentionPolicy::default(),
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::DeclareQueue(Ok(()))) => true,
                WriteResult::Ok(QueueOperationResponse::DeclareQueue(
                    Err(PelicanError::QueueAlreadyExists { .. }),
                )) => true,
                _ => false,
            }
        }
    }
}

async fn consume_one(engine: &AppEngine, queue: &str) -> Option<(DeliveryTag, Message)> {
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
                _ => None,
            }
        }
    }
}

async fn ack_message(engine: &AppEngine, queue: &str, tag: DeliveryTag) {
    let result = match engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            mgr.ack(queue, tag)
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::Ack {
                queue: queue.to_string(),
                tag,
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::Ack(result)) => result,
                _ => {
                    warn!("MQTT: ack failed for {} tag {}", queue, tag.0);
                    return;
                }
            }
        }
    };
    if let Err(e) = result {
        warn!("MQTT: ack error for {} tag {}: {e}", queue, tag.0);
    }
}

async fn publish_message(engine: &AppEngine, queue: &str, payload: Vec<u8>) -> bool {
    let msg = Message {
        id: uuid::Uuid::new_v4(),
        payload,
        headers: HashMap::new(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        delivery_attempts: 0,
        priority: 0,
        deliver_at: None,
        dedup_key: None,
    };

    match engine {
        AppEngine::Solo(qm_arc) => {
            let mut mgr = qm_arc.lock().unwrap();
            mgr.publish(queue, msg).is_ok()
        }
        AppEngine::Flock(flock) => {
            let op = QueueOperation::Publish {
                queue: queue.to_string(),
                message: msg,
            };
            match flock.client_write(op).await {
                WriteResult::Ok(QueueOperationResponse::Publish(Ok(_))) => true,
                _ => false,
            }
        }
    }
}

// ---------- subscription poller ----------

async fn subscription_poller(
    state: SharedState,
    queue: String,
    tx: mpsc::Sender<MqttOutgoing>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        interval.tick().await;
        let result = consume_one(&state.engine, &queue).await;
        if let Some((tag, msg)) = result {
            let outgoing = MqttOutgoing::Incoming {
                queue: queue.clone(),
                tag,
                payload: msg.payload,
            };
            if tx.send(outgoing).await.is_err() {
                break;
            }
        }
    }
}

// ---------- per-connection handler ----------

fn make_codec() -> Codec {
    Codec {
        max_incoming_size: 10 * 1024 * 1024,
        max_outgoing_size: 10 * 1024 * 1024,
    }
}

async fn handle_connection(stream: TcpStream, state: SharedState) {
    let (read_half, write_half) = stream.into_split();
    let mut read = FramedRead::new(read_half, make_codec());
    let mut write = FramedWrite::new(write_half, make_codec());

    // ----- CONNECT -----
    match read.next().await {
        Some(Ok(Packet::Connect(_))) => {}
        Some(Ok(other)) => {
            warn!("MQTT: expected CONNECT, got {:?}", other);
            return;
        }
        Some(Err(e)) => {
            warn!("MQTT: CONNECT read error: {e}");
            return;
        }
        None => return,
    }

    let connack = Packet::ConnAck(ConnAck {
        code: ConnectReturnCode::Success,
        session_present: false,
    });
    if let Err(e) = write.send(connack).await {
        warn!("MQTT: CONNACK error: {e}");
        return;
    }

    // ----- per-connection state -----
    let (tx, mut rx) = mpsc::channel::<MqttOutgoing>(256);
    let mut inflight: HashMap<u16, (String, DeliveryTag)> = HashMap::new();
    let mut next_pkid: u16 = 1;
    let mut subscriptions: HashMap<String, JoinHandle<()>> = HashMap::new();

    // ----- main loop -----
    loop {
        tokio::select! {
            incoming = read.next() => {
                match incoming {
                    Some(Ok(packet)) => {
                        if !handle_client_packet(
                            packet,
                            &state,
                            &mut write,
                            &mut inflight,
                            &mut subscriptions,
                            &tx,
                        ).await {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        warn!("MQTT: read error: {e}");
                        break;
                    }
                    None => break,
                }
            }
            outgoing = rx.recv() => {
                match outgoing {
                    Some(MqttOutgoing::Incoming { queue, tag, payload }) => {
                        let pkid = next_pkid;
                        next_pkid = next_pkid.wrapping_add(1);
                        let publish = Packet::Publish(Publish {
                            dup: false,
                            qos: QoS::AtLeastOnce,
                            retain: false,
                            topic: queue.clone(),
                            pkid,
                            payload: payload.into(),
                        });
                        inflight.insert(pkid, (queue, tag));
                        if let Err(e) = write.send(publish).await {
                            warn!("MQTT: publish write error: {e}");
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    // ----- cleanup: abort all subscription pollers -----
    for (_, handle) in subscriptions {
        handle.abort();
    }
    debug!("MQTT: connection closed");
}

#[allow(clippy::too_many_arguments)]
async fn handle_client_packet(
    packet: Packet,
    state: &SharedState,
    write: &mut FramedWrite<tokio::net::tcp::OwnedWriteHalf, Codec>,
    inflight: &mut HashMap<u16, (String, DeliveryTag)>,
    subscriptions: &mut HashMap<String, JoinHandle<()>>,
    tx: &mpsc::Sender<MqttOutgoing>,
) -> bool {
    match packet {
        Packet::Publish(p) => {
            let queue = p.topic;
            let qos = p.qos;
            let pkid = p.pkid;

            declare_queue(&state.engine, &queue).await;
            let ok = publish_message(&state.engine, &queue, p.payload.to_vec()).await;

            if (qos as u8) >= 1 {
                let puback = Packet::PubAck(PubAck { pkid });
                if let Err(e) = write.send(puback).await {
                    warn!("MQTT: puback error: {e}");
                    return false;
                }
            }

            if !ok {
                warn!("MQTT: publish to {queue} failed");
            }
            true
        }

        Packet::Subscribe(sub) => {
            let sub_pkid = sub.pkid;
            let mut return_codes = Vec::with_capacity(sub.filters.len());

            for filter in sub.filters {
                if has_wildcard(&filter.path) {
                    return_codes.push(SubscribeReasonCode::Failure);
                    continue;
                }

                let queue = &filter.path;
                declare_queue(&state.engine, queue).await;

                if subscriptions.contains_key(queue) {
                    return_codes.push(SubscribeReasonCode::Success(QoS::AtLeastOnce));
                    continue;
                }

                let qos = if filter.qos as u8 == 0 {
                    QoS::AtMostOnce
                } else {
                    QoS::AtLeastOnce
                };

                let ps = state.clone();
                let pq = queue.clone();
                let pt = tx.clone();
                let handle = tokio::spawn(subscription_poller(ps, pq, pt));

                subscriptions.insert(queue.clone(), handle);
                return_codes.push(SubscribeReasonCode::Success(qos));
            }

            let suback = Packet::SubAck(SubAck {
                pkid: sub_pkid,
                return_codes,
            });
            if let Err(e) = write.send(suback).await {
                warn!("MQTT: suback error: {e}");
                return false;
            }
            true
        }

        Packet::Unsubscribe(unsub) => {
            for topic in unsub.topics {
                if let Some(handle) = subscriptions.remove(&topic) {
                    handle.abort();
                }
            }
            let ua = Packet::UnsubAck(UnsubAck {
                pkid: unsub.pkid,
            });
            if let Err(e) = write.send(ua).await {
                warn!("MQTT: unsuback error: {e}");
                return false;
            }
            true
        }

        Packet::PubAck(puback) => {
            if let Some((queue, tag)) = inflight.remove(&puback.pkid) {
                ack_message(&state.engine, &queue, tag).await;
            }
            true
        }

        Packet::PingReq => {
            if let Err(e) = write.send(Packet::PingResp).await {
                warn!("MQTT: pingresp error: {e}");
                return false;
            }
            true
        }

        Packet::Disconnect => {
            debug!("MQTT: client DISCONNECT");
            false
        }

        _ => {
            debug!("MQTT: ignoring unhandled packet");
            true
        }
    }
}
