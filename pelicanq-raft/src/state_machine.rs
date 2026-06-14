use std::path::Path;

use pelicanq_core::error::PelicanError;
use pelicanq_core::queue::QueueManager;
use pelicanq_core::snapshot::QueueManagerSnapshot;

use crate::requests::{QueueOperation, QueueOperationResponse};

/// Apply a single `QueueOperation` to a `QueueManager`, returning the
/// corresponding `QueueOperationResponse`.
pub fn apply_operation(
    qm: &mut QueueManager,
    op: QueueOperation,
) -> QueueOperationResponse {
    match op {
        QueueOperation::DeclareQueue { name, policy } => {
            let result = if policy == pelicanq_core::retention::RetentionPolicy::default() {
                qm.declare_queue(&name)
            } else {
                qm.declare_queue_with_retention(&name, policy)
            };
            QueueOperationResponse::DeclareQueue(result)
        }
        QueueOperation::Publish { queue, message } => {
            let result = qm.publish(&queue, message);
            QueueOperationResponse::Publish(result)
        }
        QueueOperation::PublishBatch { queue, messages } => {
            let mut outcomes = Vec::with_capacity(messages.len());
            for msg in messages {
                match qm.publish(&queue, msg) {
                    Ok(outcome) => outcomes.push(outcome),
                    Err(e) => return QueueOperationResponse::PublishBatch(Err(e)),
                }
            }
            QueueOperationResponse::PublishBatch(Ok(outcomes))
        }
        QueueOperation::Consume { queue } => {
            let result = qm.consume(&queue);
            QueueOperationResponse::Consume(result)
        }
        QueueOperation::ConsumeBatch { queue, max } => {
            let mut results = Vec::with_capacity(max);
            for _ in 0..max {
                match qm.consume(&queue) {
                    Ok(Some(pair)) => results.push(pair),
                    Ok(None) => break,
                    Err(e) => return QueueOperationResponse::ConsumeBatch(Err(e)),
                }
            }
            QueueOperationResponse::ConsumeBatch(Ok(results))
        }
        QueueOperation::Ack { queue, tag } => {
            let result = qm.ack(&queue, tag);
            QueueOperationResponse::Ack(result)
        }
        QueueOperation::Nack { queue, tag } => {
            let result = qm.nack(&queue, tag);
            QueueOperationResponse::Nack(result)
        }
        QueueOperation::ApplyRetention { queue } => {
            let result = qm.apply_retention(&queue);
            QueueOperationResponse::ApplyRetention(result)
        }
        QueueOperation::PromoteScheduled { queue } => {
            let result = qm.promote_scheduled(&queue);
            QueueOperationResponse::PromoteScheduled(result)
        }
    }
}

/// Build a snapshot by exporting the QueueManager's state.
pub fn build_snapshot(qm: &QueueManager) -> Result<Vec<u8>, PelicanError> {
    let snapshot = qm.export_snapshot()?;
    let data = serde_json::to_vec(&snapshot)
        .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
    Ok(data)
}

/// Install a snapshot from serialized bytes, restoring the QueueManager's state.
pub fn install_snapshot(
    qm: &mut QueueManager,
    data: &[u8],
) -> Result<(), PelicanError> {
    let snapshot: QueueManagerSnapshot = serde_json::from_slice(data)
        .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
    qm.restore_from_snapshot(&snapshot)
}

/// Open a QueueManager at the given data directory.
pub fn open_queue_manager(data_dir: &Path) -> Result<QueueManager, PelicanError> {
    QueueManager::open(data_dir, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pelicanq_core::message::Message;

    fn with_qm<F>(f: F)
    where
        F: FnOnce(QueueManager),
    {
        let dir = tempfile::tempdir().unwrap();
        let qm = QueueManager::open(dir.path(), None).unwrap();
        f(qm);
    }

    #[test]
    fn test_declare_then_publish_then_consume() {
        with_qm(|mut qm| {
            // Declare
            let resp = apply_operation(
                &mut qm,
                QueueOperation::DeclareQueue {
                    name: "orders".into(),
                    policy: Default::default(),
                },
            );
            assert!(matches!(resp, QueueOperationResponse::DeclareQueue(Ok(()))));

            // Publish
            let resp = apply_operation(
                &mut qm,
                QueueOperation::Publish {
                    queue: "orders".into(),
                    message: Message::new(b"order-1".to_vec(), Default::default()),
                },
            );
            assert!(matches!(resp, QueueOperationResponse::Publish(Ok(_))));

            // Consume directly to verify
            let consumed = qm.consume("orders").unwrap();
            assert!(consumed.is_some());
            let (_, msg) = consumed.unwrap();
            assert_eq!(msg.payload, b"order-1");
        });
    }

    #[test]
    fn test_snapshot_round_trip() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();

        let mut qm1 = QueueManager::open(dir1.path(), None).unwrap();

        // Setup: declare queues with various policies, publish mix of messages
        qm1.declare_queue_with_retention(
            "fastq",
            pelicanq_core::retention::RetentionPolicy::new(Some(3600), Some(100), None, None),
        )
        .unwrap();
        qm1.declare_queue("normal").unwrap();

        qm1.publish(
            "fastq",
            Message::new(b"a".to_vec(), Default::default()),
        )
        .unwrap();
        qm1.publish(
            "fastq",
            Message::new(b"b".to_vec(), Default::default()),
        )
        .unwrap();
        qm1.publish(
            "normal",
            Message::new(b"x".to_vec(), Default::default()),
        )
        .unwrap();

        // Consume and nack one to create in-flight / re-queue state
        let (tag, _) = qm1.consume("fastq").unwrap().unwrap();
        qm1.nack("fastq", tag).unwrap();

        // Consume another into in-flight (will remain in-flight in snapshot)
        let (_tag2, _) = qm1.consume("fastq").unwrap().unwrap();

        // Snapshot export
        let data = build_snapshot(&qm1).unwrap();

        // Restore into fresh QueueManager
        let mut qm2 = QueueManager::open(dir2.path(), None).unwrap();
        install_snapshot(&mut qm2, &data).unwrap();

        // Verify state matches
        let mut queues1 = qm1.list_queues();
        let mut queues2 = qm2.list_queues();
        queues1.sort();
        queues2.sort();
        assert_eq!(queues1, queues2);

        assert_eq!(qm1.depth("fastq").unwrap(), qm2.depth("fastq").unwrap());
        assert_eq!(qm1.depth("normal").unwrap(), qm2.depth("normal").unwrap());
        assert_eq!(
            qm1.dead_letter_count("fastq").unwrap(),
            qm2.dead_letter_count("fastq").unwrap()
        );
    }
}
