use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::message::Message;
use crate::queue::QueueManager;
use crate::retention::RetentionPolicy;

/// A complete, serializable dump of a QueueManager's state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueManagerSnapshot {
    /// Per-queue state, keyed by queue name (sorted for reproducibility).
    pub queues: BTreeMap<String, QueueSnapshot>,
}

/// Per-queue contents in a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueSnapshot {
    pub policy: RetentionPolicy,
    /// Messages in the main (ready-to-consume) tree, in arbitrary order.
    pub messages: Vec<Message>,
    /// Messages in the scheduled tree, in arbitrary order.
    pub scheduled: Vec<Message>,
    /// Messages in the in-flight tree with their delivery tags preserved.
    /// Each tuple is (delivery_tag, message). Tags must be preserved to ensure
    /// that clients' ack/nack operations target the correct messages after restore.
    pub inflight: Vec<(u64, Message)>,
    /// Messages in the dead-letter queue.
    pub dead_letter: Vec<Message>,
}

impl QueueManager {
    /// Exports the current state of all queues into a `QueueManagerSnapshot`.
    ///
    /// This is a read-only operation: the manager is borrowed immutably.
    /// Dedup trees are NOT included in the snapshot because dedup keys are
    /// ephemeral and re-created on publish.
    /// In-flight messages are exported with their original delivery tags to
    /// ensure ack/nack semantics survive snapshot restore.
    pub fn export_snapshot(&self) -> Result<QueueManagerSnapshot, crate::error::PelicanError> {
        use crate::error::PelicanError;
        let mut queues = BTreeMap::new();

        for name in self.list_queues() {
            let policy: RetentionPolicy = self
                .retention_tree
                .get(&name)
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?
                .and_then(|b| bincode::deserialize(&b).ok())
                .unwrap_or_default();

            let mut messages = Vec::new();
            let tree = self
                .db
                .open_tree(&name)
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            for entry in tree.iter() {
                let (_, value) = entry
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
                let msg: Message = bincode::deserialize(&value)
                    .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
                messages.push(msg);
            }

            let mut scheduled = Vec::new();
            let sched_tree = self
                .db
                .open_tree(QueueManager::scheduled_tree_name(&name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            for entry in sched_tree.iter() {
                let (_, value) = entry
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
                let msg: Message = bincode::deserialize(&value)
                    .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
                scheduled.push(msg);
            }

            let mut inflight = Vec::new();
            let inflight_tree = self
                .db
                .open_tree(QueueManager::inflight_tree_name(&name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            for entry in inflight_tree.iter() {
                let (key, value) = entry
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
                let tag = u64::from_be_bytes(
                    key[..8]
                        .try_into()
                        .map_err(|e| PelicanError::Storage {
                            message: format!("invalid inflight key format: {e}"),
                        })?,
                );
                let msg: Message = bincode::deserialize(&value)
                    .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
                inflight.push((tag, msg));
            }

            let mut dead_letter = Vec::new();
            let dlq_tree = self
                .db
                .open_tree(QueueManager::dlq_tree_name(&name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            for entry in dlq_tree.iter() {
                let (_, value) = entry
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
                let msg: Message = bincode::deserialize(&value)
                    .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
                dead_letter.push(msg);
            }

            queues.insert(
                name,
                QueueSnapshot {
                    policy,
                    messages,
                    scheduled,
                    inflight,
                    dead_letter,
                },
            );
        }

        Ok(QueueManagerSnapshot { queues })
    }

    /// Restores state from a snapshot, writing all data into this manager's
    /// sled trees. The manager must be freshly opened (empty or discarded).
    /// In-flight messages are restored with their original delivery tags to
    /// preserve ack/nack semantics across snapshot restores.
    pub fn restore_from_snapshot(
        &mut self,
        snapshot: &QueueManagerSnapshot,
    ) -> Result<(), crate::error::PelicanError> {
        use crate::error::PelicanError;

        for (name, qs) in &snapshot.queues {
            // Declare the queue with its retention policy.
            self.meta_tree
                .insert(name.as_bytes(), &[])
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            let policy_bytes = bincode::serialize(&qs.policy)
                .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
            self.retention_tree
                .insert(name.as_bytes(), policy_bytes)
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;

            // Ensure all sub-trees exist.
            self.db
                .open_tree(name.as_bytes())
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            self.db
                .open_tree(Self::inflight_tree_name(name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            self.db
                .open_tree(Self::dlq_tree_name(name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            self.db
                .open_tree(Self::scheduled_tree_name(name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            self.db
                .open_tree(Self::dedup_tree_name(name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;

            // Write main messages.
            let tree = self
                .db
                .open_tree(name.as_bytes())
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            for msg in &qs.messages {
                let value = bincode::serialize(msg)
                    .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
                let id = self
                    .db
                    .generate_id()
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
                let key = Self::encode_key(msg.priority, id);
                tree.insert(key.as_slice(), value)
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            }

            // Write scheduled messages.
            let sched_tree = self
                .db
                .open_tree(Self::scheduled_tree_name(name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            for msg in &qs.scheduled {
                let value = bincode::serialize(msg)
                    .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
                let id = self
                    .db
                    .generate_id()
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
                let deliver_at = msg.deliver_at.unwrap_or_else(|| crate::message::Message::now_ms());
                let key = Self::encode_scheduled_key(deliver_at, id);
                sched_tree
                    .insert(key.as_slice(), value)
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            }

            // Write in-flight messages with preserved delivery tags.
            let inflight_tree = self
                .db
                .open_tree(Self::inflight_tree_name(name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            for (tag, msg) in &qs.inflight {
                let value = bincode::serialize(msg)
                    .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
                // Reuse the original delivery tag instead of generating a new one.
                // This ensures that clients' subsequent ack/nack calls with the same tag
                // will target the correct message after snapshot restore.
                inflight_tree
                    .insert(&tag.to_be_bytes(), value)
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            }

            // Write dead-letter messages.
            let dlq_tree = self
                .db
                .open_tree(Self::dlq_tree_name(name))
                .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            for msg in &qs.dead_letter {
                let value = bincode::serialize(msg)
                    .map_err(|e| PelicanError::Serialization { message: e.to_string() })?;
                let id = self
                    .db
                    .generate_id()
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
                let key = Self::encode_key(msg.priority, id);
                dlq_tree
                    .insert(key.as_slice(), value)
                    .map_err(|e| PelicanError::Storage { message: e.to_string() })?;
            }
        }

        // Recalculate byte tracking.
        self.current_bytes = self.recalculate_bytes()?;
        Ok(())
    }

}
