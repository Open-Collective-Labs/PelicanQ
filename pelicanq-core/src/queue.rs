use std::path::Path;

use sled::transaction::ConflictableTransactionError;
use sled::Transactional;

use crate::error::PelicanError;
use crate::message::DeliveryTag;
use crate::message::Message;
use crate::retention::RetentionPolicy;

/// Outcome of a `publish` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The message was stored. Contains the message's UUID.
    Stored(uuid::Uuid),
    /// The message was rejected as a duplicate (same `dedup_key` within the
    /// deduplication window). Not stored.
    Deduplicated,
}

/// An in-memory FIFO queue of messages.
pub struct Queue {
    name: String,
    messages: std::collections::VecDeque<Message>,
}

impl Queue {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            messages: std::collections::VecDeque::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Adds a message to the back of the queue.
    pub fn publish(&mut self, message: Message) {
        self.messages.push_back(message);
    }

    /// Removes and returns the message at the front of the queue, if any.
    pub fn consume(&mut self) -> Option<Message> {
        self.messages.pop_front()
    }

    /// Number of messages currently in the queue.
    pub fn depth(&self) -> usize {
        self.messages.len()
    }
}

const META_QUEUES_TREE: &str = "__meta__queues";
const META_RETENTION_TREE: &str = "__meta__retention";

/// Manages a named collection of queues backed by sled storage.
pub struct QueueManager {
    db: sled::Db,
    meta_tree: sled::Tree,
    retention_tree: sled::Tree,
    current_bytes: u64,
    max_bytes: Option<u64>,
}

impl QueueManager {
    /// Opens (or creates) a persistent queue store at the given data directory.
    ///
    /// `max_bytes` sets the approximate byte limit for engine-attributed storage.
    /// `None` means no limit.
    pub fn open(data_dir: &Path, max_bytes: Option<u64>) -> Result<Self, PelicanError> {
        let db = sled::open(data_dir)?;
        let meta_tree = db.open_tree(META_QUEUES_TREE)?;
        let retention_tree = db.open_tree(META_RETENTION_TREE)?;

        let mut mgr = Self {
            db,
            meta_tree,
            retention_tree,
            current_bytes: 0,
            max_bytes,
        };

        // Phase 1 simplification: tracks engine-attributed bytes, not raw OS disk usage.
        mgr.current_bytes = mgr.recalculate_bytes()?;
        mgr.recover_inflight()?;

        Ok(mgr)
    }

    /// Recalculates the total byte count by summing all queue, inflight, DLQ, and scheduled entries.
    fn recalculate_bytes(&self) -> Result<u64, PelicanError> {
        let mut total: u64 = 0;
        for name in self.list_queues() {
            let tree = self.db.open_tree(&name)?;
            for entry in tree.iter() {
                let (_, value) = entry?;
                total += value.len() as u64;
            }
            let inflight = self.db.open_tree(Self::inflight_tree_name(&name))?;
            for entry in inflight.iter() {
                let (_, value) = entry?;
                total += value.len() as u64;
            }
            let dlq = self.db.open_tree(Self::dlq_tree_name(&name))?;
            for entry in dlq.iter() {
                let (_, value) = entry?;
                total += value.len() as u64;
            }
            let scheduled = self.db.open_tree(Self::scheduled_tree_name(&name))?;
            for entry in scheduled.iter() {
                let (_, value) = entry?;
                total += value.len() as u64;
            }
            let dedup = self.db.open_tree(Self::dedup_tree_name(&name))?;
            for entry in dedup.iter() {
                let (_, value) = entry?;
                total += value.len() as u64;
            }
        }
        Ok(total)
    }

    /// Moves all in-flight messages back to their respective queues (crash recovery),
    /// preserving message priority in the re-inserted keys.
    fn recover_inflight(&mut self) -> Result<(), PelicanError> {
        let names: Vec<String> = self.list_queues();
        for name in names {
            let inflight = self.db.open_tree(format!("inflight:{}", &name))?;
            let tree = self.db.open_tree(&name)?;

            let entries: Vec<(sled::IVec, sled::IVec)> = inflight
                .iter()
                .filter_map(|e| e.ok())
                .collect();

            if entries.is_empty() {
                continue;
            }

            for (_key_bytes, value) in &entries {
                let msg: Message = bincode::deserialize(value)?;
                let new_id = self.db.generate_id()?;
                let new_key = Self::encode_key(msg.priority, new_id);
                tree.insert(new_key.as_slice(), value.as_ref())?;
            }

            for (key_bytes, _) in &entries {
                inflight.remove(key_bytes)?;
            }
        }
        Ok(())
    }

    fn inflight_tree_name(name: &str) -> String {
        format!("inflight:{}", name)
    }

    fn dlq_tree_name(name: &str) -> String {
        format!("dlq:{}", name)
    }

    fn scheduled_tree_name(name: &str) -> String {
        format!("scheduled:{}", name)
    }

    fn dedup_tree_name(name: &str) -> String {
        format!("dedup:{}", name)
    }

    /// Encodes a 16-byte scheduled key: 8 bytes big-endian `deliver_at_ms`
    /// (cast to u64 — always positive for future timestamps), followed by
    /// 8 bytes big-endian id.
    fn encode_scheduled_key(deliver_at_ms: i64, id: u64) -> [u8; 16] {
        let mut key = [0u8; 16];
        key[0..8].copy_from_slice(&(deliver_at_ms as u64).to_be_bytes());
        key[8..16].copy_from_slice(&id.to_be_bytes());
        key
    }

    /// Encodes a 9-byte storage key: 1 byte priority prefix so higher priority
    /// sorts first, followed by 8 bytes big-endian id.
    fn encode_key(priority: u8, id: u64) -> [u8; 9] {
        let mut key = [0u8; 9];
        key[0] = 9u8.saturating_sub(priority.min(9));
        key[1..9].copy_from_slice(&id.to_be_bytes());
        key
    }

    /// Creates a new empty queue. Errors if a queue with this name already exists.
    pub fn declare_queue(&mut self, name: &str) -> Result<(), PelicanError> {
        self.declare_queue_with_retention(name, RetentionPolicy::default())
    }

    /// Declares a queue with a specific retention policy.
    pub fn declare_queue_with_retention(
        &mut self,
        name: &str,
        policy: RetentionPolicy,
    ) -> Result<(), PelicanError> {
        if self.meta_tree.contains_key(name)? {
            return Err(PelicanError::QueueAlreadyExists(name.to_string()));
        }
        self.meta_tree.insert(name, &[])?;
        self.db.open_tree(name)?;
        self.db.open_tree(Self::inflight_tree_name(name))?;
        self.db.open_tree(Self::dlq_tree_name(name))?;
        self.db.open_tree(Self::scheduled_tree_name(name))?;
        self.db.open_tree(Self::dedup_tree_name(name))?;
        let policy_bytes = bincode::serialize(&policy)?;
        self.retention_tree.insert(name, policy_bytes)?;
        Ok(())
    }

    /// Publishes a message to the named queue. If `message.deliver_at` is set
    /// to a future timestamp, the message is stored in the scheduled tree and
    /// won't be visible to `consume` until `promote_scheduled` moves it to the
    /// main tree. If the queue has deduplication enabled and `message.dedup_key`
    /// matches a key seen within the configured window, returns
    /// `PublishOutcome::Deduplicated` without storing anything.
    /// Errors if the queue doesn't exist or if the storage limit would be exceeded.
    pub fn publish(
        &mut self,
        queue: &str,
        message: Message,
    ) -> Result<PublishOutcome, PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }

        // Deduplication check
        let policy: RetentionPolicy = self
            .retention_tree
            .get(queue)?
            .and_then(|b| bincode::deserialize(&b).ok())
            .unwrap_or_default();

        if let Some(window_secs) = policy.dedup_window_secs {
            if window_secs > 0 {
                if let Some(ref dedup_key) = message.dedup_key {
                    let dedup_tree = self.db.open_tree(Self::dedup_tree_name(queue))?;
                    if let Some(recorded) = dedup_tree.get(dedup_key.as_bytes())? {
                        let recorded_ms = i64::from_be_bytes(
                            recorded[..8]
                                .try_into()
                                .map_err(|e: std::array::TryFromSliceError| {
                                    PelicanError::Storage(sled::Error::Io(
                                        std::io::Error::new(
                                            std::io::ErrorKind::InvalidData,
                                            e,
                                        ),
                                    ))
                                })?,
                        );
                        let window_ms = (window_secs as i64) * 1000;
                        if recorded_ms > Message::now_ms() - window_ms {
                            return Ok(PublishOutcome::Deduplicated);
                        }
                    }
                }
            }
        }

        let value = bincode::serialize(&message)?;
        let msg_len = value.len() as u64;

        if let Some(max) = self.max_bytes {
            if self.current_bytes + msg_len > max {
                let used_pct = if max > 0 {
                    (self.current_bytes * 100 / max) as u8
                } else {
                    100
                };
                return Err(PelicanError::StorageLimitExceeded {
                    used_pct,
                    limit_pct: 100,
                });
            }
        }

        let stored = message.id;
        let id = self.db.generate_id()?;

        if let Some(deliver_at) = message.deliver_at {
            if deliver_at > Message::now_ms() {
                let scheduled = self.db.open_tree(Self::scheduled_tree_name(queue))?;
                let key = Self::encode_scheduled_key(deliver_at, id);
                scheduled.insert(key.as_slice(), value.as_slice())?;
                self.current_bytes += msg_len;
                self.record_dedup_key(queue, &message, &policy)?;
                return Ok(PublishOutcome::Stored(stored));
            }
        }

        let tree = self.db.open_tree(queue)?;
        let key = Self::encode_key(message.priority, id);
        tree.insert(key.as_slice(), value.as_slice())?;

        self.current_bytes += msg_len;
        self.record_dedup_key(queue, &message, &policy)?;
        Ok(PublishOutcome::Stored(stored))
    }

    /// Records the message's `dedup_key` in the dedup index, if deduplication
    /// is enabled and the message has a key.
    fn record_dedup_key(
        &self,
        queue: &str,
        message: &Message,
        policy: &RetentionPolicy,
    ) -> Result<(), PelicanError> {
        if policy.dedup_window_secs.is_some() {
            if let Some(ref dedup_key) = message.dedup_key {
                let dedup_tree = self.db.open_tree(Self::dedup_tree_name(queue))?;
                dedup_tree.insert(
                    dedup_key.as_bytes(),
                    &Message::now_ms().to_be_bytes(),
                )?;
            }
        }
        Ok(())
    }

    /// Consumes the next message (highest priority, oldest first), moving it to the
    /// in-flight store. Returns the message along with a delivery tag needed to
    /// ack/nack it. Returns Ok(None) if the queue is empty.
    pub fn consume(
        &mut self,
        queue: &str,
    ) -> Result<Option<(DeliveryTag, Message)>, PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }

        let tree = self.db.open_tree(queue)?;
        let inflight = self.db.open_tree(Self::inflight_tree_name(queue))?;

        loop {
            let min_key = tree
                .iter()
                .next()
                .and_then(|r| r.ok())
                .map(|(k, _)| k.to_vec());

            let key = match min_key {
                Some(k) => k,
                None => return Ok(None),
            };

            let id_bytes: [u8; 8] = key[1..9]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| {
                    PelicanError::Storage(sled::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e,
                    )))
                })?;
            let id = u64::from_be_bytes(id_bytes);

            let result: sled::transaction::TransactionResult<Vec<u8>, ()> =
                (&tree, &inflight).transaction(|(tx_tree, tx_inflight)| {
                    match tx_tree.remove(key.as_slice())? {
                        Some(value) => {
                            let val_bytes = value.to_vec();
                            // Inflight key is just the 8-byte id (no priority prefix)
                            tx_inflight.insert(&id_bytes, value)?;
                            Ok(val_bytes)
                        }
                        None => Err(ConflictableTransactionError::Abort(())),
                    }
                });

            match result {
                Ok(val_bytes) => {
                    let msg: Message = bincode::deserialize(&val_bytes)?;
                    return Ok(Some((DeliveryTag(id), msg)));
                }
                Err(sled::transaction::TransactionError::Abort(())) => {
                    // Key was taken between iter and transaction; retry with next key
                    continue;
                }
                Err(sled::transaction::TransactionError::Storage(e)) => {
                    return Err(PelicanError::Storage(e));
                }
            }
        }
    }

    /// Permanently removes a message from the in-flight store.
    pub fn ack(&mut self, queue: &str, delivery_tag: DeliveryTag) -> Result<(), PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }

        let inflight = self.db.open_tree(Self::inflight_tree_name(queue))?;
        let key = delivery_tag.0.to_be_bytes();

        match inflight.remove(key.as_slice())? {
            Some(value) => {
                self.current_bytes = self.current_bytes.saturating_sub(value.len() as u64);
                Ok(())
            }
            None => Err(PelicanError::InvalidDeliveryTag(delivery_tag)),
        }
    }

    /// Moves a message from the in-flight store back to the main queue,
    /// incrementing its delivery attempt counter. If the queue's retention policy
    /// specifies `max_delivery_attempts` and the message has reached that threshold,
    /// it is routed to the dead-letter queue (DLQ) instead of being requeued.
    /// The re-inserted key is computed from the message's priority so that higher
    /// priority messages are delivered first.
    pub fn nack(
        &mut self,
        queue: &str,
        delivery_tag: DeliveryTag,
    ) -> Result<(), PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }

        let policy: RetentionPolicy = self
            .retention_tree
            .get(queue)?
            .and_then(|b| bincode::deserialize(&b).ok())
            .unwrap_or_default();

        let tree = self.db.open_tree(queue)?;
        let inflight = self.db.open_tree(Self::inflight_tree_name(queue))?;
        let dlq_tree = self.db.open_tree(Self::dlq_tree_name(queue))?;
        let old_key = delivery_tag.0.to_be_bytes();
        let new_id = self.db.generate_id()?;

        let max_attempts = policy.max_delivery_attempts;

        let result: sled::transaction::TransactionResult<(), ()> =
            (&inflight, &tree, &dlq_tree).transaction(
                |(tx_inflight, tx_tree, tx_dlq)| {
                    let value = tx_inflight
                        .remove(old_key.as_slice())?
                        .ok_or(ConflictableTransactionError::Abort(()))?;

                    let mut msg: Message = bincode::deserialize(&value)
                        .map_err(|_| ConflictableTransactionError::Abort(()))?;

                    msg.delivery_attempts += 1;

                    let new_value = bincode::serialize(&msg)
                        .map_err(|_| ConflictableTransactionError::Abort(()))?;

                    let new_key = Self::encode_key(msg.priority, new_id);

                    if let Some(max) = max_attempts {
                        if max > 0 && msg.delivery_attempts >= max {
                            tx_dlq.insert(new_key.as_slice(), new_value)?;
                            return Ok(());
                        }
                    }

                    tx_tree.insert(new_key.as_slice(), new_value)?;
                    Ok(())
                },
            );

        match result {
            Ok(()) => Ok(()),
            Err(sled::transaction::TransactionError::Abort(())) => {
                Err(PelicanError::InvalidDeliveryTag(delivery_tag))
            }
            Err(sled::transaction::TransactionError::Storage(e)) => {
                Err(PelicanError::Storage(e))
            }
        }
    }

    /// Returns the number of messages currently in the dead-letter queue for the
    /// named queue. Errors if the queue doesn't exist.
    pub fn dead_letter_count(&self, queue: &str) -> Result<usize, PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }
        let dlq = self.db.open_tree(Self::dlq_tree_name(queue))?;
        Ok(dlq.len())
    }

    /// Moves all scheduled messages whose `deliver_at` has passed (<= now) from
    /// the scheduled tree into the main queue tree, using the message's priority
    /// for key encoding. Returns the number of messages promoted.
    /// This does NOT run automatically — callers invoke it periodically.
    pub fn promote_scheduled(&mut self, queue: &str) -> Result<usize, PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }

        let scheduled = self.db.open_tree(Self::scheduled_tree_name(queue))?;
        let tree = self.db.open_tree(queue)?;
        let now_ms = Message::now_ms();

        let mut to_promote: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        for entry in scheduled.iter() {
            let (key, value) = entry?;
            let deliver_at_u64 = u64::from_be_bytes(
                key[..8]
                    .try_into()
                    .map_err(|e: std::array::TryFromSliceError| {
                        PelicanError::Storage(sled::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e,
                        )))
                    })?,
            );
            let deliver_at_ms = deliver_at_u64 as i64;
            if deliver_at_ms > now_ms {
                break;
            }
            to_promote.push((key.to_vec(), value.to_vec()));
        }

        let mut promoted = 0usize;
        for (scheduled_key, value) in &to_promote {
            let msg: Message = bincode::deserialize(value)?;
            let new_id = self.db.generate_id()?;
            let main_key = Self::encode_key(msg.priority, new_id);

            let result: sled::transaction::TransactionResult<(), ()> =
                (&scheduled, &tree).transaction(|(tx_scheduled, tx_tree)| {
                    tx_scheduled.remove(scheduled_key.as_slice())?;
                    tx_tree.insert(main_key.as_slice(), value.as_slice())?;
                    Ok(())
                });

            match result {
                Ok(()) => promoted += 1,
                Err(sled::transaction::TransactionError::Abort(())) => {
                    // Should not happen with our closure
                }
                Err(sled::transaction::TransactionError::Storage(e)) => {
                    return Err(PelicanError::Storage(e));
                }
            }
        }

        Ok(promoted)
    }

    /// Number of messages waiting in the scheduled (not-yet-due) store for this queue.
    pub fn scheduled_depth(&self, queue: &str) -> Result<usize, PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }
        let scheduled = self.db.open_tree(Self::scheduled_tree_name(queue))?;
        Ok(scheduled.len())
    }

    /// Returns the current depth of the named queue (main tree only).
    /// Errors if the queue doesn't exist.
    pub fn depth(&self, queue: &str) -> Result<usize, PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }
        let tree = self.db.open_tree(queue)?;
        Ok(tree.len())
    }

    /// Lists all declared queue names.
    pub fn list_queues(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .meta_tree
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .filter_map(|k| String::from_utf8(k.to_vec()).ok())
            .collect();
        names.sort();
        names
    }

    /// Removes messages exceeding the queue's retention policy (max_age_secs, max_messages).
    /// Returns the number of messages removed.
    pub fn apply_retention(&mut self, queue: &str) -> Result<usize, PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }

        let policy: RetentionPolicy = self
            .retention_tree
            .get(queue)?
            .and_then(|b| bincode::deserialize(&b).ok())
            .unwrap_or_default();

        if policy.max_age_secs.is_none() && policy.max_messages.is_none() {
            return Ok(0);
        }

        let tree = self.db.open_tree(queue)?;
        let mut removed = 0usize;

        if let Some(max_age_secs) = policy.max_age_secs {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let max_age_ms = (max_age_secs as i64) * 1000;
            let mut to_remove = vec![];

            for entry in tree.iter() {
                let (key, value) = entry?;
                let msg: Message = bincode::deserialize(&value)?;
                let age_ms = now_ms - msg.timestamp;
                if age_ms >= max_age_ms {
                    to_remove.push(key.to_vec());
                }
                // No early break — priority-based keys are not strictly chronological.
            }

            for key in &to_remove {
                tree.remove(key)?;
            }
            removed += to_remove.len();
        }

        if let Some(max_msgs) = policy.max_messages {
            let current = tree.len();
            if current > max_msgs as usize {
                let excess = current - max_msgs as usize;
                let mut count = 0usize;
                let mut to_remove = vec![];

                for entry in tree.iter() {
                    if count >= excess {
                        break;
                    }
                    let (key, _) = entry?;
                    to_remove.push(key.to_vec());
                    count += 1;
                }

                for key in &to_remove {
                    tree.remove(key)?;
                }
                removed += to_remove.len();
            }
        }

        if removed > 0 {
            self.current_bytes = self.recalculate_bytes()?;
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_manager<F>(f: F)
    where
        F: FnOnce(QueueManager),
    {
        let dir = tempfile::tempdir().unwrap();
        let mgr = QueueManager::open(dir.path(), None).unwrap();
        f(mgr);
    }

    // --- Step 1 & 2 acceptance tests (adapted) ---

    #[test]
    fn test_declare_queue_then_depth_zero() {
        with_manager(|mut mgr| {
            mgr.declare_queue("test").unwrap();
            assert_eq!(mgr.depth("test").unwrap(), 0);
        });
    }

    #[test]
    fn test_declare_queue_twice_errors() {
        with_manager(|mut mgr| {
            mgr.declare_queue("test").unwrap();
            let err = mgr.declare_queue("test").unwrap_err();
            assert!(matches!(err, PelicanError::QueueAlreadyExists(_)));
        });
    }

    #[test]
    fn test_publish_consume_fifo() {
        with_manager(|mut mgr| {
            mgr.declare_queue("q").unwrap();
            mgr.publish("q", Message::new(b"first".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            mgr.publish("q", Message::new(b"second".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            mgr.publish("q", Message::new(b"third".to_vec(), std::collections::HashMap::new()))
                .unwrap();

            let (_, m1) = mgr.consume("q").unwrap().unwrap();
            let (_, m2) = mgr.consume("q").unwrap().unwrap();
            let (_, m3) = mgr.consume("q").unwrap().unwrap();
            assert_eq!(m1.payload, b"first");
            assert_eq!(m2.payload, b"second");
            assert_eq!(m3.payload, b"third");
        });
    }

    #[test]
    fn test_consume_empty_queue_returns_none() {
        with_manager(|mut mgr| {
            mgr.declare_queue("empty").unwrap();
            assert!(mgr.consume("empty").unwrap().is_none());
        });
    }

    #[test]
    fn test_operations_on_nonexistent_queue_error() {
        with_manager(|mut mgr| {
            let err1 =
                mgr.publish("nonexistent", Message::new(b"x".to_vec(), std::collections::HashMap::new()));
            assert!(matches!(err1, Err(PelicanError::QueueNotFound(_))));

            let err2 = mgr.consume("nonexistent");
            assert!(matches!(err2, Err(PelicanError::QueueNotFound(_))));

            let err3 = mgr.depth("nonexistent");
            assert!(matches!(err3, Err(PelicanError::QueueNotFound(_))));
        });
    }

    #[test]
    fn test_list_queues() {
        with_manager(|mut mgr| {
            mgr.declare_queue("alpha").unwrap();
            mgr.declare_queue("beta").unwrap();
            mgr.declare_queue("gamma").unwrap();

            let names = mgr.list_queues();
            assert_eq!(names, vec!["alpha", "beta", "gamma"]);
        });
    }

    #[test]
    fn test_depth_reflects_message_count() {
        with_manager(|mut mgr| {
            mgr.declare_queue("q").unwrap();
            assert_eq!(mgr.depth("q").unwrap(), 0);

            mgr.publish("q", Message::new(b"a".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            assert_eq!(mgr.depth("q").unwrap(), 1);

            mgr.publish("q", Message::new(b"b".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            assert_eq!(mgr.depth("q").unwrap(), 2);

            let (_, _) = mgr.consume("q").unwrap().unwrap();
            assert_eq!(mgr.depth("q").unwrap(), 1);
        });
    }

    #[test]
    fn test_persistence_across_restart() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut mgr = QueueManager::open(dir.path(), None).unwrap();
            mgr.declare_queue("persist").unwrap();
            mgr.publish("persist", Message::new(b"hello".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            mgr.publish("persist", Message::new(b"world".to_vec(), std::collections::HashMap::new()))
                .unwrap();
        }

        let mut mgr = QueueManager::open(dir.path(), None).unwrap();
        let (_, m1) = mgr.consume("persist").unwrap().unwrap();
        let (_, m2) = mgr.consume("persist").unwrap().unwrap();
        assert_eq!(m1.payload, b"hello");
        assert_eq!(m2.payload, b"world");
    }

    #[test]
    fn test_declare_queue_persists() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut mgr = QueueManager::open(dir.path(), None).unwrap();
            mgr.declare_queue("perm").unwrap();
        }

        let mut mgr = QueueManager::open(dir.path(), None).unwrap();
        let err = mgr.declare_queue("perm").unwrap_err();
        assert!(matches!(err, PelicanError::QueueAlreadyExists(_)));
    }

    #[test]
    fn test_list_queues_persists() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut mgr = QueueManager::open(dir.path(), None).unwrap();
            mgr.declare_queue("alpha").unwrap();
            mgr.declare_queue("beta").unwrap();
        }

        let mgr = QueueManager::open(dir.path(), None).unwrap();
        let names = mgr.list_queues();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    // --- Step 3 acceptance tests (ack/nack, crash recovery) ---

    #[test]
    fn test_basic_ack() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut mgr = QueueManager::open(dir.path(), None).unwrap();
            mgr.declare_queue("q").unwrap();
            mgr.publish("q", Message::new(b"data".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            let (tag, _msg) = mgr.consume("q").unwrap().unwrap();
            mgr.ack("q", tag).unwrap();
            assert_eq!(mgr.depth("q").unwrap(), 0);
        }

        // Re-open — message should NOT be redelivered
        let mut mgr = QueueManager::open(dir.path(), None).unwrap();
        assert_eq!(mgr.depth("q").unwrap(), 0);
        assert!(mgr.consume("q").unwrap().is_none());
    }

    #[test]
    fn test_basic_nack() {
        with_manager(|mut mgr| {
            mgr.declare_queue("q").unwrap();
            mgr.publish("q", Message::new(b"A".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            mgr.publish("q", Message::new(b"B".to_vec(), std::collections::HashMap::new()))
                .unwrap();

            let (tag_a, msg_a) = mgr.consume("q").unwrap().unwrap();
            assert_eq!(msg_a.payload, b"A");

            mgr.nack("q", tag_a).unwrap();

            // Next consume returns B (A went to the back)
            let (_, msg_b) = mgr.consume("q").unwrap().unwrap();
            assert_eq!(msg_b.payload, b"B");

            // Then A again
            let (_, msg_a2) = mgr.consume("q").unwrap().unwrap();
            assert_eq!(msg_a2.payload, b"A");
        });
    }

    #[test]
    fn test_crash_recovery_inflight_requeued() {
        let dir = tempfile::tempdir().unwrap();

        // Publish, consume (moves to inflight), drop without ack (simulate crash)
        {
            let mut mgr = QueueManager::open(dir.path(), None).unwrap();
            mgr.declare_queue("q").unwrap();
            mgr.publish("q", Message::new(b"crash-test".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            let (_tag, _) = mgr.consume("q").unwrap().unwrap();
            // drop WITHOUT ack — tag is forgotten
        }

        // Re-open — in-flight message should be requeued
        let mut mgr = QueueManager::open(dir.path(), None).unwrap();
        assert_eq!(mgr.depth("q").unwrap(), 1);
        let (_, msg) = mgr.consume("q").unwrap().unwrap();
        assert_eq!(msg.payload, b"crash-test");
    }

    #[test]
    fn test_invalid_delivery_tag() {
        with_manager(|mut mgr| {
            mgr.declare_queue("q").unwrap();

            let err = mgr.ack("q", DeliveryTag(999));
            assert!(matches!(err, Err(PelicanError::InvalidDeliveryTag(_))));

            let err = mgr.nack("q", DeliveryTag(999));
            assert!(matches!(err, Err(PelicanError::InvalidDeliveryTag(_))));
        });
    }

    // --- Step 4 acceptance tests (retention, storage limits) ---

    #[test]
    fn test_max_messages_retention() {
        with_manager(|mut mgr| {
            mgr.declare_queue_with_retention(
                "q",
                RetentionPolicy::new(None, Some(2), None, None),
            )
            .unwrap();

            mgr.publish("q", Message::new(b"oldest".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            mgr.publish("q", Message::new(b"middle".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            mgr.publish("q", Message::new(b"newest".to_vec(), std::collections::HashMap::new()))
                .unwrap();

            assert_eq!(mgr.depth("q").unwrap(), 3);

            let removed = mgr.apply_retention("q").unwrap();
            assert_eq!(removed, 1);
            assert_eq!(mgr.depth("q").unwrap(), 2);

            // Oldest message should be gone
            let (_, msg) = mgr.consume("q").unwrap().unwrap();
            assert_eq!(msg.payload, b"middle");
        });
    }

    #[test]
    fn test_max_age_retention() {
        with_manager(|mut mgr| {
            mgr.declare_queue_with_retention(
                "q",
                RetentionPolicy::new(Some(0), None, None, None),
            )
            .unwrap();

            mgr.publish("q", Message::new(b"too-old".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            assert_eq!(mgr.depth("q").unwrap(), 1);

            let removed = mgr.apply_retention("q").unwrap();
            assert_eq!(removed, 1);
            assert_eq!(mgr.depth("q").unwrap(), 0);
        });
    }

    #[test]
    fn test_byte_limit_reject() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = QueueManager::open(dir.path(), Some(100)).unwrap();
        mgr.declare_queue("q").unwrap();

        // Publish until we hit the limit
        let mut count = 0;
        loop {
            let payload = vec![b'x'; 32];
            match mgr.publish("q", Message::new(payload, std::collections::HashMap::new())) {
                Ok(PublishOutcome::Stored(_)) => count += 1,
                Err(PelicanError::StorageLimitExceeded { .. }) => break,
                Ok(PublishOutcome::Deduplicated) => panic!("unexpected dedup"),
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(mgr.depth("q").unwrap(), count);
    }

    #[test]
    fn test_default_policy_no_limits() {
        with_manager(|mut mgr| {
            mgr.declare_queue("q").unwrap();
            for i in 0..100 {
                mgr.publish("q", Message::new(format!("msg-{}", i).into_bytes(), std::collections::HashMap::new()))
                    .unwrap();
            }
            let removed = mgr.apply_retention("q").unwrap();
            assert_eq!(removed, 0);
            assert_eq!(mgr.depth("q").unwrap(), 100);
        });
    }

    // --- Phase 2, Step 1 acceptance tests (delivery attempts & DLQ) ---

    #[test]
    fn test_delivery_attempt_increments_on_nack() {
        with_manager(|mut mgr| {
            mgr.declare_queue_with_retention(
                "q",
                RetentionPolicy::new(None, None, Some(5), None),
            )
            .unwrap();

            mgr.publish("q", Message::new(b"msg".to_vec(), std::collections::HashMap::new()))
                .unwrap();

            let (tag, msg) = mgr.consume("q").unwrap().unwrap();
            assert_eq!(msg.delivery_attempts, 0);

            mgr.nack("q", tag).unwrap();

            let (_, msg2) = mgr.consume("q").unwrap().unwrap();
            assert_eq!(msg2.delivery_attempts, 1);
        });
    }

    #[test]
    fn test_nack_exceeds_max_delivery_attempts_routes_to_dlq() {
        with_manager(|mut mgr| {
            mgr.declare_queue_with_retention(
                "dlq-test",
                RetentionPolicy::new(None, None, Some(2), None),
            )
            .unwrap();

            mgr.publish("dlq-test", Message::new(b"will-die".to_vec(), std::collections::HashMap::new()))
                .unwrap();

            // First consume + nack → delivery_attempts becomes 1, requeued
            let (tag1, msg1) = mgr.consume("dlq-test").unwrap().unwrap();
            assert_eq!(msg1.payload, b"will-die");
            mgr.nack("dlq-test", tag1).unwrap();
            assert_eq!(mgr.dead_letter_count("dlq-test").unwrap(), 0);

            // Second consume + nack → delivery_attempts becomes 2, dead-lettered
            let (tag2, msg2) = mgr.consume("dlq-test").unwrap().unwrap();
            assert_eq!(msg2.payload, b"will-die");
            mgr.nack("dlq-test", tag2).unwrap();

            // Queue should now be empty
            assert_eq!(mgr.depth("dlq-test").unwrap(), 0);
            assert!(mgr.consume("dlq-test").unwrap().is_none());

            // DLQ should have the message
            assert_eq!(mgr.dead_letter_count("dlq-test").unwrap(), 1);
        });
    }

    #[test]
    fn test_nack_without_max_attempts_requeues_indefinitely() {
        with_manager(|mut mgr| {
            mgr.declare_queue("q").unwrap();

            mgr.publish("q", Message::new(b"forever".to_vec(), std::collections::HashMap::new()))
                .unwrap();

            for _ in 0..5 {
                let (tag, msg) = mgr.consume("q").unwrap().unwrap();
                assert_eq!(msg.payload, b"forever");
                mgr.nack("q", tag).unwrap();
            }

            // Message should still be in the queue after 5 nacks
            assert_eq!(mgr.depth("q").unwrap(), 1);
            assert_eq!(mgr.dead_letter_count("q").unwrap(), 0);
        });
    }

    #[test]
    fn test_dead_letter_count_on_nonexistent_queue() {
        with_manager(|mgr| {
            let err = mgr.dead_letter_count("nope");
            assert!(matches!(err, Err(PelicanError::QueueNotFound(_))));
        });
    }

    #[test]
    fn test_dlq_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut mgr = QueueManager::open(dir.path(), None).unwrap();
            mgr.declare_queue_with_retention(
                "q",
                RetentionPolicy::new(None, None, Some(1), None),
            )
            .unwrap();

            mgr.publish("q", Message::new(b"persist-dlq".to_vec(), std::collections::HashMap::new()))
                .unwrap();

            let (tag, _) = mgr.consume("q").unwrap().unwrap();
            mgr.nack("q", tag).unwrap();

            assert_eq!(mgr.dead_letter_count("q").unwrap(), 1);
        }

        // Re-open — DLQ should still have the message
        let mgr = QueueManager::open(dir.path(), None).unwrap();
        assert_eq!(mgr.dead_letter_count("q").unwrap(), 1);
    }

    #[test]
    fn test_delivery_attempts_binary_backward_compat() {
        // Verify that a freshly created message serializes/deserializes
        // with delivery_attempts = 0.
        let msg = Message::new(b"compat".to_vec(), std::collections::HashMap::new());
        let bytes = bincode::serialize(&msg).unwrap();
        let deserialized: Message = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.delivery_attempts, 0);
        assert_eq!(deserialized.payload, b"compat");
    }

    // --- Phase 2, Step 3 acceptance tests (priority queues) ---

    #[test]
    fn test_priority_delivery_order() {
        with_manager(|mut mgr| {
            mgr.declare_queue("prio").unwrap();

            // Publish A (priority 0), B (priority 5), C (priority 0)
            mgr.publish(
                "prio",
                Message::new(b"A".to_vec(), std::collections::HashMap::new()),
            )
            .unwrap();
            mgr.publish(
                "prio",
                Message::new(b"B".to_vec(), std::collections::HashMap::new())
                    .with_priority(5),
            )
            .unwrap();
            mgr.publish(
                "prio",
                Message::new(b"C".to_vec(), std::collections::HashMap::new()),
            )
            .unwrap();

            // Consume 3 times → order is B, A, C
            let (_, m1) = mgr.consume("prio").unwrap().unwrap();
            let (_, m2) = mgr.consume("prio").unwrap().unwrap();
            let (_, m3) = mgr.consume("prio").unwrap().unwrap();

            assert_eq!(m1.payload, b"B");
            assert_eq!(m2.payload, b"A");
            assert_eq!(m3.payload, b"C");
        });
    }

    #[test]
    fn test_priority_clamped_to_9() {
        with_manager(|mut mgr| {
            mgr.declare_queue("clamp").unwrap();

            mgr.publish(
                "clamp",
                Message::new(b"high".to_vec(), std::collections::HashMap::new())
                    .with_priority(15),
            )
            .unwrap();
            mgr.publish(
                "clamp",
                Message::new(b"low".to_vec(), std::collections::HashMap::new()),
            )
            .unwrap();

            let (_, m1) = mgr.consume("clamp").unwrap().unwrap();
            assert_eq!(m1.payload, b"high");
        });
    }

    #[test]
    fn test_priority_fifo_within_same_priority() {
        with_manager(|mut mgr| {
            mgr.declare_queue("fifo-prio").unwrap();

            mgr.publish(
                "fifo-prio",
                Message::new(b"first".to_vec(), std::collections::HashMap::new())
                    .with_priority(3),
            )
            .unwrap();
            mgr.publish(
                "fifo-prio",
                Message::new(b"second".to_vec(), std::collections::HashMap::new())
                    .with_priority(3),
            )
            .unwrap();

            let (_, m1) = mgr.consume("fifo-prio").unwrap().unwrap();
            let (_, m2) = mgr.consume("fifo-prio").unwrap().unwrap();

            assert_eq!(m1.payload, b"first");
            assert_eq!(m2.payload, b"second");
        });
    }

    #[test]
    fn test_delivery_tag_ack_round_trip_with_priority() {
        with_manager(|mut mgr| {
            mgr.declare_queue("prio-ack").unwrap();

            mgr.publish(
                "prio-ack",
                Message::new(b"prio-msg".to_vec(), std::collections::HashMap::new())
                    .with_priority(7),
            )
            .unwrap();

            let (tag, msg) = mgr.consume("prio-ack").unwrap().unwrap();
            assert_eq!(msg.payload, b"prio-msg");

            mgr.ack("prio-ack", tag).unwrap();
            assert_eq!(mgr.depth("prio-ack").unwrap(), 0);
        });
    }

    #[test]
    fn test_nack_preserves_priority() {
        with_manager(|mut mgr| {
            mgr.declare_queue("prio-nack").unwrap();

            mgr.publish(
                "prio-nack",
                Message::new(b"urgent".to_vec(), std::collections::HashMap::new())
                    .with_priority(9),
            )
            .unwrap();
            mgr.publish(
                "prio-nack",
                Message::new(b"normal".to_vec(), std::collections::HashMap::new()),
            )
            .unwrap();

            let (tag, m) = mgr.consume("prio-nack").unwrap().unwrap();
            assert_eq!(m.payload, b"urgent");
            mgr.nack("prio-nack", tag).unwrap();

            let (_, m_again) = mgr.consume("prio-nack").unwrap().unwrap();
            assert_eq!(m_again.payload, b"urgent");
        });
    }

    #[test]
    fn test_priority_does_not_break_basic_fifo() {
        with_manager(|mut mgr| {
            mgr.declare_queue("fifo").unwrap();

            mgr.publish("fifo", Message::new(b"1".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            mgr.publish("fifo", Message::new(b"2".to_vec(), std::collections::HashMap::new()))
                .unwrap();
            mgr.publish("fifo", Message::new(b"3".to_vec(), std::collections::HashMap::new()))
                .unwrap();

            let (_, m1) = mgr.consume("fifo").unwrap().unwrap();
            let (_, m2) = mgr.consume("fifo").unwrap().unwrap();
            let (_, m3) = mgr.consume("fifo").unwrap().unwrap();
            assert_eq!(m1.payload, b"1");
            assert_eq!(m2.payload, b"2");
            assert_eq!(m3.payload, b"3");
        });
    }

    // --- Phase 2, Step 4 acceptance tests (delayed / scheduled messages) ---

    #[test]
    fn test_scheduled_message_not_visible_to_consume() {
        with_manager(|mut mgr| {
            mgr.declare_queue("sched").unwrap();

            let future = Message::now_ms() + 3_600_000; // 1 hour
            mgr.publish(
                "sched",
                Message::new(b"future".to_vec(), std::collections::HashMap::new())
                    .with_deliver_at(future),
            )
            .unwrap();

            assert_eq!(mgr.depth("sched").unwrap(), 0);
            assert_eq!(mgr.scheduled_depth("sched").unwrap(), 1);
            assert!(mgr.consume("sched").unwrap().is_none());
        });
    }

    #[test]
    fn test_scheduled_not_yet_due_not_promoted() {
        with_manager(|mut mgr| {
            mgr.declare_queue("sched2").unwrap();

            let future = Message::now_ms() + 3_600_000;
            mgr.publish(
                "sched2",
                Message::new(b"future".to_vec(), std::collections::HashMap::new())
                    .with_deliver_at(future),
            )
            .unwrap();

            let promoted = mgr.promote_scheduled("sched2").unwrap();
            assert_eq!(promoted, 0);
            assert_eq!(mgr.depth("sched2").unwrap(), 0);
            assert_eq!(mgr.scheduled_depth("sched2").unwrap(), 1);
        });
    }

    #[test]
    fn test_already_due_goes_to_main_tree() {
        with_manager(|mut mgr| {
            mgr.declare_queue("due").unwrap();

            let past = Message::now_ms() - 100;
            mgr.publish(
                "due",
                Message::new(b"past-due".to_vec(), std::collections::HashMap::new())
                    .with_deliver_at(past),
            )
            .unwrap();

            assert_eq!(mgr.depth("due").unwrap(), 1);
            assert_eq!(mgr.scheduled_depth("due").unwrap(), 0);

            let (_, msg) = mgr.consume("due").unwrap().unwrap();
            assert_eq!(msg.payload, b"past-due");
        });
    }

    #[test]
    fn test_deliver_at_none_behaves_normally() {
        with_manager(|mut mgr| {
            mgr.declare_queue("normal").unwrap();

            mgr.publish(
                "normal",
                Message::new(b"immediate".to_vec(), std::collections::HashMap::new()),
            )
            .unwrap();

            assert_eq!(mgr.depth("normal").unwrap(), 1);
            assert_eq!(mgr.scheduled_depth("normal").unwrap(), 0);

            let (_, msg) = mgr.consume("normal").unwrap().unwrap();
            assert_eq!(msg.payload, b"immediate");
        });
    }

    #[test]
    fn test_promote_scheduled_after_wait() {
        with_manager(|mut mgr| {
            mgr.declare_queue("delay").unwrap();

            // Schedule a message 50ms in the future
            let future = Message::now_ms() + 50;
            mgr.publish(
                "delay",
                Message::new(b"delayed".to_vec(), std::collections::HashMap::new())
                    .with_deliver_at(future),
            )
            .unwrap();

            assert_eq!(mgr.depth("delay").unwrap(), 0);
            assert_eq!(mgr.scheduled_depth("delay").unwrap(), 1);

            // Wait long enough for it to become due
            std::thread::sleep(std::time::Duration::from_millis(60));

            let promoted = mgr.promote_scheduled("delay").unwrap();
            assert_eq!(promoted, 1);
            assert_eq!(mgr.depth("delay").unwrap(), 1);
            assert_eq!(mgr.scheduled_depth("delay").unwrap(), 0);

            let (_, msg) = mgr.consume("delay").unwrap().unwrap();
            assert_eq!(msg.payload, b"delayed");
        });
    }

    #[test]
    fn test_scheduled_depth_on_nonexistent_queue() {
        with_manager(|mgr| {
            let err = mgr.scheduled_depth("nope");
            assert!(matches!(err, Err(PelicanError::QueueNotFound(_))));
        });
    }

    #[test]
    fn test_promote_scheduled_on_nonexistent_queue() {
        with_manager(|mut mgr| {
            let err = mgr.promote_scheduled("nope");
            assert!(matches!(err, Err(PelicanError::QueueNotFound(_))));
        });
    }

    // --- Phase 2, Step 5 acceptance tests (deduplication) ---

    #[test]
    fn test_dedup_rejects_duplicate_within_window() {
        with_manager(|mut mgr| {
            mgr.declare_queue_with_retention(
                "payments",
                RetentionPolicy::new(None, None, None, Some(60)),
            )
            .unwrap();

            let outcome1 = mgr.publish(
                "payments",
                Message::new(b"txn-1".to_vec(), std::collections::HashMap::new())
                    .with_dedup_key("txn-123"),
            )
            .unwrap();
            assert!(matches!(outcome1, PublishOutcome::Stored(_)));
            assert_eq!(mgr.depth("payments").unwrap(), 1);

            let outcome2 = mgr.publish(
                "payments",
                Message::new(b"txn-2".to_vec(), std::collections::HashMap::new())
                    .with_dedup_key("txn-123"),
            )
            .unwrap();
            assert_eq!(outcome2, PublishOutcome::Deduplicated);
            assert_eq!(mgr.depth("payments").unwrap(), 1);
        });
    }

    #[test]
    fn test_dedup_no_key_always_stored() {
        with_manager(|mut mgr| {
            mgr.declare_queue_with_retention(
                "payments",
                RetentionPolicy::new(None, None, None, Some(60)),
            )
            .unwrap();

            let outcome1 = mgr.publish(
                "payments",
                Message::new(b"a".to_vec(), std::collections::HashMap::new()),
            )
            .unwrap();
            assert!(matches!(outcome1, PublishOutcome::Stored(_)));

            let outcome2 = mgr.publish(
                "payments",
                Message::new(b"b".to_vec(), std::collections::HashMap::new()),
            )
            .unwrap();
            assert!(matches!(outcome2, PublishOutcome::Stored(_)));

            assert_eq!(mgr.depth("payments").unwrap(), 2);
        });
    }

    #[test]
    fn test_dedup_disabled_stores_duplicates() {
        with_manager(|mut mgr| {
            mgr.declare_queue_with_retention(
                "logs",
                RetentionPolicy::new(None, None, None, None),
            )
            .unwrap();

            let outcome1 = mgr.publish(
                "logs",
                Message::new(b"x".to_vec(), std::collections::HashMap::new())
                    .with_dedup_key("x"),
            )
            .unwrap();
            assert!(matches!(outcome1, PublishOutcome::Stored(_)));

            let outcome2 = mgr.publish(
                "logs",
                Message::new(b"x".to_vec(), std::collections::HashMap::new())
                    .with_dedup_key("x"),
            )
            .unwrap();
            assert!(matches!(outcome2, PublishOutcome::Stored(_)));

            assert_eq!(mgr.depth("logs").unwrap(), 2);
        });
    }

    #[test]
    fn test_dedup_zero_window_never_blocks() {
        with_manager(|mut mgr| {
            mgr.declare_queue_with_retention(
                "events",
                RetentionPolicy::new(None, None, None, Some(0)),
            )
            .unwrap();

            let outcome1 = mgr.publish(
                "events",
                Message::new(b"a".to_vec(), std::collections::HashMap::new())
                    .with_dedup_key("a"),
            )
            .unwrap();
            assert!(matches!(outcome1, PublishOutcome::Stored(_)));

            let outcome2 = mgr.publish(
                "events",
                Message::new(b"a".to_vec(), std::collections::HashMap::new())
                    .with_dedup_key("a"),
            )
            .unwrap();
            assert!(matches!(outcome2, PublishOutcome::Stored(_)));

            assert_eq!(mgr.depth("events").unwrap(), 2);
        });
    }

    #[test]
    fn test_publish_returns_stored_uuid() {
        with_manager(|mut mgr| {
            mgr.declare_queue("q").unwrap();
            let msg = Message::new(b"hello".to_vec(), std::collections::HashMap::new());
            let msg_id = msg.id;

            let outcome = mgr.publish("q", msg).unwrap();
            assert_eq!(outcome, PublishOutcome::Stored(msg_id));
        });
    }
}
