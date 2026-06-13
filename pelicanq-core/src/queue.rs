use std::path::Path;

use sled::transaction::ConflictableTransactionError;
use sled::Transactional;

use crate::error::PelicanError;
use crate::message::Message;
use crate::retention::RetentionPolicy;

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

    /// Recalculates the total byte count by summing all queue and inflight entries.
    fn recalculate_bytes(&self) -> Result<u64, PelicanError> {
        let mut total: u64 = 0;
        for name in self.list_queues() {
            let tree = self.db.open_tree(&name)?;
            for entry in tree.iter() {
                let (_, value) = entry?;
                total += value.len() as u64;
            }
            let inflight = self.db.open_tree(format!("inflight:{}", &name))?;
            for entry in inflight.iter() {
                let (_, value) = entry?;
                total += value.len() as u64;
            }
        }
        Ok(total)
    }

    /// Moves all in-flight messages back to their respective queues (crash recovery).
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
                let new_id = self.db.generate_id()?;
                tree.insert(new_id.to_be_bytes(), value.as_ref())?;
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
        let policy_bytes = bincode::serialize(&policy)?;
        self.retention_tree.insert(name, policy_bytes)?;
        Ok(())
    }

    /// Publishes a message to the named queue. Errors if the queue doesn't exist
    /// or if the storage limit would be exceeded.
    pub fn publish(&mut self, queue: &str, message: Message) -> Result<(), PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
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

        let tree = self.db.open_tree(queue)?;
        let id = self.db.generate_id()?;
        tree.insert(id.to_be_bytes(), value.as_slice())?;

        self.current_bytes += msg_len;
        Ok(())
    }

    /// Consumes the next message, moving it to the in-flight store.
    /// Returns the message along with a delivery tag needed to ack/nack it.
    /// Returns Ok(None) if the queue is empty.
    pub fn consume(&mut self, queue: &str) -> Result<Option<(u64, Message)>, PelicanError> {
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

            let result: sled::transaction::TransactionResult<Vec<u8>, ()> =
                (&tree, &inflight).transaction(|(tx_tree, tx_inflight)| {
                    match tx_tree.remove(key.as_slice())? {
                        Some(value) => {
                            let val_bytes = value.to_vec();
                            tx_inflight.insert(key.as_slice(), value)?;
                            Ok(val_bytes)
                        }
                        None => Err(ConflictableTransactionError::Abort(())),
                    }
                });

            match result {
                Ok(val_bytes) => {
                    let tag = u64::from_be_bytes(
                        key[..8]
                            .try_into()
                            .map_err(|e: std::array::TryFromSliceError| {
                                sled::Error::Io(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    e,
                                ))
                            })?,
                    );
                    let msg: Message = bincode::deserialize(&val_bytes)?;
                    return Ok(Some((tag, msg)));
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
    pub fn ack(&mut self, queue: &str, delivery_tag: u64) -> Result<(), PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }

        let inflight = self.db.open_tree(Self::inflight_tree_name(queue))?;
        let key = delivery_tag.to_be_bytes();

        match inflight.remove(key.as_slice())? {
            Some(value) => {
                self.current_bytes = self.current_bytes.saturating_sub(value.len() as u64);
                Ok(())
            }
            None => Err(PelicanError::InvalidDeliveryTag(delivery_tag)),
        }
    }

    /// Moves a message from the in-flight store back to the end of the main queue.
    pub fn nack(&mut self, queue: &str, delivery_tag: u64) -> Result<(), PelicanError> {
        if !self.meta_tree.contains_key(queue)? {
            return Err(PelicanError::QueueNotFound(queue.to_string()));
        }

        let tree = self.db.open_tree(queue)?;
        let inflight = self.db.open_tree(Self::inflight_tree_name(queue))?;
        let old_key = delivery_tag.to_be_bytes();
        let new_id = self.db.generate_id()?;
        let new_key = new_id.to_be_bytes();

        let result: sled::transaction::TransactionResult<(), ()> =
            (&inflight, &tree).transaction(|(tx_inflight, tx_tree)| {
                match tx_inflight.remove(old_key.as_slice())? {
                    Some(value) => {
                        tx_tree.insert(new_key.as_slice(), value)?;
                        Ok(())
                    }
                    None => Err(ConflictableTransactionError::Abort(())),
                }
            });

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

    /// Returns the current depth of the named queue. Errors if the queue doesn't exist.
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
                } else {
                    break;
                }
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

            let err = mgr.ack("q", 999);
            assert!(matches!(err, Err(PelicanError::InvalidDeliveryTag(999))));

            let err = mgr.nack("q", 999);
            assert!(matches!(err, Err(PelicanError::InvalidDeliveryTag(999))));
        });
    }

    // --- Step 4 acceptance tests (retention, storage limits) ---

    #[test]
    fn test_max_messages_retention() {
        with_manager(|mut mgr| {
            mgr.declare_queue_with_retention(
                "q",
                RetentionPolicy::new(None, Some(2)),
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
                RetentionPolicy::new(Some(0), None),
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
                Ok(()) => count += 1,
                Err(PelicanError::StorageLimitExceeded { .. }) => break,
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
}
