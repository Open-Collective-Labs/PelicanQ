use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{RaftLogReader, RaftSnapshotBuilder, RaftStorage};
use openraft::{ErrorSubject, ErrorVerb};
use openraft::{
    EntryPayload, LogId, LogState, OptionalSend, Snapshot, SnapshotMeta, StorageError,
    StoredMembership,
};
use pelicanq_core::queue::QueueManager;
use tokio::sync::Mutex;

use crate::requests::QueueOperationResponse;
use crate::state_machine;
use crate::{Node, TypeConfig};

#[derive(Debug)]
pub(crate) struct RaftStoreInner {
    pub(crate) db: sled::Db,
    pub(crate) log_tree: sled::Tree,
    pub(crate) meta_tree: sled::Tree,
    pub(crate) state_machine: QueueManager,
    pub(crate) current_snapshot: Option<StoredSnapshot>,
}

#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<u64, Node>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RaftStore {
    pub(crate) inner: Arc<Mutex<RaftStoreInner>>,
}

fn io_err(msg: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, msg.to_string())
}

fn to_storage_err(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    e: impl std::fmt::Display,
) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, io_err(e))
}

fn be64(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

fn meta_set<T: serde::Serialize>(
    tree: &sled::Tree,
    db: &sled::Db,
    key: &str,
    value: &T,
) -> Result<(), StorageError<u64>> {
    let data = serde_json::to_vec(value)
        .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
    tree.insert(key, data)
        .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
    db.flush()
        .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
    Ok(())
}

fn meta_get_single<T: serde::de::DeserializeOwned>(
    tree: &sled::Tree,
    key: &str,
) -> Result<Option<T>, StorageError<u64>> {
    match tree
        .get(key)
        .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?
    {
        Some(val) => {
            let t: T = serde_json::from_slice(&val)
                .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
            Ok(Some(t))
        }
        None => Ok(None),
    }
}

impl RaftStore {
    /// Access the local QueueManager for read-only operations.
    ///
    /// This is used by tests and by `FlockHandle` for read-only access to
    /// the local state machine.
    pub async fn with_qm<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&pelicanq_core::queue::QueueManager) -> R,
    {
        let inner = self.inner.lock().await;
        f(&inner.state_machine)
    }

    pub async fn open(data_dir: &Path) -> Result<Self, pelicanq_core::error::PelicanError> {
        let qm = QueueManager::open(data_dir, None)?;

        let raft_path = data_dir.join("raft");
        std::fs::create_dir_all(&raft_path).ok();
        let db = sled::open(&raft_path)
            .map_err(|e| pelicanq_core::error::PelicanError::Storage {
                message: e.to_string(),
            })?;
        let log_tree = db.open_tree("log").map_err(|e| {
            pelicanq_core::error::PelicanError::Storage {
                message: e.to_string(),
            }
        })?;
        let meta_tree = db.open_tree("meta").map_err(|e| {
            pelicanq_core::error::PelicanError::Storage {
                message: e.to_string(),
            }
        })?;

        Ok(Self {
            inner: Arc::new(Mutex::new(RaftStoreInner {
                db,
                log_tree,
                meta_tree,
                state_machine: qm,
                current_snapshot: None,
            })),
        })
    }
}

impl RaftLogReader<TypeConfig> for RaftStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.lock().await;

        let start_u64 = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => i.saturating_add(1),
            std::ops::Bound::Unbounded => 0,
        };
        let end_u64 = match range.end_bound() {
            std::ops::Bound::Included(i) => i.saturating_add(1),
            std::ops::Bound::Excluded(i) => *i,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        if start_u64 >= end_u64 {
            return Ok(Vec::new());
        }

        let start_key = be64(start_u64);
        let end_key = be64(end_u64);

        let mut entries = Vec::new();
        for result in inner.log_tree.range(start_key..end_key) {
            let (_, value) =
                result.map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
            let entry: openraft::Entry<TypeConfig> = serde_json::from_slice(&value)
                .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RaftStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let mut inner = self.inner.lock().await;

        let data = state_machine::build_snapshot(&inner.state_machine)
            .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

        let last_applied: Option<LogId<u64>> = meta_get_single(&inner.meta_tree, "last_applied")?;
        let last_membership: StoredMembership<u64, Node> =
            meta_get_single(&inner.meta_tree, "last_membership")?.unwrap_or_default();

        let snapshot_idx: u64 = inner
            .meta_tree
            .get("snapshot_idx")
            .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?
            .map(|v| {
                let arr: [u8; 8] = v[..8].try_into().unwrap();
                u64::from_be_bytes(arr)
            })
            .unwrap_or(0)
            + 1;

        meta_set(
            &inner.meta_tree,
            &inner.db,
            "snapshot_idx",
            &snapshot_idx,
        )?;

        let snapshot_id = if let Some(ref last) = last_applied {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for RaftStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.inner.lock().await;

        let last_purged: Option<LogId<u64>> =
            meta_get_single(&inner.meta_tree, "last_purged")?;

        let last_log_id = match inner.log_tree.last() {
            Ok(Some((_, value))) => {
                let entry: openraft::Entry<TypeConfig> = serde_json::from_slice(&value)
                    .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
                Some(entry.log_id)
            }
            Ok(None) => last_purged,
            Err(e) => {
                return Err(to_storage_err(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    e,
                ))
            }
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StorageError<u64>> {
        let inner = self.inner.lock().await;
        meta_set(&inner.meta_tree, &inner.db, "vote", vote)
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        meta_get_single(&inner.meta_tree, "vote")
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
    {
        let inner = self.inner.lock().await;
        for entry in entries {
            let key = be64(entry.log_id.index);
            let value = serde_json::to_vec(&entry)
                .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
            inner
                .log_tree
                .insert(&key[..], value)
                .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        }
        inner
            .db
            .flush()
            .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let inner = self.inner.lock().await;
        let start_key = be64(log_id.index);
        let end_key = be64(u64::MAX);
        let keys: Vec<sled::IVec> = inner
            .log_tree
            .range(start_key..=end_key)
            .keys()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        for key in keys {
            inner
                .log_tree
                .remove(key)
                .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        }
        inner
            .db
            .flush()
            .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let inner = self.inner.lock().await;
        let start_key = be64(0);
        let end_key = be64(log_id.index);
        let keys: Vec<sled::IVec> = inner
            .log_tree
            .range(start_key..=end_key)
            .keys()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        for key in keys {
            inner
                .log_tree
                .remove(key)
                .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        }
        meta_set(&inner.meta_tree, &inner.db, "last_purged", &log_id)?;
        inner
            .db
            .flush()
            .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, Node>,
        ),
        StorageError<u64>,
    > {
        let inner = self.inner.lock().await;
        let last_applied: Option<LogId<u64>> =
            meta_get_single(&inner.meta_tree, "last_applied")?;
        let last_membership: StoredMembership<u64, Node> =
            meta_get_single(&inner.meta_tree, "last_membership")?.unwrap_or_default();
        Ok((last_applied, last_membership))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[openraft::Entry<TypeConfig>],
    ) -> Result<Vec<QueueOperationResponse>, StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        let mut resps = Vec::with_capacity(entries.len());

        for entry in entries {
            let qm_resp = match &entry.payload {
                EntryPayload::Blank => QueueOperationResponse::Ack(Ok(())),
                EntryPayload::Normal(op) => {
                    state_machine::apply_operation(&mut inner.state_machine, op.clone())
                }
                EntryPayload::Membership(mem) => {
                    let membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    meta_set(
                        &inner.meta_tree,
                        &inner.db,
                        "last_membership",
                        &membership,
                    )?;
                    QueueOperationResponse::Ack(Ok(()))
                }
            };

            meta_set(
                &inner.meta_tree,
                &inner.db,
                "last_applied",
                &entry.log_id,
            )?;

            resps.push(qm_resp);
        }

        Ok(resps)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, Node>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let mut inner = self.inner.lock().await;

        state_machine::install_snapshot(&mut inner.state_machine, &data).map_err(|e| {
            to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e)
        })?;

        meta_set(
            &inner.meta_tree,
            &inner.db,
            "last_applied",
            &meta.last_log_id,
        )?;
        meta_set(
            &inner.meta_tree,
            &inner.db,
            "last_membership",
            &meta.last_membership,
        )?;

        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });

        inner
            .db
            .flush()
            .map_err(|e| to_storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        match &inner.current_snapshot {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use openraft::testing::log_id;
    use openraft::EntryPayload;

    fn make_entry(index: u64, term: u64) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: log_id(term, 1, index),
            payload: EntryPayload::Blank,
        }
    }

    async fn open_store(dir: &Path) -> RaftStore {
        RaftStore::open(dir).await.unwrap()
    }

    #[tokio::test]
    async fn test_append_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open_store(dir.path()).await;

        store
            .append_to_log(vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)])
            .await
            .unwrap();

        let entries = store
            .try_get_log_entries(1..4)
            .await
            .unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].log_id, log_id(1, 1, 1));
        assert_eq!(entries[1].log_id, log_id(1, 1, 2));
        assert_eq!(entries[2].log_id, log_id(2, 1, 3));
    }

    #[tokio::test]
    async fn test_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();

        // First session
        {
            let mut store = open_store(dir.path()).await;
            store
                .append_to_log(vec![make_entry(10, 1), make_entry(11, 2)])
                .await
                .unwrap();
            // store dropped (simulates restart)
        }

        // Second session — reopen same data dir
        {
            let mut store = open_store(dir.path()).await;
            let entries = store
                .try_get_log_entries(10..12)
                .await
                .unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].log_id, log_id(1, 1, 10));
            assert_eq!(entries[1].log_id, log_id(2, 1, 11));
        }
    }

    #[tokio::test]
    async fn test_vote_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let vote = openraft::Vote::new(3, 1);

        // First session
        {
            let mut store = open_store(dir.path()).await;
            store.save_vote(&vote).await.unwrap();
        }

        // Second session
        {
            let mut store = open_store(dir.path()).await;
            let read = store.read_vote().await.unwrap();
            assert_eq!(read, Some(vote));
        }
    }

    #[tokio::test]
    async fn test_last_applied_persists() {
        let dir = tempfile::tempdir().unwrap();

        // First session
        {
            let mut store = open_store(dir.path()).await;
            let entry = make_entry(5, 1);
            store
                .append_to_log(vec![entry])
                .await
                .unwrap();
            // Apply to state machine, which records last_applied
            let entries = store
                .try_get_log_entries(5..6)
                .await
                .unwrap();
            store.apply_to_state_machine(&entries).await.unwrap();

            let (last_applied, _) = store.last_applied_state().await.unwrap();
            assert_eq!(last_applied, Some(log_id(1, 1, 5)));
        }

        // Second session: last_applied should survive restart
        {
            let mut store = open_store(dir.path()).await;
            let (last_applied, _) = store.last_applied_state().await.unwrap();
            assert_eq!(last_applied, Some(log_id(1, 1, 5)));
        }
    }

    #[tokio::test]
    async fn test_truncate_from_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open_store(dir.path()).await;

        store
            .append_to_log(vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)])
            .await
            .unwrap();

        // Delete from index 2 onwards
        store
            .delete_conflict_logs_since(log_id(1, 1, 2))
            .await
            .unwrap();

        let entries = store
            .try_get_log_entries(1..4)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_truncate_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();

        // First session
        {
            let mut store = open_store(dir.path()).await;
            store
                .append_to_log(vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)])
                .await
                .unwrap();
            store
                .delete_conflict_logs_since(log_id(1, 1, 2))
                .await
                .unwrap();
        }

        // Second session
        {
            let mut store = open_store(dir.path()).await;
            let entries = store
                .try_get_log_entries(1..4)
                .await
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].log_id.index, 1);
        }
    }

    #[tokio::test]
    async fn test_purge_updates_last_purged() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open_store(dir.path()).await;

        store
            .append_to_log(vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)])
            .await
            .unwrap();

        store
            .purge_logs_upto(log_id(1, 1, 2))
            .await
            .unwrap();

        // Only entry 3 should remain
        let entries = store
            .try_get_log_entries(1..4)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 3);

        // last_purged should be set
        let log_state = store.get_log_state().await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(log_id(1, 1, 2)));
    }
}
