use std::fmt;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::Adaptor;
use serde::{Deserialize, Serialize};

pub use crate::requests::{QueueOperation, QueueOperationResponse};
pub use crate::storage::RaftStore;

/// Our custom Node type carries both the Raft RPC address and the
/// client-facing HTTP address.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct Node {
    pub raft_addr: String,
    pub client_addr: String,
}

impl fmt::Display for QueueOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueOperation::DeclareQueue { name, .. } => {
                write!(f, "DeclareQueue({name})")
            }
            QueueOperation::Publish { queue, .. } => write!(f, "Publish({queue})"),
            QueueOperation::PublishBatch { queue, messages } => {
                write!(f, "PublishBatch({queue}, count={})", messages.len())
            }
            QueueOperation::Consume { queue } => write!(f, "Consume({queue})"),
            QueueOperation::ConsumeBatch { queue, max } => {
                write!(f, "ConsumeBatch({queue}, max={max})")
            }
            QueueOperation::Ack { queue, tag } => write!(f, "Ack({queue}, {tag})"),
            QueueOperation::Nack { queue, tag } => write!(f, "Nack({queue}, {tag})"),
            QueueOperation::ApplyRetention { queue } => write!(f, "ApplyRetention({queue})"),
            QueueOperation::PromoteScheduled { queue } => {
                write!(f, "PromoteScheduled({queue})")
            }
        }
    }
}

impl fmt::Display for QueueOperationResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueOperationResponse::DeclareQueue(r) => write!(f, "DeclareQueue({r:?})"),
            QueueOperationResponse::Publish(r) => write!(f, "Publish({r:?})"),
            QueueOperationResponse::PublishBatch(r) => write!(f, "PublishBatch({r:?})"),
            QueueOperationResponse::Consume(r) => write!(f, "Consume({r:?})"),
            QueueOperationResponse::ConsumeBatch(r) => write!(f, "ConsumeBatch({r:?})"),
            QueueOperationResponse::Ack(r) => write!(f, "Ack({r:?})"),
            QueueOperationResponse::Nack(r) => write!(f, "Nack({r:?})"),
            QueueOperationResponse::ApplyRetention(r) => write!(f, "ApplyRetention({r:?})"),
            QueueOperationResponse::PromoteScheduled(r) => write!(f, "PromoteScheduled({r:?})"),
        }
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = QueueOperation,
        R = QueueOperationResponse,
        NodeId = u64,
        Node = Node,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub mod network;
pub mod requests;
pub mod state_machine;
pub mod storage;

/// Start a single Raft node.
///
/// * `node_id` — unique node identifier.
/// * `raft_addr` — address for inter-node Raft RPCs (e.g. `"127.0.0.1:21011"`).
/// * `data_dir` — filesystem directory for the local sled-backed QueueManager.
///
/// Returns a `Raft` handle and the `RaftStore` (for test inspection).
pub async fn start_node(
    node_id: u64,
    raft_addr: &str,
    data_dir: &Path,
) -> Result<(network::Raft, RaftStore), Box<dyn std::error::Error>> {
    let config = Arc::new(
        openraft::Config {
            heartbeat_interval: 250,
            election_timeout_min: 750,
            election_timeout_max: 1500,
            ..Default::default()
        }
        .validate()
        .expect("invalid Raft config"),
    );

    let store = RaftStore::open(data_dir).await?;
    let (log_store, state_machine) = Adaptor::new(store.clone());

    let network = network::NetworkFactory::new();

    let raft =
        network::Raft::new(node_id, config, network, log_store, state_machine).await?;

    // Start the Raft RPC server.
    let raft_clone = raft.clone();
    let addr_str = raft_addr.to_string();
    tokio::spawn(async move {
        network::run_server(raft_clone, &addr_str).await;
    });

    tracing::info!("Raft node {node_id} listening on {raft_addr}");
    Ok((raft, store))
}

/// Handle used by `pelicanqd` in Flock (clustered) mode.
///
/// Wraps the Raft instance for `client_write` calls and provides read
/// access to the local state machine's `QueueManager`.
#[derive(Clone)]
pub struct FlockHandle {
    pub raft: network::Raft,
    pub store: RaftStore,
}

/// Result of a client_write attempt via the FlockHandle.
pub enum WriteResult {
    /// The operation was applied. Contains the response.
    Ok(QueueOperationResponse),
    /// This node is not the leader. Contains the leader's Node if known.
    NotLeader {
        leader_node: Option<Node>,
    },
    /// A fatal/internal error occurred.
    Error(String),
}

impl FlockHandle {
    /// Create a new FlockHandle from a Raft instance and its backing store.
    pub fn new(raft: network::Raft, store: RaftStore) -> Self {
        Self { raft, store }
    }

    /// Submit a mutating operation through Raft consensus.
    ///
    /// Returns `WriteResult::Ok` on success only after the operation has been
    /// applied to the state machine (committed and executed), ensuring durability
    /// across node restarts and leader failovers.
    ///
    /// Returns `WriteResult::NotLeader` if this node is a follower (with optional
    /// leader info), or `WriteResult::Error` for fatal/internal errors.
    pub async fn client_write(&self, op: QueueOperation) -> WriteResult {
        match self.raft.client_write(op).await {
            Ok(resp) => {
                // client_write() from openraft 0.9+ guarantees the entry is replicated
                // to a quorum and applied to the state machine before returning.
                // The response is safe to return to the client as durable.
                WriteResult::Ok(resp.data)
            }
            Err(raft_err) => {
                // Check if this is a ForwardToLeader error.
                if let Some(forward) = raft_err.forward_to_leader() {
                    return WriteResult::NotLeader {
                        leader_node: forward.leader_node.clone(),
                    };
                }
                WriteResult::Error(raft_err.to_string())
            }
        }
    }

    /// Access the local QueueManager for read-only operations.
    ///
    /// The returned lock guard provides access to the local state machine's
    /// QueueManager. Reads are locally-served and may lag the leader.
    pub async fn with_qm<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&pelicanq_core::queue::QueueManager) -> R,
    {
        let inner = self.store.inner.lock().await;
        f(&inner.state_machine)
    }
}

pub use network::Raft;
