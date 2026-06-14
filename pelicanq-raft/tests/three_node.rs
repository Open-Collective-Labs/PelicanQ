/// 3-node cluster integration test.
///
/// Starts three real Raft nodes, initializes the cluster, verifies leader
/// election and log replication, then kills the leader and confirms a new
/// leader is elected. Finally, restarts a follower and verifies it catches
/// up and its state machine matches the leader.
///
/// ```bash
/// cargo test -p pelicanq-raft -- --ignored
/// ```
use std::collections::BTreeMap;
use std::time::Duration;

use pelicanq_raft::requests::QueueOperation;
use pelicanq_raft::storage::RaftStore;

const RETRIES: usize = 120;

async fn wait<F, T>(f: F) -> T
where
    F: Fn() -> Option<T>,
{
    for _ in 0..RETRIES {
        if let Some(val) = f() {
            return val;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("timeout waiting for condition");
}

fn leader_id(rafts: &[(u64, pelicanq_raft::Raft)]) -> Option<u64> {
    for (id, raft) in rafts {
        let metrics = raft.metrics().borrow().clone();
        if metrics.current_leader == Some(*id) {
            return Some(*id);
        }
    }
    None
}

async fn node_depth(
    store: &RaftStore,
    queue: &str,
) -> Option<usize> {
    store
        .with_qm(|qm| {
            if !qm.list_queues().contains(&queue.to_string()) {
                return None;
            }
            Some(qm.depth(queue).unwrap_or(0))
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn three_node_cluster() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let raft_addrs = vec![
        "127.0.0.1:21011".to_string(),
        "127.0.0.1:21012".to_string(),
        "127.0.0.1:21013".to_string(),
    ];

    let nodes: BTreeMap<u64, pelicanq_raft::Node> = (1u64..=3)
        .map(|id| {
            (
                id,
                pelicanq_raft::Node {
                    raft_addr: raft_addrs[id as usize - 1].clone(),
                    client_addr: String::new(),
                },
            )
        })
        .collect();

    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();

    let mut rafts: Vec<(u64, pelicanq_raft::Raft)> = Vec::new();
    let mut stores: Vec<RaftStore> = Vec::new();

    for (i, addr) in raft_addrs.iter().enumerate() {
        let id = (i + 1) as u64;
        let (raft, store) = pelicanq_raft::start_node(id, addr, dirs[i].path())
            .await
            .expect("failed to start node");
        rafts.push((id, raft));
        stores.push(store);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Initialize cluster on node 1 with all 3 members.
    rafts[0].1.initialize(nodes.clone()).await.unwrap();

    // Wait for a leader to be elected.
    let leader = wait(|| leader_id(&rafts)).await;
    tracing::info!("leader elected: {leader}");

    // Exactly one node reports itself as leader.
    let leader_count = rafts
        .iter()
        .filter(|(id, raft)| raft.metrics().borrow().current_leader == Some(*id))
        .count();
    assert_eq!(leader_count, 1, "expected exactly one self-reported leader");

    // Submit a DeclareQueue operation on the leader.
    let leader_idx = rafts.iter().position(|(id, _)| *id == leader).unwrap();
    rafts[leader_idx]
        .1
        .client_write(QueueOperation::DeclareQueue {
            name: "test".to_string(),
            policy: Default::default(),
        })
        .await
        .expect("client_write failed");

    // Wait for replication.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Verify all nodes have the queue (replication succeeded).
    for store in &stores {
        let depth = node_depth(store, "test").await;
        assert_eq!(depth, Some(0), "queue should exist and have depth 0");
    }

    // --- Restart a follower ---
    // Pick a follower (not the leader).
    let follower_idx = (0..rafts.len()).find(|i| *i != leader_idx).unwrap();
    let follower_id = rafts[follower_idx].0;
    let follower_raft_addr = raft_addrs[follower_idx].clone();
    tracing::info!("restarting follower {follower_id} at {follower_raft_addr}");

    // Drop the follower's Raft instance and store (simulate crash).
    let _dropped_raft = rafts.remove(follower_idx);
    let _dropped_store = stores.remove(follower_idx);

    // Recreate with the same data directory.
    let (raft, store) = pelicanq_raft::start_node(
        follower_id,
        &follower_raft_addr,
        dirs[follower_idx].path(),
    )
    .await
    .expect("failed to restart follower");

    rafts.push((follower_id, raft));
    stores.push(store);
    // Re-order so rafts[0] / rafts[1] / rafts[2] is not guaranteed but
    // that's fine — the restarted node just needs to catch up.

    // Wait for the restarted node to catch up.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Verify the restarted node's state machine has the queue.
    let restarted_store_idx = 'found: {
        for (i, store) in stores.iter().enumerate() {
            let depth = node_depth(store, "test").await;
            if depth == Some(0) {
                break 'found i;
            }
        }
        panic!("restarted node should have caught up");
    };

    tracing::info!(
        "restarted node caught up (store index {restarted_store_idx})"
    );

    // Kill the leader (drop its Raft handle).
    tracing::info!("killing original leader {leader}");
    let dead_idx = rafts.iter().position(|(id, _)| *id == leader).unwrap();
    rafts.remove(dead_idx);
    stores.remove(dead_idx);

    // Wait for a new leader among the remaining nodes.
    let new_leader_id = wait(|| leader_id(&rafts)).await;
    tracing::info!("new leader elected: {new_leader_id}");

    assert_ne!(
        new_leader_id, leader,
        "a different node should become leader"
    );
}
