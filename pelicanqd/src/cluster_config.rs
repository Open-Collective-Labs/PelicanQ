use serde::{Deserialize, Serialize};

/// Identifies a node in the cluster. Small positive integers, e.g. 1, 2, 3.
pub type NodeId = u64;

/// Static description of one cluster member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: NodeId,
    /// Address other nodes use to reach this node's internal Raft RPC port,
    /// e.g. "10.0.0.2:7071". Distinct from the client-facing HTTP API port.
    pub raft_addr: String,
    /// Address clients use to reach this node's HTTP API, e.g. "10.0.0.2:7070".
    /// Used for leader-redirect responses in Step 4.
    pub client_addr: String,
}

/// Full static cluster topology, loaded from config.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's own ID. Must appear in `members`.
    pub self_id: NodeId,
    pub members: Vec<NodeConfig>,
}

impl ClusterConfig {
    /// Parses cluster config from environment variables.
    ///
    /// If `PELICANQ_NODE_ID` is not set, returns `None` (Solo mode — no Raft).
    /// Returns an error if the env vars are malformed or `self_id` is not in
    /// the members list.
    pub fn from_env() -> Result<Option<Self>, String> {
        let self_id = match std::env::var("PELICANQ_NODE_ID") {
            Ok(v) => v.parse::<NodeId>().map_err(|e| {
                format!("invalid PELICANQ_NODE_ID '{v}': {e}")
            })?,
            Err(_) => return Ok(None),
        };

        let members_str = std::env::var("PELICANQ_CLUSTER_MEMBERS").map_err(|_| {
            "PELICANQ_NODE_ID is set but PELICANQ_CLUSTER_MEMBERS is missing".to_string()
        })?;

        let mut members = Vec::new();
        for entry in members_str.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let (id_addr_part, client_addr) = entry.split_once('=').ok_or_else(|| {
                format!(
                    "invalid cluster member entry (missing '='): {entry}"
                )
            })?;
            let (id_part, raft_addr) = id_addr_part.split_once('@').ok_or_else(|| {
                format!(
                    "invalid cluster member entry (missing '@'): {entry}"
                )
            })?;
            let id: NodeId = id_part.parse().map_err(|e| {
                format!("invalid node id in '{entry}': {e}")
            })?;
            members.push(NodeConfig {
                id,
                raft_addr: raft_addr.to_string(),
                client_addr: client_addr.to_string(),
            });
        }

        if members.is_empty() {
            return Err(
                "PELICANQ_CLUSTER_MEMBERS is empty".to_string(),
            );
        }

        if !members.iter().any(|m| m.id == self_id) {
            return Err(format!(
                "self_id {self_id} is not present in PELICANQ_CLUSTER_MEMBERS"
            ));
        }

        Ok(Some(ClusterConfig { self_id, members }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_solo_mode_when_node_id_unset() {
        temp_env::with_var("PELICANQ_NODE_ID", None::<&str>, || {
            let result = ClusterConfig::from_env().unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_parses_three_node_cluster() {
        temp_env::with_vars(
            vec![
                ("PELICANQ_NODE_ID", Some("2")),
                (
                    "PELICANQ_CLUSTER_MEMBERS",
                    Some("1@10.0.0.1:7071=10.0.0.1:7070,2@10.0.0.2:7071=10.0.0.2:7070,3@10.0.0.3:7071=10.0.0.3:7070"),
                ),
            ],
            || {
                let cfg = ClusterConfig::from_env()
                    .unwrap()
                    .expect("should be cluster mode");
                assert_eq!(cfg.self_id, 2);
                assert_eq!(cfg.members.len(), 3);
                assert_eq!(cfg.members[0].id, 1);
                assert_eq!(cfg.members[0].raft_addr, "10.0.0.1:7071");
                assert_eq!(cfg.members[0].client_addr, "10.0.0.1:7070");
                assert_eq!(cfg.members[1].id, 2);
                assert_eq!(cfg.members[1].raft_addr, "10.0.0.2:7071");
                assert_eq!(cfg.members[1].client_addr, "10.0.0.2:7070");
                assert_eq!(cfg.members[2].id, 3);
                assert_eq!(cfg.members[2].raft_addr, "10.0.0.3:7071");
                assert_eq!(cfg.members[2].client_addr, "10.0.0.3:7070");
            },
        );
    }

    #[test]
    fn test_self_id_not_in_members_errors() {
        temp_env::with_vars(
            vec![
                ("PELICANQ_NODE_ID", Some("99")),
                (
                    "PELICANQ_CLUSTER_MEMBERS",
                    Some("1@10.0.0.1:7071=10.0.0.1:7070,2@10.0.0.2:7071=10.0.0.2:7070"),
                ),
            ],
            || {
                let err = ClusterConfig::from_env().unwrap_err();
                assert!(err.contains("self_id 99"), "{err}");
            },
        );
    }

    #[test]
    fn test_malformed_entry_missing_at_sign() {
        temp_env::with_vars(
            vec![
                ("PELICANQ_NODE_ID", Some("1")),
                ("PELICANQ_CLUSTER_MEMBERS", Some("1bad:7071=10.0.0.1:7070")),
            ],
            || {
                let err = ClusterConfig::from_env().unwrap_err();
                assert!(err.contains("missing '@'"), "{err}");
            },
        );
    }

    #[test]
    fn test_malformed_entry_missing_equals() {
        temp_env::with_vars(
            vec![
                ("PELICANQ_NODE_ID", Some("1")),
                ("PELICANQ_CLUSTER_MEMBERS", Some("1@10.0.0.1:7071")),
            ],
            || {
                let err = ClusterConfig::from_env().unwrap_err();
                assert!(err.contains("missing '='"), "{err}");
            },
        );
    }

    #[test]
    fn test_invalid_node_id_format() {
        temp_env::with_vars(
            vec![
                ("PELICANQ_NODE_ID", Some("abc")),
                ("PELICANQ_CLUSTER_MEMBERS", Some("1@10.0.0.1:7071=10.0.0.1:7070")),
            ],
            || {
                let err = ClusterConfig::from_env().unwrap_err();
                assert!(err.contains("invalid PELICANQ_NODE_ID"), "{err}");
            },
        );
    }
}
