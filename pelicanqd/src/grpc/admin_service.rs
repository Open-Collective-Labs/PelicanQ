use tonic::{Request, Response, Status};

use crate::api::{AppEngine, SharedState};
use crate::grpc::pb::admin_service_server::AdminService;
use crate::grpc::pb::{ClusterMember, ClusterStatusResponse, HealthResponse};

pub struct AdminServiceImpl {
    state: SharedState,
}

impl AdminServiceImpl {
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn health(
        &self,
        _request: Request<crate::grpc::pb::HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".to_string(),
        }))
    }

    async fn cluster_status(
        &self,
        _request: Request<crate::grpc::pb::ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let (self_id, is_leader, current_leader_id, members) =
            match &self.state.engine {
                AppEngine::Flock(handle) => {
                    let metrics = handle.raft.metrics().borrow().clone();
                    let id = metrics.id;
                    let leader = metrics.current_leader;
                    let is_leader = leader == Some(id);
                    let members: Vec<ClusterMember> = self
                        .state
                        .cluster
                        .as_ref()
                        .map(|c| {
                            c.members
                                .iter()
                                .map(|m| ClusterMember {
                                    id: m.id,
                                    raft_addr: m.raft_addr.clone(),
                                    client_addr: m.client_addr.clone(),
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    (id, is_leader, leader, members)
                }
                AppEngine::Solo(_) => (0, true, Some(0), vec![]),
            };

        Ok(Response::new(ClusterStatusResponse {
            self_id,
            is_leader,
            current_leader_id,
            members,
        }))
    }
}
