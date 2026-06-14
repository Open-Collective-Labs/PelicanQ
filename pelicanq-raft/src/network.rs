use axum::extract::State;
use axum::routing::post;
use axum::Json;
use openraft::error::{NetworkError, RPCError, RemoteError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::{Node, TypeConfig};

// ── Network Factory ──────────────────────────────────────────────────────

#[derive(Clone)]
pub struct NetworkFactory;

impl NetworkFactory {
    pub fn new() -> Self {
        Self
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Client;

    async fn new_client(&mut self, _target: u64, node: &Node) -> Self::Network {
        Client::new(node.raft_addr.clone())
    }
}

// ── Network Client ───────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Client {
    addr: String,
    client: reqwest::Client,
}

impl Client {
    fn new(addr: String) -> Self {
        Self {
            addr,
            client: reqwest::Client::new(),
        }
    }

    /// POST JSON to a Raft endpoint and return the raw response body as text.
    async fn post_json_raw<Req>(&self, uri: &str, req: &Req) -> Result<String, NetworkError>
    where
        Req: serde::Serialize,
    {
        let url = format!("http://{}{}", self.addr, uri);

        let resp = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| NetworkError::new(&e))?;

        let body = resp
            .text()
            .await
            .map_err(|e| NetworkError::new(&e))?;

        Ok(body)
    }
}

impl RaftNetwork<TypeConfig> for Client {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, Node, openraft::error::RaftError<u64>>> {
        let body = self.post_json_raw("/raft-append-entries", &rpc).await
            .map_err(|e| RPCError::Network(e))?;
        let result: Result<AppendEntriesResponse<u64>, openraft::error::RaftError<u64>> =
            serde_json::from_str(&body)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(0, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<
            u64,
            Node,
            openraft::error::RaftError<u64, openraft::error::InstallSnapshotError>,
        >,
    > {
        let body = self.post_json_raw("/raft-snapshot", &rpc).await
            .map_err(|e| RPCError::Network(e))?;
        let result: Result<
            InstallSnapshotResponse<u64>,
            openraft::error::RaftError<u64, openraft::error::InstallSnapshotError>,
        > = serde_json::from_str(&body)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(0, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, Node, openraft::error::RaftError<u64>>> {
        let body = self.post_json_raw("/raft-vote", &rpc).await
            .map_err(|e| RPCError::Network(e))?;
        let result: Result<VoteResponse<u64>, openraft::error::RaftError<u64>> =
            serde_json::from_str(&body)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(0, e)))
    }
}

// ── Raft RPC Server ──────────────────────────────────────────────────────

pub type Raft = openraft::Raft<TypeConfig>;

pub async fn run_server(raft: Raft, addr: &str) {
    let app = axum::Router::new()
        .route("/raft-append-entries", post(handle_append_entries))
        .route("/raft-vote", post(handle_vote))
        .route("/raft-snapshot", post(handle_snapshot))
        .with_state(raft);

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, app).await.expect("server failed");
}

async fn handle_append_entries(
    State(raft): State<Raft>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<Result<AppendEntriesResponse<u64>, openraft::error::RaftError<u64>>> {
    Json(raft.append_entries(req).await)
}

async fn handle_vote(
    State(raft): State<Raft>,
    Json(req): Json<VoteRequest<u64>>,
) -> Json<Result<VoteResponse<u64>, openraft::error::RaftError<u64>>> {
    Json(raft.vote(req).await)
}

async fn handle_snapshot(
    State(raft): State<Raft>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<
    Result<
        InstallSnapshotResponse<u64>,
        openraft::error::RaftError<u64, openraft::error::InstallSnapshotError>,
    >,
> {
    Json(raft.install_snapshot(req).await)
}
