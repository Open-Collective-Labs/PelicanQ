use crate::cluster_config::ClusterConfig;

/// Daemon configuration, loaded from environment variables.
pub struct Config {
    /// Directory for persisted queue data. Env: PELICANQ_DATA_DIR. Default: "./data".
    pub data_dir: std::path::PathBuf,
    /// HTTP listen address. Env: PELICANQ_LISTEN_ADDR. Default: "127.0.0.1:7070".
    pub listen_addr: String,
    /// gRPC listen address. Env: PELICANQ_GRPC_ADDR. Default: "127.0.0.1:7072".
    pub grpc_addr: String,
    /// Optional max bytes for storage watermark. Env: PELICANQ_MAX_BYTES. Default: None.
    pub max_bytes: Option<u64>,
    /// Cluster configuration. `None` means Solo mode (no Raft).
    pub cluster: Option<ClusterConfig>,
}

impl Config {
    /// Loads config from environment variables, applying defaults for unset values.
    /// Fails fast with a clear message if cluster env vars are malformed.
    pub fn from_env() -> Self {
        let data_dir = std::env::var("PELICANQ_DATA_DIR")
            .unwrap_or_else(|_| "./data".to_string());
        let listen_addr = std::env::var("PELICANQ_LISTEN_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:7070".to_string());
        let grpc_addr = std::env::var("PELICANQ_GRPC_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:7072".to_string());
        let max_bytes = std::env::var("PELICANQ_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok());

        let cluster = match ClusterConfig::from_env() {
            Ok(c) => c,
            Err(msg) => {
                eprintln!("FATAL: {msg}");
                std::process::exit(1);
            }
        };

        Self {
            data_dir: std::path::PathBuf::from(data_dir),
            listen_addr,
            grpc_addr,
            max_bytes,
            cluster,
        }
    }
}
