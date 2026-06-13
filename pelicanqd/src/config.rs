/// Daemon configuration, loaded from environment variables (no config file in Phase 1).
pub struct Config {
    /// Directory for persisted queue data. Env: PELICANQ_DATA_DIR. Default: "./data".
    pub data_dir: std::path::PathBuf,
    /// HTTP listen address. Env: PELICANQ_LISTEN_ADDR. Default: "127.0.0.1:7070".
    pub listen_addr: String,
    /// Optional max bytes for storage watermark (see Step 4). Env: PELICANQ_MAX_BYTES. Default: None (no limit).
    pub max_bytes: Option<u64>,
}

impl Config {
    /// Loads config from environment variables, applying defaults for unset values.
    pub fn from_env() -> Self {
        let data_dir = std::env::var("PELICANQ_DATA_DIR")
            .unwrap_or_else(|_| "./data".to_string());
        let listen_addr = std::env::var("PELICANQ_LISTEN_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:7070".to_string());
        let max_bytes = std::env::var("PELICANQ_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok());

        Self {
            data_dir: std::path::PathBuf::from(data_dir),
            listen_addr,
            max_bytes,
        }
    }
}
