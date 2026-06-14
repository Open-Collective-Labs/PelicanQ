use thiserror::Error;

#[derive(Debug, Error)]
pub enum PelicanClientError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("rpc error: {0}")]
    Rpc(tonic::Status),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("server error: {0}")]
    Server(String),
}

impl From<tonic::Status> for PelicanClientError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::NotFound => {
                PelicanClientError::NotFound(status.message().to_string())
            }
            _ => PelicanClientError::Rpc(status),
        }
    }
}
