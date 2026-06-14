pub mod admin_service;
pub mod queue_service;

pub mod pb {
    tonic::include_proto!("pelicanq.v1");
}
