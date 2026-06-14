pub mod client;
pub mod error;
pub mod types;

pub(crate) mod pb {
    tonic::include_proto!("pelicanq.v1");
}

pub use client::PelicanClient;
pub use error::PelicanClientError;
pub use types::{
    ClientMessage, Delivery, PublishResult, QueueInfo, QueueOptions,
};

#[cfg(test)]
mod tests {
    use super::types::ClientMessage;

    #[test]
    fn test_client_message_new() {
        let msg = ClientMessage::new(b"hello");
        assert_eq!(msg.payload, b"hello");
        assert!(msg.headers.is_empty());
        assert_eq!(msg.priority, 0);
        assert!(msg.deliver_at.is_none());
        assert!(msg.dedup_key.is_none());
    }

    #[test]
    fn test_client_message_with_priority() {
        let msg = ClientMessage::new(b"").with_priority(5);
        assert_eq!(msg.priority, 5);
    }

    #[test]
    fn test_client_message_clamps_priority() {
        let msg = ClientMessage::new(b"").with_priority(15);
        assert_eq!(msg.priority, 9);
    }

    #[test]
    fn test_client_message_with_deliver_at() {
        let msg = ClientMessage::new(b"").with_deliver_at(1000);
        assert_eq!(msg.deliver_at, Some(1000));
    }

    #[test]
    fn test_client_message_with_dedup_key() {
        let msg = ClientMessage::new(b"").with_dedup_key("key1");
        assert_eq!(msg.dedup_key, Some("key1".to_string()));
    }

    #[test]
    fn test_client_message_with_header() {
        let msg = ClientMessage::new(b"")
            .with_header("content-type", "text/plain");
        assert_eq!(msg.headers.get("content-type").unwrap(), "text/plain");
    }

    #[test]
    fn test_queue_options_default_all_none() {
        let opts = super::types::QueueOptions::default();
        assert!(opts.max_age_secs.is_none());
        assert!(opts.max_messages.is_none());
        assert!(opts.max_delivery_attempts.is_none());
        assert!(opts.dead_letter_queue.is_none());
        assert!(opts.dedup_window_secs.is_none());
    }
}
