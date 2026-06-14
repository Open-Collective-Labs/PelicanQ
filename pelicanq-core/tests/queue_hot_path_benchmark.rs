use std::hint::black_box;
use std::time::Instant;

use pelicanq_core::message::Message;
use pelicanq_core::queue::QueueManager;

#[test]
#[ignore = "benchmark: run explicitly with cargo test -p pelicanq-core --test queue_hot_path_benchmark -- --ignored --nocapture"]
fn queue_publish_and_publish_consume_ack_hot_paths() {
    const ITERS: usize = 1_000;

    let dir = tempfile::tempdir().unwrap();
    let mut mgr = QueueManager::open(dir.path(), None).unwrap();
    mgr.declare_queue("publish").unwrap();

    let started = Instant::now();
    for _ in 0..ITERS {
        let msg = Message::new(black_box(b"payload".to_vec()), Default::default());
        black_box(mgr.publish("publish", msg).unwrap());
    }
    let publish_elapsed = started.elapsed();

    let dir = tempfile::tempdir().unwrap();
    let mut mgr = QueueManager::open(dir.path(), None).unwrap();
    mgr.declare_queue("roundtrip").unwrap();

    let started = Instant::now();
    for _ in 0..ITERS {
        let msg = Message::new(black_box(b"payload".to_vec()), Default::default());
        black_box(mgr.publish("roundtrip", msg).unwrap());
        let (tag, msg) = mgr.consume("roundtrip").unwrap().unwrap();
        black_box(msg);
        mgr.ack("roundtrip", tag).unwrap();
    }
    let roundtrip_elapsed = started.elapsed();

    println!(
        "queue_publish: {:?} total ({:?}/op)",
        publish_elapsed,
        publish_elapsed / ITERS as u32
    );
    println!(
        "queue_publish_consume_ack: {:?} total ({:?}/op)",
        roundtrip_elapsed,
        roundtrip_elapsed / ITERS as u32
    );
}
