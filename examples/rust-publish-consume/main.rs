use std::time::Duration;
use pelicanq::{ClientMessage, PelicanClient, QueueOptions};

#[tokio::main]
async fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:7072".to_string());

    println!("Connecting to PelicanQ at {addr} ...");
    let mut client = PelicanClient::connect(&addr).await
        .expect("failed to connect to PelicanQ daemon");

    client.health().await.expect("health check failed");
    println!("Health check OK");

    let queue_name = "example_queue";

    let created = client
        .declare_queue(queue_name, QueueOptions::default())
        .await
        .expect("failed to declare queue");
    println!(
        "Queue '{queue_name}' {}",
        if created { "created" } else { "already exists" }
    );

    let msg = ClientMessage::new(b"Hello, PelicanQ!")
        .with_priority(5)
        .with_header("content-type", "text/plain");
    let result = client.publish(queue_name, msg).await
        .expect("failed to publish message");
    println!("Published message id={}", result.id);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let delivery = client.consume(queue_name).await
        .expect("failed to consume message")
        .expect("no message available");
    println!(
        "Consumed message: payload={:?} tag={}",
        String::from_utf8_lossy(&delivery.message.payload),
        delivery.delivery_tag,
    );

    client.ack(queue_name, delivery.delivery_tag).await
        .expect("failed to ack message");
    println!("Acknowledged message");

    let queues = client.list_queues().await
        .expect("failed to list queues");
    println!("Queues:");
    for q in &queues {
        println!("  {} (depth={})", q.name, q.depth);
    }

    println!("Done!");
}
