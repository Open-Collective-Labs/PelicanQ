package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Open-Collective-Labs/PelicanQ/sdks/go/pelicanq"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := pelicanq.Connect("127.0.0.1:7072")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	created, err := client.DeclareQueue(ctx, "example-queue", pelicanq.QueueOptions{})
	if err != nil {
		log.Fatalf("declare_queue: %v", err)
	}
	fmt.Printf("Queue created: %v\n", created)

	msg := pelicanq.NewMessage([]byte("Hello, PelicanQ!")).
		WithPriority(5).
		WithHeader("content-type", "text/plain")
	result, err := client.Publish(ctx, "example-queue", msg)
	if err != nil {
		log.Fatalf("publish: %v", err)
	}
	fmt.Printf("Published: id=%s\n", result.ID)

	delivery, err := client.Consume(ctx, "example-queue")
	if err != nil {
		log.Fatalf("consume: %v", err)
	}
	if delivery == nil {
		log.Fatal("no message received")
	}
	fmt.Printf("Received: payload=%q tag=%d\n", string(delivery.Message.Payload), delivery.DeliveryTag)

	if err := client.Ack(ctx, "example-queue", delivery.DeliveryTag); err != nil {
		log.Fatalf("ack: %v", err)
	}
	fmt.Println("Done!")
}
