//go:build integration

package pelicanq

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestIntegrationPublishConsumeAck(t *testing.T) {
	addr := os.Getenv("PELICANQ_GRPC_ADDR")
	if addr == "" {
		addr = "127.0.0.1:7072"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := Connect(addr)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	created, err := client.DeclareQueue(ctx, "test-go-integration", QueueOptions{})
	if err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}
	t.Logf("queue created: %v", created)

	msg := NewMessage([]byte("hello from go")).WithPriority(3)
	result, err := client.Publish(ctx, "test-go-integration", msg)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Logf("published: id=%s dedup=%v", result.ID, result.Deduplicated)

	delivery, err := client.Consume(ctx, "test-go-integration")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if delivery == nil {
		t.Fatal("expected a message, got nil")
	}
	t.Logf("consumed: tag=%d payload=%q", delivery.DeliveryTag, string(delivery.Message.Payload))

	if err := client.Ack(ctx, "test-go-integration", delivery.DeliveryTag); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	if err := client.Health(ctx); err != nil {
		t.Fatalf("Health: %v", err)
	}
}
