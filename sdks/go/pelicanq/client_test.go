package pelicanq

import (
	"testing"
)

func TestNewMessage(t *testing.T) {
	msg := NewMessage([]byte("hello"))
	if string(msg.Payload) != "hello" {
		t.Fatalf("expected payload 'hello', got %q", msg.Payload)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("expected empty headers, got %v", msg.Headers)
	}
	if msg.Priority != 0 {
		t.Fatalf("expected priority 0, got %d", msg.Priority)
	}
}

func TestWithPriorityClamps(t *testing.T) {
	msg := NewMessage(nil).WithPriority(15)
	if msg.Priority != 9 {
		t.Fatalf("expected priority 9, got %d", msg.Priority)
	}
}

func TestWithPriorityNormal(t *testing.T) {
	msg := NewMessage(nil).WithPriority(5)
	if msg.Priority != 5 {
		t.Fatalf("expected priority 5, got %d", msg.Priority)
	}
}

func TestWithDeliverAt(t *testing.T) {
	msg := NewMessage(nil).WithDeliverAt(1000)
	if msg.DeliverAt == nil || *msg.DeliverAt != 1000 {
		t.Fatalf("expected deliver_at 1000, got %v", msg.DeliverAt)
	}
}

func TestWithDedupKey(t *testing.T) {
	msg := NewMessage(nil).WithDedupKey("k1")
	if msg.DedupKey == nil || *msg.DedupKey != "k1" {
		t.Fatalf("expected dedup_key 'k1', got %v", msg.DedupKey)
	}
}

func TestWithHeader(t *testing.T) {
	msg := NewMessage(nil).WithHeader("ct", "text/plain")
	if msg.Headers["ct"] != "text/plain" {
		t.Fatalf("expected header ct=text/plain, got %v", msg.Headers)
	}
}

func TestQueueOptionsDefaultAllNil(t *testing.T) {
	opts := QueueOptions{}
	if opts.MaxAgeSecs != nil {
		t.Fatal("expected nil MaxAgeSecs")
	}
	if opts.MaxMessages != nil {
		t.Fatal("expected nil MaxMessages")
	}
	if opts.MaxDeliveryAttempts != nil {
		t.Fatal("expected nil MaxDeliveryAttempts")
	}
	if opts.DeadLetterQueue != nil {
		t.Fatal("expected nil DeadLetterQueue")
	}
	if opts.DedupWindowSecs != nil {
		t.Fatal("expected nil DedupWindowSecs")
	}
}
