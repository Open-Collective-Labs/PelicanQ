package pelicanq

import (
	"context"
	"fmt"

	pbv1 "github.com/Open-Collective-Labs/PelicanQ/sdks/go/pelicanq/pb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PelicanClient is a client for interacting with a PelicanQ daemon over gRPC.
type PelicanClient struct {
	conn   *grpc.ClientConn
	queue  pbv1.QueueServiceClient
	admin  pbv1.AdminServiceClient
}

// Connect connects to a PelicanQ daemon at the given address (e.g. "127.0.0.1:7072").
func Connect(addr string) (*PelicanClient, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("pelicanq: connect: %w", err)
	}
	return &PelicanClient{
		conn:  conn,
		queue: pbv1.NewQueueServiceClient(conn),
		admin: pbv1.NewAdminServiceClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *PelicanClient) Close() error {
	return c.conn.Close()
}

// DeclareQueue declares a queue. Idempotent — returns true if newly created.
func (c *PelicanClient) DeclareQueue(ctx context.Context, name string, opts QueueOptions) (bool, error) {
	resp, err := c.queue.DeclareQueue(ctx, &pbv1.DeclareQueueRequest{
		Name:                name,
		MaxAgeSecs:          opts.MaxAgeSecs,
		MaxMessages:         opts.MaxMessages,
		MaxDeliveryAttempts: opts.MaxDeliveryAttempts,
		DeadLetterQueue:     opts.DeadLetterQueue,
		DedupWindowSecs:     opts.DedupWindowSecs,
	})
	if err != nil {
		return false, fmt.Errorf("pelicanq: declare_queue: %w", err)
	}
	return resp.Created, nil
}

// Publish publishes a single message to a queue.
func (c *PelicanClient) Publish(ctx context.Context, queue string, msg ClientMessage) (PublishResult, error) {
	resp, err := c.queue.Publish(ctx, &pbv1.PublishRequest{
		Queue:   queue,
		Message: msg.toProto(),
	})
	if err != nil {
		return PublishResult{}, fmt.Errorf("pelicanq: publish: %w", err)
	}
	return publishResultFromProto(resp), nil
}

// PublishBatch publishes multiple messages in a single batch call.
func (c *PelicanClient) PublishBatch(ctx context.Context, queue string, msgs []ClientMessage) ([]PublishResult, error) {
	protoMsgs := make([]*pbv1.Message, len(msgs))
	for i, m := range msgs {
		protoMsgs[i] = m.toProto()
	}
	resp, err := c.queue.PublishBatch(ctx, &pbv1.PublishBatchRequest{
		Queue:    queue,
		Messages: protoMsgs,
	})
	if err != nil {
		return nil, fmt.Errorf("pelicanq: publish_batch: %w", err)
	}
	results := make([]PublishResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = publishResultFromProto(r)
	}
	return results, nil
}

// Consume consumes a single message. Returns nil if the queue is empty.
func (c *PelicanClient) Consume(ctx context.Context, queue string) (*Delivery, error) {
	resp, err := c.queue.Consume(ctx, &pbv1.ConsumeRequest{
		Queue: queue,
	})
	if err != nil {
		return nil, fmt.Errorf("pelicanq: consume: %w", err)
	}
	if resp.Message == nil {
		return nil, nil
	}
	d := deliveryFromProto(resp.Message)
	return &d, nil
}

// ConsumeBatch consumes up to max messages from a queue.
func (c *PelicanClient) ConsumeBatch(ctx context.Context, queue string, max uint32) ([]Delivery, error) {
	resp, err := c.queue.ConsumeBatch(ctx, &pbv1.ConsumeBatchRequest{
		Queue: queue,
		Max:   max,
	})
	if err != nil {
		return nil, fmt.Errorf("pelicanq: consume_batch: %w", err)
	}
	deliveries := make([]Delivery, len(resp.Messages))
	for i, m := range resp.Messages {
		deliveries[i] = deliveryFromProto(m)
	}
	return deliveries, nil
}

// Ack acknowledges a message, removing it from the in-flight store.
func (c *PelicanClient) Ack(ctx context.Context, queue string, deliveryTag uint64) error {
	_, err := c.queue.Ack(ctx, &pbv1.AckRequest{
		Queue:       queue,
		DeliveryTag: deliveryTag,
	})
	if err != nil {
		return fmt.Errorf("pelicanq: ack: %w", err)
	}
	return nil
}

// Nack negatively acknowledges a message, returning it to the queue or
// dead-lettering it if delivery attempts are exhausted.
func (c *PelicanClient) Nack(ctx context.Context, queue string, deliveryTag uint64) error {
	_, err := c.queue.Nack(ctx, &pbv1.NackRequest{
		Queue:       queue,
		DeliveryTag: deliveryTag,
	})
	if err != nil {
		return fmt.Errorf("pelicanq: nack: %w", err)
	}
	return nil
}

// ListQueues lists all queues and their depths.
func (c *PelicanClient) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	resp, err := c.queue.ListQueues(ctx, &pbv1.ListQueuesRequest{})
	if err != nil {
		return nil, fmt.Errorf("pelicanq: list_queues: %w", err)
	}
	infos := make([]QueueInfo, len(resp.Queues))
	for i, q := range resp.Queues {
		infos[i] = queueInfoFromProto(q)
	}
	return infos, nil
}

// Health checks the daemon health. Returns nil if healthy.
func (c *PelicanClient) Health(ctx context.Context) error {
	resp, err := c.admin.Health(ctx, &pbv1.HealthRequest{})
	if err != nil {
		return fmt.Errorf("pelicanq: health: %w", err)
	}
	if resp.Status != "ok" {
		return fmt.Errorf("pelicanq: health: unhealthy: %s", resp.Status)
	}
	return nil
}
