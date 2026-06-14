package pelicanq

import (
	"time"

	pbv1 "github.com/anomalyco/pelicanq/sdks/go/pelicanq/pb/v1"
)

// ClientMessage is a builder for a message to publish.
type ClientMessage struct {
	Payload    []byte
	Headers    map[string]string
	Priority   uint8
	DeliverAt  *int64
	DedupKey   *string
}

// NewMessage creates a new ClientMessage with the given payload.
func NewMessage(payload []byte) ClientMessage {
	return ClientMessage{
		Payload:  payload,
		Headers:  make(map[string]string),
		Priority: 0,
	}
}

// WithPriority sets the delivery priority (0-9, clamped).
func (m ClientMessage) WithPriority(p uint8) ClientMessage {
	if p > 9 {
		p = 9
	}
	m.Priority = p
	return m
}

// WithDeliverAt sets the scheduled delivery time (ms since epoch).
func (m ClientMessage) WithDeliverAt(ms int64) ClientMessage {
	m.DeliverAt = &ms
	return m
}

// WithDedupKey sets the deduplication key.
func (m ClientMessage) WithDedupKey(key string) ClientMessage {
	m.DedupKey = &key
	return m
}

// WithHeader adds a header.
func (m ClientMessage) WithHeader(k, v string) ClientMessage {
	m.Headers[k] = v
	return m
}

// PublishResult is the response from publishing a message.
type PublishResult struct {
	ID          string
	Deduplicated bool
}

// Delivery is a consumed message with its delivery tag.
type Delivery struct {
	DeliveryTag      uint64
	Message          ClientMessage
	ID               string
	Timestamp        int64
	DeliveryAttempts uint32
}

// QueueOptions are optional queue declaration parameters.
type QueueOptions struct {
	MaxAgeSecs          *uint64
	MaxMessages         *uint64
	MaxDeliveryAttempts *uint32
	DeadLetterQueue     *string
	DedupWindowSecs     *uint64
}

// QueueInfo holds information about a queue.
type QueueInfo struct {
	Name           string
	Depth          uint64
	ScheduledDepth uint64
}

// ---------------------------------------------------------------------------
// conversions to/from proto types
// ---------------------------------------------------------------------------

func (m ClientMessage) toProto() *pbv1.Message {
	return &pbv1.Message{
		Id:       "",
		Payload:  m.Payload,
		Headers:  m.Headers,
		Priority: uint32(m.Priority),
		DeliverAt: m.DeliverAt,
		DedupKey:  m.DedupKey,
	}
}

func clientMessageFromProto(msg *pbv1.Message) ClientMessage {
	return ClientMessage{
		Payload:   msg.Payload,
		Headers:   msg.Headers,
		Priority:  uint8(msg.Priority),
		DeliverAt: msg.DeliverAt,
		DedupKey:  msg.DedupKey,
	}
}

func deliveryFromProto(cm *pbv1.ConsumedMessage) Delivery {
	d := Delivery{
		DeliveryTag: cm.DeliveryTag,
	}
	if cm.Message != nil {
		d.Message = clientMessageFromProto(cm.Message)
		d.ID = cm.Message.Id
		d.Timestamp = cm.Message.Timestamp
		d.DeliveryAttempts = cm.Message.DeliveryAttempts
	}
	return d
}

func publishResultFromProto(pr *pbv1.PublishResponse) PublishResult {
	return PublishResult{
		ID:           pr.Id,
		Deduplicated: pr.Deduplicated,
	}
}

func queueInfoFromProto(qi *pbv1.QueueInfo) QueueInfo {
	return QueueInfo{
		Name:           qi.Name,
		Depth:          qi.Depth,
		ScheduledDepth: qi.ScheduledDepth,
	}
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}
