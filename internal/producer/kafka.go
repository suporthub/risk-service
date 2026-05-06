// ─────────────────────────────────────────────────────────────────────────────
// internal/producer/kafka.go
//
// Kafka Producer for the risk-service.
//
// Publishes notification events to the `notification.send` topic consumed by
// the notification-service. This is a write-only, fire-and-forget producer —
// we do not wait for acks beyond what kafka-go's synchronous Write provides.
//
// The payload schema MUST match the zod schema in:
//   notification-service/src/lib/kafka.ts (notificationEventSchema)
// ─────────────────────────────────────────────────────────────────────────────

package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

const topicNotificationSend = "notification.send"

// NotificationEvent is the payload written to the notification.send topic.
// Schema is validated by the notification-service's zod schema — all field
// names and enum values MUST match exactly.
type NotificationEvent struct {
	EventID   string                 `json:"eventId"`           // UUID v4 — deduplication key
	Channel   string                 `json:"channel"`           // "email" | "push" | "sms"
	Template  string                 `json:"template"`          // must match NotificationTemplate enum
	Priority  string                 `json:"priority"`          // "high" | "normal" | "low"
	Recipient string                 `json:"recipient"`         // email address for "email" channel
	Data      map[string]interface{} `json:"data"`              // template-specific payload fields
	UserID    string                 `json:"userId,omitempty"`  // for preference checks + audit
	UserType  string                 `json:"userType,omitempty"` // "live" | "demo" | "admin"
	CreatedAt string                 `json:"createdAt"`         // ISO 8601 — used for staleness guard (5-min window)
}

// KafkaProducer writes notification events to Kafka.
type KafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer creates a producer connected to the given brokers.
func NewKafkaProducer(brokers []string) *KafkaProducer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topicNotificationSend,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		// RequiredAcks=1: leader-only ack — fast and safe enough for notifications.
		// (Acks=-1/All is reserved for financial writes like orders.executed.)
		RequiredAcks: kafka.RequireOne,
		Async:        false, // synchronous: we want to know if the write failed
	}
	return &KafkaProducer{writer: w}
}

// Close shuts down the writer gracefully. Call during service shutdown.
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

// PublishMarginCall publishes a margin_call notification event for the given user.
// It generates a fresh UUID eventId for deduplication in the notification-service.
//
// Blocking call — returns an error if the write fails after kafka-go's internal retries.
// The caller (Dispatcher) logs the error and continues; a failed notification
// is not fatal to the risk engine.
func (p *KafkaProducer) PublishMarginCall(ctx context.Context, userID, accountNumber, email string, marginLevel float64) error {
	evt := NotificationEvent{
		EventID:   uuid.New().String(),
		Channel:   "email",
		Template:  "margin_call",
		Priority:  "high",
		Recipient: email,
		Data: map[string]interface{}{
			"accountNumber": accountNumber,
			"marginLevel":   fmt.Sprintf("%.2f", marginLevel),
		},
		UserID:    userID,
		UserType:  "live",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal margin_call event: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(userID), // key by userID → same partition → ordered per user
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("write margin_call to kafka: %w", err)
	}

	slog.Info("margin_call notification published",
		"user_id", userID,
		"account_number", accountNumber,
		"margin_level", fmt.Sprintf("%.2f%%", marginLevel),
	)
	return nil
}
