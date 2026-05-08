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
	EventID   string                 `json:"eventId"`            // UUID v4 — deduplication key
	Channel   string                 `json:"channel"`            // "email" | "push" | "sms"
	Template  string                 `json:"template"`           // must match NotificationTemplate enum
	Priority  string                 `json:"priority"`           // "high" | "normal" | "low"
	Recipient string                 `json:"recipient"`          // email address for "email" channel
	Data      map[string]interface{} `json:"data"`               // template-specific payload fields
	UserID    string                 `json:"userId,omitempty"`   // for preference checks + audit
	UserType  string                 `json:"userType,omitempty"` // "live" | "demo" | "admin"
	CreatedAt string                 `json:"createdAt"`          // ISO 8601 — used for staleness guard (5-min window)
}

// RiskNotificationTask is a self-contained payload for any risk-engine notification.
// It decouples the producer package from the engine package (no circular imports).
// The engine package creates these and passes them to the dispatcher, which calls
// the producer with this struct directly.
type RiskNotificationTask struct {
	Template      string  // "margin_call" | "auto_cutoff"
	UserID        string
	AccountNumber string
	Email         string
	MarginLevel   float64
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

// buildData constructs the template-specific data map.
// Keys map directly to the string-interpolation variables in email.templates.ts.
func buildData(task RiskNotificationTask) map[string]interface{} {
	switch task.Template {
	case "auto_cutoff":
		return map[string]interface{}{
			"accountNumber": task.AccountNumber,
			"marginLevel":   fmt.Sprintf("%.2f", task.MarginLevel),
			"closedAt":      time.Now().UTC().Format(time.RFC1123),
		}
	default: // "margin_call"
		return map[string]interface{}{
			"accountNumber": task.AccountNumber,
			"marginLevel":   fmt.Sprintf("%.2f", task.MarginLevel),
		}
	}
}

// PublishNotification is the single entry-point for all risk-engine notification emails.
// It builds the correct data payload for the template and writes it to Kafka.
//
// Blocking call — returns an error if the write fails.
// The caller (NotificationDispatcher) logs and continues; a failed notification
// email is never fatal to the risk engine.
func (p *KafkaProducer) PublishNotification(ctx context.Context, task RiskNotificationTask) error {
	evt := NotificationEvent{
		EventID:   uuid.New().String(),
		Channel:   "email",
		Template:  task.Template,
		Priority:  "high", // both margin_call and auto_cutoff are high-priority
		Recipient: task.Email,
		Data:      buildData(task),
		UserID:    task.UserID,
		UserType:  "live",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal %s event: %w", task.Template, err)
	}

	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(task.UserID), // key by userID → same partition → ordered per user
		Value: payload,
	}); err != nil {
		return fmt.Errorf("write %s to kafka: %w", task.Template, err)
	}

	slog.Info("risk notification published",
		"template",       task.Template,
		"user_id",        task.UserID,
		"account_number", task.AccountNumber,
		"margin_level",   fmt.Sprintf("%.2f%%", task.MarginLevel),
	)
	return nil
}
