// ─────────────────────────────────────────────────────────────────────────────
// internal/engine/dispatcher.go
//
// Notification Dispatcher — off-loads Kafka publishing from the tick hot path.
//
// Design rationale:
//   The tick processor (processor.go) must run at sub-millisecond speeds.
//   Kafka writes take 1–10ms (network RTT + broker ack). If we wrote directly
//   from processTick(), every margin call event would stall the tick loop and
//   starve other users' PnL updates.
//
//   Solution: the tick processor pushes a lightweight NotificationTask struct
//   (just a few strings + a float64) into a buffered channel. The Dispatcher
//   goroutine reads from that channel at its own pace and calls the Kafka
//   producer. The two goroutines share nothing except the channel — zero lock
//   contention and zero tick-loop latency added.
//
// Channel sizing:
//   Buffer of 500 tasks. In the worst case (mass margin calls during a flash
//   crash), the processor can enqueue 500 warnings before the channel blocks.
//   At typical Kafka write speeds of 1ms/msg, the dispatcher drains 500 tasks
//   in ~0.5s — well within the typical flash crash window. If the channel fills,
//   the processor logs and drops (non-blocking select) rather than stalling.
// ─────────────────────────────────────────────────────────────────────────────

package engine

import (
	"context"
	"log/slog"

	"github.com/livefxhub/risk-service/internal/producer"
)

// NotificationTask carries the minimal data needed for a margin call email.
// It is constructed inside the per-user lock in processTick() and passed
// to the Dispatcher via a buffered channel — zero shared state after the push.
type NotificationTask struct {
	UserID        string
	AccountNumber string
	Email         string
	MarginLevel   float64
}

// NotificationDispatcher reads NotificationTasks from the queue and publishes
// them to Kafka via the KafkaProducer.
type NotificationDispatcher struct {
	queue    chan NotificationTask
	producer *producer.KafkaProducer
}

// NewNotificationDispatcher creates a dispatcher with a buffered task queue.
func NewNotificationDispatcher(p *producer.KafkaProducer) *NotificationDispatcher {
	return &NotificationDispatcher{
		queue:    make(chan NotificationTask, 500),
		producer: p,
	}
}

// Queue returns the send-only channel for the tick processor to push tasks.
// Exposing a directional channel prevents anyone from accidentally reading from it.
func (d *NotificationDispatcher) Queue() chan<- NotificationTask {
	return d.queue
}

// Start launches the background goroutine that drains the task queue.
// Call this once from main() in its own goroutine:
//
//	go dispatcher.Start(ctx)
func (d *NotificationDispatcher) Start(ctx context.Context) {
	slog.Info("notification dispatcher started")
	for {
		select {
		case <-ctx.Done():
			slog.Info("notification dispatcher shutting down")
			return
		case task, ok := <-d.queue:
			if !ok {
				return // channel closed
			}
			d.publish(ctx, task)
		}
	}
}

// publish calls the Kafka producer. Any error is logged but does NOT crash the
// dispatcher — a failed notification email must never stop the risk engine.
func (d *NotificationDispatcher) publish(ctx context.Context, task NotificationTask) {
	if err := d.producer.PublishMarginCall(ctx, task.UserID, task.AccountNumber, task.Email, task.MarginLevel); err != nil {
		slog.Error("failed to publish margin_call notification",
			"user_id", task.UserID,
			"account_number", task.AccountNumber,
			"error", err,
		)
	}
}
