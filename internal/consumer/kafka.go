// ─────────────────────────────────────────────────────────────────────────────
// internal/consumer/kafka.go
//
// Kafka State Consumer — keeps the RAM ledger in sync with trade events.
//
// Topics consumed:
//   orders.executed — a new position was opened by the execution-service.
//   orders.closed   — a position was closed (by user, TP, SL, or force-liquidate).
//
// Why Kafka (not Redis pub/sub) for state events?
//   Kafka provides durable, ordered, replayable events per partition.
//   On risk-service restart, the consumer rewinds to its committed offset and
//   replays all executed/closed events to rebuild the RAM ledger — giving us
//   eventual consistency with the execution-service's state.
//
//   Redis pub/sub is fire-and-forget: any message published while the
//   risk-service is down would be permanently lost.
//
// Concurrency:
//   Two separate reader goroutines per topic run concurrently.
//   Each message handler acquires the minimal required locks (GlobalLedger write
//   lock + user write lock) for the shortest possible critical section.
// ─────────────────────────────────────────────────────────────────────────────

package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/livefxhub/risk-service/internal/logger"
	"go.uber.org/zap"

	kafka "github.com/segmentio/kafka-go"

	"github.com/livefxhub/risk-service/internal/model"
)

// ─────────────────────────────────────────────────────────────────────────────
// Kafka event schemas
// These structs mirror the payloads published by the execution-service.
// They MUST stay in sync with OrderExecutedEvent in execution-service/internal/engine/kafka_producer.go
// ─────────────────────────────────────────────────────────────────────────────

// WalletTransactionEvent is the payload on the wallet.transactions topic.
type WalletTransactionEvent struct {
	UserID          string  `json:"user_id"`
	TransactionType string  `json:"transaction_type"` // "DEPOSIT", "WITHDRAWAL", "CREDIT"
	Amount          float64 `json:"amount"`
}

// OrderExecutedEvent is the payload on the orders.executed topic.
// Published by the execution-service after every successful PlaceOrder.
type OrderExecutedEvent struct {
	TicketID          string    `json:"ticket_id"`
	UserID            string    `json:"user_id"`
	GroupName         string    `json:"group_name"`
	Symbol            string    `json:"symbol"`
	OrderSide         string    `json:"order_side"` // "BUY" or "SELL"
	Volume            float64   `json:"volume,string"`
	ContractSize      float64   `json:"contract_size,string"`
	ExecutionPrice    float64   `json:"execution_price,string"`
	MarginUsed        float64   `json:"margin_used,string"`        // margin delta locked (USD)
	CommissionCharged float64   `json:"commission_charged,string"` // already deducted from Balance
	ClientIP          string    `json:"client_ip"`
	ExecutedAt        time.Time `json:"executed_at"`
}

// OrderClosedEvent is the payload on the orders.closed topic.
// Published by the execution-service after a position is closed (any reason).
type OrderClosedEvent struct {
	TicketID     string    `json:"ticket_id"`
	UserID       string    `json:"user_id"`
	Symbol       string    `json:"symbol"`
	RealizedPnL  float64   `json:"realized_pnl,string"`  // final profit/loss in USD
	MarginReturn float64   `json:"margin_return,string"` // margin being freed (USD)
	Balance      string    `json:"balance"`              // definitive post-trade balance from execution-engine (stringified decimal)
	ClosedAt     time.Time `json:"closed_at"`
}

// ─────────────────────────────────────────────────────────────────────────────
// KafkaConsumer
// ─────────────────────────────────────────────────────────────────────────────

const (
	topicOrdersExecuted  = "orders.executed"
	topicOrdersClosed    = "orders.closed"
	topicWalletTransacts = "wallet.transactions"

	// ContractSizeDefault is used when contract size cannot be inferred from
	// the event. In Phase 1, we default to 100,000 (standard FX lot).
	// Phase 2: load from instrument config or include in Kafka event payload.
	ContractSizeDefault = 100_000.0
)

// KafkaConsumer subscribes to order lifecycle topics and mutates the GlobalLedger.
type KafkaConsumer struct {
	brokers []string
	groupID string
	ledger  *model.GlobalLedger
	loader  model.UserLoader
}

// NewKafkaConsumer creates a consumer that will hydrate the given ledger.
func NewKafkaConsumer(brokers []string, groupID string, ledger *model.GlobalLedger, loader model.UserLoader) *KafkaConsumer {
	return &KafkaConsumer{
		brokers: brokers,
		groupID: groupID,
		ledger:  ledger,
		loader:  loader,
	}
}

// Start launches two background goroutines — one per topic.
// They run until ctx is cancelled (e.g. on SIGTERM).
// Errors are logged but do NOT crash the process — the consumer retries
// the connection automatically through kafka-go's built-in reconnect logic.
func (c *KafkaConsumer) Start(ctx context.Context) {
	go c.consumeLoop(ctx, topicOrdersExecuted, c.handleOrderExecuted)
	go c.consumeLoop(ctx, topicOrdersClosed, c.handleOrderClosed)
	go c.consumeLoop(ctx, topicWalletTransacts, c.handleWalletTransaction)
	logger.Telemetry.Info("kafka consumer started", zap.Strings("brokers", c.brokers), zap.String("group", c.groupID))
}

// consumeLoop is the generic reader loop used by both topics.
// kafka-go's Reader handles offset commits, reconnects, and partition rebalancing.
//
// StartOffset = kafka.LastOffset (NOT FirstOffset):
//
//	The GlobalLedger is pre-populated by the eager DB snapshot at boot.
//	Replaying historical Kafka events would double-count positions, corrupt
//	balances, and trigger false stop-outs. Kafka is now a LIVE-DELTA stream
//	only — we consume events that arrive AFTER boot, not before.
func (c *KafkaConsumer) consumeLoop(ctx context.Context, topic string, handler func([]byte) error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.brokers,
		Topic:          topic,
		GroupID:        c.groupID,
		MinBytes:       1,        // fetch as soon as data is available
		MaxBytes:       10 << 20, // 10 MiB max per fetch batch
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset, // ← live delta only; DB snapshot handles history
	})
	defer reader.Close()

	logger.Telemetry.Info("kafka reader started", zap.String("topic", topic))

	for {
		// FetchMessage blocks until a message is available or ctx is done.
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				logger.Telemetry.Info("kafka reader shutting down", zap.String("topic", topic))
				return
			}
			logger.Error.Error("kafka fetch error", zap.String("topic", topic), zap.Error(err))
			time.Sleep(2 * time.Second) // brief back-off before retrying
			continue
		}

		if err := handler(msg.Value); err != nil {
			// Log and commit anyway — a poison pill should not halt the consumer.
			// In production, route to a dead-letter topic.
			logger.Error.Error("kafka message handler error",
				zap.String("topic", topic),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
		}

		// Commit the offset only AFTER the handler completes.
		// This guarantees at-least-once delivery: if the service crashes mid-handler,
		// the message will be re-delivered on restart.
		if err := reader.CommitMessages(ctx, msg); err != nil {
			logger.Error.Warn("kafka commit failed", zap.String("topic", topic), zap.Error(err))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler: orders.executed
// ─────────────────────────────────────────────────────────────────────────────

// handleOrderExecuted processes a new position event and updates the RAM ledger:
//  1. Get or create the RiskUser (first-ever order for this user).
//  2. Deduct the commission from Balance (already charged by execution-service).
//  3. Add margin to UsedMargin.
//  4. Create a RiskPosition and register it in both Users[id].Positions
//     and the global SymbolIndex.
func (c *KafkaConsumer) handleOrderExecuted(data []byte) error {
	var evt OrderExecutedEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal orders.executed: %w", err)
	}

	if evt.TicketID == "" || evt.UserID == "" || evt.Symbol == "" {
		return fmt.Errorf("orders.executed: missing required fields (ticket=%q user=%q symbol=%q)",
			evt.TicketID, evt.UserID, evt.Symbol)
	}

	// Get or create the RiskUser with the balance BEFORE commission deduction.
	// The commission is deducted in the next step so we can log the before/after.
	// Uses the database loader for JIT hydration.
	user, err := c.ledger.GetOrCreateUser(context.Background(), evt.UserID, 0.0, c.loader)
	if err != nil {
		return fmt.Errorf("failed to jit load user %q: %w", evt.UserID, err)
	}

	// Build the RiskPosition from the event.
	pos := &model.RiskPosition{
		TicketID:     evt.TicketID,
		UserID:       evt.UserID, // back-reference: lets tick processor do O(1) user lookup from SymbolIndex
		Symbol:       evt.Symbol,
		Group:        evt.GroupName, // spread-group: selects correct bid/ask from multi-group tick payload
		OrderType:    evt.OrderSide,
		Volume:       evt.Volume,
		OpenPrice:    evt.ExecutionPrice,
		ContractSize: evt.ContractSize,
		CurrentPnL:   0.0, // starts at zero; updated on first tick
	}

	if pos.ContractSize == 0 {
		pos.ContractSize = ContractSizeDefault // Fallback for older events in queue
	}

	// Acquire both locks in a consistent order to prevent deadlock:
	// always GlobalLedger.mu THEN user.mu — never in reverse.
	c.ledger.Lock() // GlobalLedger write-lock (to mutate SymbolIndex)
	user.Lock()     // per-user write-lock (to mutate Balance, UsedMargin, Positions)

	// Deduct the commission that the execution-service already charged.
	// We reflect this in the risk ledger so Equity stays accurate.
	user.Balance -= evt.CommissionCharged

	// Lock the margin for this position.
	user.UsedMargin += evt.MarginUsed

	// Register position in both lookup structures (shared pointer).
	c.ledger.AddPosition(user, pos)

	user.Unlock()
	c.ledger.Unlock()

	logger.Audit.Info("position opened in risk ledger",
		zap.String("ticket_id", evt.TicketID),
		zap.String("user_id", evt.UserID),
		zap.String("symbol", evt.Symbol),
		zap.String("side", evt.OrderSide),
		zap.Float64("volume", evt.Volume),
		zap.Float64("margin_used", evt.MarginUsed),
	)

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler: orders.closed
// ─────────────────────────────────────────────────────────────────────────────

// handleOrderClosed processes a position closure event:
//  1. Find the user by UserID.
//  2. Remove the position from Users[id].Positions AND SymbolIndex.
//  3. Free UsedMargin.
//  4. Apply realized PnL to Balance.
//  5. Subtract the position's last CurrentPnL from TotalFloatingPnL
//     (since the floating PnL is now realised and reflected in Balance).
func (c *KafkaConsumer) handleOrderClosed(data []byte) error {
	var evt OrderClosedEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal orders.closed: %w", err)
	}

	if evt.TicketID == "" || evt.UserID == "" {
		return fmt.Errorf("orders.closed: missing required fields (ticket=%q user=%q)",
			evt.TicketID, evt.UserID)
	}

	// Look up the user — if not in RAM, the position is unknown to us.
	c.ledger.RLock()
	user, ok := c.ledger.Users[evt.UserID]
	c.ledger.RUnlock()

	if !ok {
		// This can happen if the risk-service was restarted and the Kafka
		// consumer is mid-replay: we may see a close before the open.
		// In this case, there is no RAM state to clean up — log and skip.
		logger.Error.Warn("orders.closed for unknown user (replay gap?)",
			zap.String("ticket_id", evt.TicketID),
			zap.String("user_id", evt.UserID),
		)
		return nil
	}

	// Acquire both locks in the canonical order.
	c.ledger.Lock()
	user.Lock()

	removedPos := c.ledger.RemovePosition(user, evt.TicketID)
	if removedPos != nil {
		// Subtract the cached floating PnL from TotalFloatingPnL — it is now
		// being converted to realized PnL reflected in Balance.
		user.TotalFloatingPnL -= removedPos.CurrentPnL

		// Free the margin that was locked for this position.
		user.UsedMargin -= evt.MarginReturn
		if user.UsedMargin < 0 {
			user.UsedMargin = 0 // guard against float64 underflow
		}

		// Credit the realized PnL to the wallet balance.
		user.Balance += evt.RealizedPnL

		// ── Self-Healing Zeroing + Kafka-driven Balance Sync ─────────────
		// When the portfolio is empty, clamp running totals to absolute zero.
		// Even with perfect race guards, floating-point arithmetic can leave
		// micro-fractions (e.g. 0.000000001) behind. Zeroing ensures the
		// state machine stays pristine.
		//
		// Balance Sync: instead of querying PostgreSQL (which risks loading
		// a stale pre-close balance due to replication lag between the
		// persistence-worker and this consumer), we trust the definitive
		// post-trade balance embedded in the OrderClosedEvent by the
		// execution-engine — the single source of truth.
		if len(user.Positions) == 0 {
			user.UsedMargin = 0
			user.TotalFloatingPnL = 0

			// Sync balance from execution-engine's authoritative value.
			if evt.Balance != "" {
				if parsedBalance, err := strconv.ParseFloat(evt.Balance, 64); err == nil {
					oldBalance := user.Balance
					user.Balance = parsedBalance
					if oldBalance != parsedBalance {
						logger.Audit.Warn("balance drift corrected on portfolio empty",
							zap.String("user_id", evt.UserID),
							zap.Float64("old_balance", oldBalance),
							zap.Float64("new_balance", parsedBalance),
						)
					}
				} else {
					logger.Error.Error("failed to parse balance from OrderClosedEvent",
						zap.String("user_id", evt.UserID),
						zap.String("raw_balance", evt.Balance),
						zap.Error(err),
					)
				}
			}

			// Reset the liquidation debounce flag so the user can be
			// liquidated again if they open new positions.
			if user.IsLiquidating {
				user.IsLiquidating = false
				logger.Audit.Info("liquidation flag reset — all positions closed",
					zap.String("user_id", evt.UserID),
				)
			}
		}
	}

	user.Unlock()
	c.ledger.Unlock()

	logger.Audit.Info("position closed in risk ledger",
		zap.String("ticket_id", evt.TicketID),
		zap.String("user_id", evt.UserID),
		zap.Float64("realized_pnl", evt.RealizedPnL),
		zap.Float64("margin_freed", evt.MarginReturn),
	)

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler: wallet.transactions
// ─────────────────────────────────────────────────────────────────────────────

func (c *KafkaConsumer) handleWalletTransaction(data []byte) error {
	var evt WalletTransactionEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal wallet.transactions: %w", err)
	}

	// Look up the user — if not in RAM, they are safe to ignore.
	// The JIT loader will fetch their post-transaction balance perfectly
	// next time they open a position.
	c.ledger.RLock()
	user, ok := c.ledger.Users[evt.UserID]
	c.ledger.RUnlock()

	if !ok {
		return nil // User not in RAM. Safely ignore!
	}

	// Lock the specific user's state to mutate balance
	user.Lock()
	defer user.Unlock()

	switch evt.TransactionType {
	case "DEPOSIT", "CREDIT":
		user.Balance += evt.Amount
		logger.Audit.Info("wallet credited in RAM", zap.String("user_id", evt.UserID), zap.Float64("amount", evt.Amount), zap.Float64("new_balance", user.Balance))
	case "WITHDRAWAL":
		user.Balance -= evt.Amount
		logger.Audit.Info("wallet debited in RAM", zap.String("user_id", evt.UserID), zap.Float64("amount", evt.Amount), zap.Float64("new_balance", user.Balance))
	default:
		logger.Error.Warn("unknown wallet transaction type", zap.String("type", evt.TransactionType))
	}

	return nil
}
