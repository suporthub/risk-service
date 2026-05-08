// ─────────────────────────────────────────────────────────────────────────────
// cmd/server/main.go
//
// Risk Service — Process Entry Point
//
// Boot sequence (STRICT ORDER — no goroutine may start before its predecessor):
//
//   Step 1 — Eager DB Snapshot  (synchronous, blocks until complete)
//     Connect to user_db + order_db.
//     Call LoadAllActiveRisk() → query all OPEN positions + wallet balances.
//     Call ledger.HydrateFromSnapshot() → populate the GlobalLedger.
//     The ledger is now a consistent point-in-time image of the DB.
//     ✅ No goroutine has started yet. No risk of evaluating partial state.
//
//   Step 2 — Kafka Live-Delta Consumer  (goroutines launched)
//     Start consuming orders.executed, orders.closed, wallet.transactions
//     from kafka.LastOffset (NOT FirstOffset — history is already in the DB).
//     New trades after boot are applied as incremental deltas to the ledger.
//
//   Step 3 — Redis Tick Stream  (goroutines launched)
//     Subscribe to the tick:* Redis pattern.
//     Start the Tick Processor and gRPC Dispatcher.
//     The tick processor now evaluates a fully-hydrated ledger — zero false
//     stop-outs from missing positions.
//
// Shutdown sequence (SIGTERM received):
//   1. Cancel root context → all goroutines drain and exit
//   2. Redis subscriber closes pub/sub connection
//   3. Kafka consumer commits current offset and closes readers
//   4. Processor drains in-flight ticks
//   5. Dispatcher finishes any in-flight ForceLiquidate gRPC call
//   6. DB pools close, process exits
// ─────────────────────────────────────────────────────────────────────────────

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/livefxhub/risk-service/internal/config"
	"github.com/livefxhub/risk-service/internal/consumer"
	"github.com/livefxhub/risk-service/internal/db"
	"github.com/livefxhub/risk-service/internal/engine"
	grpcDispatcher "github.com/livefxhub/risk-service/internal/grpc"
	"github.com/livefxhub/risk-service/internal/model"
	"github.com/livefxhub/risk-service/internal/producer"
	redisSubscriber "github.com/livefxhub/risk-service/internal/redis"
)

func main() {
	// ── Structured logger ─────────────────────────────────────────────────────
	logFile, err := os.OpenFile("/app/logs/risk-service.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	var logWriter io.Writer = os.Stdout
	if err == nil {
		logWriter = io.MultiWriter(os.Stdout, logFile)
	}

	var logHandler slog.Handler = slog.NewTextHandler(logWriter, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	if os.Getenv("ENV") == "production" {
		logHandler = slog.NewJSONHandler(logWriter, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	}
	slog.SetDefault(slog.New(logHandler))
	slog.Info("risk-service starting", "pid", os.Getpid())

	// ── Load config ───────────────────────────────────────────────────────────
	cfg := config.Load()

	// ── Root cancellable context ──────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 1: Eager DB Snapshot — populate RAM before any goroutine starts.
	//
	// The ledger MUST be fully hydrated before the tick processor and Kafka
	// consumer goroutines are launched. If we start the tick processor first,
	// it can evaluate margin levels against an empty ledger and fire false
	// stop-outs on users who already have open positions.
	// ─────────────────────────────────────────────────────────────────────────
	slog.Info("boot step 1/3: eager DB snapshot starting",
		"user_db",  cfg.DatabaseURL,
		"order_db", cfg.OrderDatabaseURL,
	)

	dbLoader, err := db.NewLoader(ctx, cfg.DatabaseURL, cfg.OrderDatabaseURL)
	if err != nil {
		slog.Error("boot step 1/3: failed to connect to databases", "error", err)
		os.Exit(1)
	}
	defer dbLoader.Close()

	// LoadAllActiveRisk blocks until both queries complete (30s timeout).
	// Fatal on failure — a partially-hydrated ledger is worse than no service.
	snapshot, err := dbLoader.LoadAllActiveRisk(ctx)
	if err != nil {
		slog.Error("boot step 1/3: eager snapshot failed", "error", err)
		os.Exit(1)
	}

	// Construct the GlobalLedger and hydrate it from the snapshot.
	// HydrateFromSnapshot is synchronous and holds the global write-lock
	// for its entire duration (safe: no goroutines are running yet).
	ledger := model.NewGlobalLedger()

	// Convert db.SnapshotPosition → model.SnapshotEntry (avoids import cycle).
	entries := make([]model.SnapshotEntry, len(snapshot.Positions))
	for i, sp := range snapshot.Positions {
		entries[i] = model.SnapshotEntry{
			TicketID:     sp.TicketID,
			UserID:       sp.UserID,
			Symbol:       sp.Symbol,
			GroupName:    sp.GroupName,
			OrderSide:    sp.OrderSide,
			Volume:       sp.Volume,
			OpenPrice:    sp.OpenPrice,
			ContractSize: sp.ContractSize,
			MarginUsed:   sp.MarginUsed,
		}
	}

	hydrated := ledger.HydrateFromSnapshot(entries, snapshot.Balances, snapshot.Emails, snapshot.AccountNumbers)
	slog.Info("boot step 1/3: ledger hydrated ✅",
		"users",     hydrated.Users,
		"positions", hydrated.Positions,
	)

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 2: Kafka Live-Delta Consumer.
	//
	// Now that the ledger contains the DB snapshot, Kafka is wired as a
	// live-delta stream starting from kafka.LastOffset. New trades placed
	// AFTER boot are applied as incremental mutations.
	//
	// The consumer still performs JIT loads for brand-new users (users with
	// no positions open AT boot time who place their first order post-boot).
	// ─────────────────────────────────────────────────────────────────────────
	slog.Info("boot step 2/3: starting Kafka live-delta consumer",
		"topics", strings.Join([]string{"orders.executed", "orders.closed", "wallet.transactions"}, ", "),
		"offset", "latest",
	)

	kafkaConsumer := consumer.NewKafkaConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaGroupID,
		ledger,
		dbLoader.LoadUserDetails, // JIT loader for new post-boot users only
	)
	kafkaConsumer.Start(ctx)
	slog.Info("boot step 2/3: Kafka live-delta consumer started ✅")

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 3: Redis Tick Stream + Risk Engine.
	//
	// The tick processor now evaluates a fully-hydrated ledger. Starting the
	// tick stream AFTER Kafka (not before) also ensures that any orders placed
	// in the narrow window between the DB snapshot and the Kafka consumer
	// reaching LastOffset are already applied before the first tick fires.
	// ─────────────────────────────────────────────────────────────────────────
	slog.Info("boot step 3/3: starting Redis tick stream and risk engine")

	sub := redisSubscriber.NewSubscriber(cfg.RedisNodes, cfg.RedisPassword)
	defer sub.Close()
	go sub.Start(ctx)

	// Notification producer + dispatcher (non-blocking margin-call events).
	notifProducer := producer.NewKafkaProducer(cfg.KafkaBrokers)
	defer func() {
		if err := notifProducer.Close(); err != nil {
			slog.Warn("notification producer close error", "error", err)
		}
	}()
	notifDispatcher := engine.NewNotificationDispatcher(notifProducer)

	// Tick processor: evaluates PnL delta + margin level on every tick.
	fxConverter := engine.NewFxConverter()
	proc := engine.NewProcessor(ledger, cfg, fxConverter, notifDispatcher.Queue())

	// gRPC Dispatcher: fires ForceLiquidate RPCs when stop-out is triggered.
	dispatcher, err := grpcDispatcher.NewDispatcher(cfg.ExecutionGRPCAddr, proc.LiquidationCh)
	if err != nil {
		slog.Error("boot step 3/3: failed to connect to execution-service gRPC",
			"addr",  cfg.ExecutionGRPCAddr,
			"error", err,
		)
		os.Exit(1)
	}

	go proc.Start(ctx, sub.TickCh)
	go dispatcher.Start(ctx)
	go notifDispatcher.Start(ctx)

	slog.Info("boot step 3/3: risk engine fully operational ✅",
		"stop_out_pct",    cfg.StopOutPct,
		"margin_call_pct", cfg.MarginCallPct,
	)

	// ── Wait for OS shutdown signal ───────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	slog.Info("shutdown signal received", "signal", sig.String())

	cancel() // propagates to all goroutines
	slog.Info("risk-service shutdown complete")
}

