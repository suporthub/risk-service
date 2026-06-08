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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/livefxhub/risk-service/internal/config"
	"github.com/livefxhub/risk-service/internal/consumer"
	"github.com/livefxhub/risk-service/internal/db"
	"github.com/livefxhub/risk-service/internal/engine"
	grpcDispatcher "github.com/livefxhub/risk-service/internal/grpc"
	"github.com/livefxhub/risk-service/internal/logger"
	"github.com/livefxhub/risk-service/internal/model"
	"github.com/livefxhub/risk-service/internal/producer"
	redisSubscriber "github.com/livefxhub/risk-service/internal/redis"
)

func main() {
	// ── Load .env ─────────────────────────────────────────────────────────────
	_ = godotenv.Load()

	// ── Structured logger ─────────────────────────────────────────────────────
	logger.Init()
	defer logger.Sync()
	logger.Telemetry.Info("risk-service starting", zap.Int("pid", os.Getpid()))

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
	logger.Telemetry.Info("boot step 1/3: eager DB snapshot starting",
		zap.String("user_db", cfg.DatabaseURL),
		zap.String("order_db", cfg.OrderDatabaseURL),
	)

	dbLoader, err := db.NewLoader(ctx, cfg.DatabaseURL, cfg.OrderDatabaseURL)
	if err != nil {
		logger.Error.Error("boot step 1/3: failed to connect to databases", zap.Error(err))
		os.Exit(1)
	}
	defer dbLoader.Close()

	// LoadAllActiveRisk blocks until both queries complete (30s timeout).
	// Fatal on failure — a partially-hydrated ledger is worse than no service.
	snapshot, err := dbLoader.LoadAllActiveRisk(ctx)
	if err != nil {
		logger.Error.Error("boot step 1/3: eager snapshot failed", zap.Error(err))
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
			TicketID:      sp.TicketID,
			UserID:        sp.UserID,
			Symbol:        sp.Symbol,
			GroupName:     sp.GroupName,
			OrderSide:     sp.OrderSide,
			Volume:        sp.Volume,
			OpenPrice:     sp.OpenPrice,
			ContractValue: sp.ContractValue,
			MarginUsed:    sp.MarginUsed,
		}
	}

	hydrated := ledger.HydrateFromSnapshot(entries, snapshot.Balances, snapshot.Emails, snapshot.AccountNumbers)
	logger.Telemetry.Info("boot step 1/3: ledger hydrated ✅",
		zap.Int("users", hydrated.Users),
		zap.Int("positions", hydrated.Positions),
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
	logger.Telemetry.Info("boot step 2/3: starting Kafka live-delta consumer",
		zap.String("topics", strings.Join([]string{"orders.executed", "orders.closed", "wallet.transactions"}, ", ")),
		zap.String("offset", "latest"),
	)

	kafkaConsumer := consumer.NewKafkaConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaGroupID,
		ledger,
		dbLoader.LoadUserDetails, // JIT loader for new post-boot users only
	)
	kafkaConsumer.Start(ctx)
	logger.Telemetry.Info("boot step 2/3: Kafka live-delta consumer started ✅")

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 3: Redis Tick Stream + Risk Engine.
	//
	// The tick processor now evaluates a fully-hydrated ledger. Starting the
	// tick stream AFTER Kafka (not before) also ensures that any orders placed
	// in the narrow window between the DB snapshot and the Kafka consumer
	// reaching LastOffset are already applied before the first tick fires.
	// ─────────────────────────────────────────────────────────────────────────
	logger.Telemetry.Info("boot step 3/3: starting Redis tick stream and risk engine")

	sub := redisSubscriber.NewSubscriber(cfg.RedisNodes, cfg.RedisPassword)
	defer sub.Close()
	go sub.Start(ctx)

	// Notification producer + dispatcher (non-blocking margin-call events).
	notifProducer := producer.NewKafkaProducer(cfg.KafkaBrokers)
	defer func() {
		if err := notifProducer.Close(); err != nil {
			logger.Error.Warn("notification producer close error", zap.Error(err))
		}
	}()
	notifDispatcher := engine.NewNotificationDispatcher(notifProducer)

	// Connect Redis Client for caching (Cooldowns)
	redisClient := redisSubscriber.NewClusterClient(cfg.RedisNodes, cfg.RedisPassword, 10, 5000*time.Millisecond)

	// Tick processor: evaluates PnL delta + margin level on every tick.
	fxConverter := engine.NewFxConverter()
	proc := engine.NewProcessor(ledger, cfg, fxConverter, notifDispatcher.Queue(), redisClient)

	// gRPC Dispatcher: fires ForceLiquidate RPCs when stop-out is triggered.
	dispatcher, err := grpcDispatcher.NewDispatcher(cfg.ExecutionGRPCAddr, proc.LiquidationCh)
	if err != nil {
		logger.Error.Error("boot step 3/3: failed to connect to execution-service gRPC",
			zap.String("addr", cfg.ExecutionGRPCAddr),
			zap.Error(err),
		)
		os.Exit(1)
	}

	go proc.Start(ctx, sub.TickCh)
	go dispatcher.Start(ctx)
	go notifDispatcher.Start(ctx)

	logger.Telemetry.Info("boot step 3/3: risk engine fully operational ✅",
		zap.Float64("stop_out_pct", cfg.StopOutPct),
		zap.Float64("margin_call_pct", cfg.MarginCallPct),
	)

	// ── Metrics HTTP server ──────────────────────────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	metricsSrv := &http.Server{
		Addr:         cfg.MetricsAddr,
		Handler:      metricsMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		logger.Telemetry.Info("metrics server listening", zap.String("addr", cfg.MetricsAddr))
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error.Error("metrics server error", zap.Error(err))
		}
	}()

	// ── Wait for OS shutdown signal ───────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	logger.Telemetry.Info("shutdown signal received", zap.String("signal", sig.String()))

	cancel() // propagates to all goroutines

	// Graceful metrics server shutdown
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	if err := metricsSrv.Shutdown(shutCtx); err != nil {
		logger.Error.Warn("metrics server shutdown error", zap.Error(err))
	}

	logger.Telemetry.Info("risk-service shutdown complete")
}
