// ─────────────────────────────────────────────────────────────────────────────
// cmd/server/main.go
//
// Risk Service — Process Entry Point
//
// Boot sequence:
//   1. Load config from .env / OS environment
//   2. Construct the GlobalLedger (empty in-RAM state store)
//   3. Start the Kafka consumer (begins replaying orders.executed + orders.closed
//      to hydrate the ledger from committed offset)
//   4. Connect the Redis tick subscriber (pattern: tick:*)
//   5. Build the Tick Processor (reads from Redis channel, evaluates risk)
//   6. Connect the gRPC Dispatcher to the execution-service
//   7. Launch all goroutines under a shared cancellable context
//   8. Block on OS signal (SIGINT / SIGTERM) for graceful shutdown
//
// Shutdown sequence (SIGTERM received):
//   1. Cancel the root context → all goroutines stop reading from their channels
//   2. Redis subscriber closes its pub/sub connection
//   3. Kafka consumer commits its offset and closes readers
//   4. Processor drains any in-flight ticks (context cancelled)
//   5. Dispatcher finishes in-flight ForceLiquidate gRPC call (if any)
//   6. Process exits cleanly
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
	"github.com/livefxhub/risk-service/internal/engine"
	grpcDispatcher "github.com/livefxhub/risk-service/internal/grpc"
	"github.com/livefxhub/risk-service/internal/model"
	redisSubscriber "github.com/livefxhub/risk-service/internal/redis"
)

func main() {
	// ── Structured logger ────────────────────────────────────────────────────
	// Use JSON handler in production; text handler is easier to read locally.
	// Detect by env var — same pattern used by execution-service.
	// Declared as slog.Handler interface so both TextHandler and JSONHandler
	// can be assigned without a type mismatch.
	// Multi-writer to log to both stdout and a persistent file
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

	// ── Step 1: Load config ──────────────────────────────────────────────────
	// Panics on missing required env vars — fail fast, fail loud.
	cfg := config.Load()

	// ── Step 2: Build the Global Ledger ─────────────────────────────────────
	// The ledger is the single source of truth for all in-RAM risk state.
	// It starts empty; the Kafka consumer hydrates it from committed offsets.
	ledger := model.NewGlobalLedger()
	slog.Info("global ledger initialised")

	// ── Step 3: Root context for cooperative shutdown ────────────────────────
	// All subsystems respect context cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── Step 4: Start Kafka consumer ─────────────────────────────────────────
	// Subscribes to orders.executed and orders.closed.
	// Replays from the last committed offset to rebuild the ledger after restart.
	kafkaConsumer := consumer.NewKafkaConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaGroupID,
		ledger,
	)
	kafkaConsumer.Start(ctx)
	slog.Info("kafka consumer started",
		"topics", strings.Join([]string{"orders.executed", "orders.closed"}, ", "),
	)

	// ── Step 5: Connect Redis tick subscriber ────────────────────────────────
	// Subscribes to pattern tick:* — receives all symbol ticks from pricing-service.
	// Ticks are pushed into sub.TickCh (buffered channel, 50k cap).
	sub := redisSubscriber.NewSubscriber(cfg.RedisNodes, cfg.RedisPassword)
	defer sub.Close()

	go sub.Start(ctx)
	slog.Info("redis tick subscriber started", "nodes", cfg.RedisNodes)

	// ── Step 6: Build the Tick Processor ────────────────────────────────────
	// Reads from sub.TickCh, evaluates PnL delta + margin level for each tick.
	// Pushes LiquidationTask to proc.LiquidationCh when stop-out threshold hit.
	fxConverter := engine.NewFxConverter()
	proc := engine.NewProcessor(ledger, cfg, fxConverter)

	// ── Step 7: Connect gRPC Dispatcher to execution-service ────────────────
	// The dispatcher is the only component that makes outbound network calls.
	// It reads from proc.LiquidationCh and fires ForceLiquidate RPCs.
	dispatcher, err := grpcDispatcher.NewDispatcher(cfg.ExecutionGRPCAddr, proc.LiquidationCh)
	if err != nil {
		slog.Error("failed to connect to execution-service gRPC",
			"addr",  cfg.ExecutionGRPCAddr,
			"error", err,
		)
		os.Exit(1)
	}
	slog.Info("grpc dispatcher connected", "execution_addr", cfg.ExecutionGRPCAddr)

	// ── Step 8: Launch goroutines ────────────────────────────────────────────
	//
	// Goroutine layout:
	//
	//   [Redis PSubscribe loop]          → sub.TickCh (chan Tick, cap 50k)
	//   [Tick Processor]    ← TickCh    → proc.LiquidationCh (chan Task, cap 1k)
	//   [gRPC Dispatcher]   ← LiquidationCh → execution-service ForceLiquidate RPC
	//   [Kafka Consumer ×2] → GlobalLedger mutations (orders.executed, orders.closed)
	//
	// All goroutines terminate when ctx is cancelled (SIGTERM path).

	go proc.Start(ctx, sub.TickCh)
	go dispatcher.Start(ctx)

	slog.Info("risk-service fully operational",
		"stop_out_pct",    cfg.StopOutPct,
		"margin_call_pct", cfg.MarginCallPct,
	)

	// ── Step 9: Wait for OS shutdown signal ─────────────────────────────────
	// SIGINT  = Ctrl+C (developer)
	// SIGTERM = Kubernetes pod eviction / docker stop
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	slog.Info("shutdown signal received", "signal", sig.String())

	// Cancel the root context — all goroutines will drain and exit.
	cancel()

	slog.Info("risk-service shutdown complete")
}
