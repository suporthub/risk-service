// ─────────────────────────────────────────────────────────────────────────────
// internal/grpc/dispatcher.go
//
// gRPC Liquidation Dispatcher
//
// This is the risk-service's gRPC CLIENT to the execution-service.
// It runs as a single background goroutine that reads LiquidationTask values
// from the processor's channel and fires ForceLiquidate RPCs immediately.
//
// Architecture:
//
//   Tick Processor → LiquidationCh (buffered) → Dispatcher goroutine → gRPC → Execution Service
//
// Why a separate goroutine?
//   The tick processor hot path must NEVER block on network I/O.
//   A gRPC call can take 1–50ms (LAN round-trip + execution engine processing).
//   The dispatcher decouples the network call from the tick evaluation loop:
//   the processor does a <1µs channel push and moves on immediately.
//
// Connection resilience:
//   The gRPC connection is established once at startup with WithBlock() so the
//   process fails fast if the execution-service is unreachable at boot.
//   For per-call timeouts, each ForceLiquidate call uses a 5-second context —
//   long enough for the execution-service to close the position, short enough
//   to detect hangs quickly.
//
// Idempotency:
//   If a stop-out is triggered on two consecutive ticks before the first
//   ForceLiquidate completes, two tasks will be queued. The execution-service
//   must handle duplicate ForceLiquidate calls gracefully (position already
//   closed → return success=true, message="already closed").
// ─────────────────────────────────────────────────────────────────────────────

package grpc

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/livefxhub/risk-service/internal/engine"
	executionpb "github.com/livefxhub/risk-service/gen/executionpb"
)

// Dispatcher holds the gRPC client connection and the LiquidationTask channel.
type Dispatcher struct {
	client executionpb.ExecutionServiceClient
	taskCh <-chan engine.LiquidationTask
}

// NewDispatcher dials the execution-service gRPC endpoint and returns a Dispatcher.
//
// addr: host:port of the execution-service (from EXECUTION_GRPC_ADDR env var).
// taskCh: the LiquidationCh from the Processor — this goroutine is the sole reader.
//
// Panics if the initial dial fails — a risk-service that cannot reach the
// execution-service cannot perform its primary function (liquidating positions).
func NewDispatcher(addr string, taskCh <-chan engine.LiquidationTask) (*Dispatcher, error) {
	// Dial with a 10-second timeout at startup. In production, use mTLS credentials
	// instead of insecure — replace insecure.NewCredentials() with your TLS config.
	dialCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		dialCtx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // fail fast at startup if execution-service is unreachable
	)
	if err != nil {
		return nil, err
	}

	slog.Info("gRPC dispatcher connected to execution-service", "addr", addr)

	return &Dispatcher{
		client: executionpb.NewExecutionServiceClient(conn),
		taskCh: taskCh,
	}, nil
}

// Start is the background goroutine entry point. It reads from the task channel
// and fires ForceLiquidate gRPC calls sequentially.
//
// Sequential dispatch is intentional for Phase 1:
//   - Stop-out events are rare (< 1 per second in normal market conditions).
//   - Sequential dispatch avoids thundering-herd on the execution-service.
//   - If peak stop-out rates exceed 10/second, upgrade to a worker pool
//     (e.g. 4 goroutines each reading from the same channel).
//
// Call this from main():
//   go dispatcher.Start(ctx)
func (d *Dispatcher) Start(ctx context.Context) {
	slog.Info("liquidation dispatcher started")

	for {
		select {
		case <-ctx.Done():
			slog.Info("liquidation dispatcher shutting down")
			return

		case task, ok := <-d.taskCh:
			if !ok {
				slog.Info("liquidation channel closed — dispatcher exiting")
				return
			}
			d.dispatch(task)
		}
	}
}

// dispatch fires a single ForceLiquidate gRPC call for the given task.
// It is synchronous from the dispatcher's perspective — the goroutine blocks
// here until the execution-service responds or the timeout expires.
//
// This is intentional: we want confirmation that the position was closed
// before processing the next stop-out event. If the RPC fails, we log the
// error — the NEXT tick will re-trigger the stop-out check and re-queue the task.
func (d *Dispatcher) dispatch(task engine.LiquidationTask) {
	// 5-second deadline per liquidation call.
	// The execution-service should close a position in <10ms on the warm path.
	// 5 seconds provides generous headroom for GC pauses and broker reconnects.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &executionpb.ForceLiquidateRequest{
		TicketId: task.TicketID,
		UserId:   task.UserID,
		Reason:   task.Reason,
	}

	slog.Info("dispatching force-liquidate",
		"ticket_id", task.TicketID,
		"user_id",   task.UserID,
		"reason",    task.Reason,
	)

	resp, err := d.client.ForceLiquidate(ctx, req)
	if err != nil {
		// Network or timeout error. The position remains open.
		// The next tick will re-trigger the stop-out check.
		slog.Error("ForceLiquidate gRPC call failed",
			"ticket_id", task.TicketID,
			"user_id",   task.UserID,
			"error",     err,
		)
		return
	}

	if !resp.Success {
		// Execution-service declined (e.g. position already closed by the user
		// between the stop-out trigger and our RPC arriving). Log and move on.
		slog.Warn("ForceLiquidate returned success=false",
			"ticket_id", task.TicketID,
			"user_id",   task.UserID,
			"message",   resp.Message,
		)
		return
	}

	slog.Info("position force-liquidated successfully",
		"ticket_id", task.TicketID,
		"user_id",   task.UserID,
		"message",   resp.Message,
	)
}
