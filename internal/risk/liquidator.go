package risk

import (
	"context"
	"time"

	"github.com/google/uuid"
	pb "github.com/livefxhub/risk-service/gen/executionpb"
	"github.com/livefxhub/risk-service/internal/config"
	"github.com/livefxhub/risk-service/internal/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

// CheckMarginRequirements acts as the margin watchdog, invoking actions if thresholds break
func CheckMarginRequirements(userID string, marginLevel float64, execClient pb.ExecutionServiceClient) {
	// Dynamically retrieve protected RAM bounds instead of hardcoded globals
	params := config.GetParams()

	// ----------------------------------------------------------------------
	// WARNING LOGIC
	// ----------------------------------------------------------------------
	if marginLevel <= params.WarningThreshold && marginLevel > params.StopOutThreshold {
		traceID := "sys-warn-" + uuid.New().String()
		reqLogger := logger.Audit.With(zap.String("trace_id", traceID), zap.String("user_id", userID))
		reqLogger.Info("Margin level breached warning threshold",
			zap.Float64("margin_level", marginLevel),
			zap.Float64("warning_threshold", params.WarningThreshold),
		)
	}

	// ----------------------------------------------------------------------
	// LIQUIDATION LOGIC
	// ----------------------------------------------------------------------
	if marginLevel <= params.StopOutThreshold {
		traceID := "sys-liq-" + uuid.New().String()
		reqLogger := logger.Audit.With(zap.String("trace_id", traceID), zap.String("user_id", userID))

		reqLogger.Warn("Margin level breached stop-out threshold, initiating Force Liquidation",
			zap.Float64("margin_level", marginLevel),
			zap.Float64("stop_out_threshold", params.StopOutThreshold),
		)

		req := &pb.ForceLiquidateRequest{
			UserId: userID,
			Reason: "MARGIN_STOP_OUT",
		}

		// Fail-fast timeout architecture ensures the risk loop never gets globally blocked
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		// CRITICAL: Bind the trace ID into gRPC metadata for the execution-service to intercept!
		ctx = metadata.AppendToOutgoingContext(ctx, "x-trace-id", traceID)

		// Fire liquidation via GRPC natively over the internal high-speed UDS/TCP backbone
		_, err := execClient.ForceLiquidate(ctx, req)
		if err != nil {
			reqLogger.Error("Failed to fire Force Liquidation over gRPC", zap.Error(err))
		} else {
			reqLogger.Info("Force Liquidation successfully pushed to execution-service")
		}
	}
}
