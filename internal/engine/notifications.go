package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/livefxhub/risk-service/internal/logger"
	"go.uber.org/zap"

	"github.com/livefxhub/risk-service/internal/model"
)

// SendMarginWarning uses Redis SetNX to implement a 24-hour cooldown for margin warnings.
// It returns true if a warning was successfully published (or queued), false if on cooldown or error.
func (p *Processor) SendMarginWarning(ctx context.Context, user *model.RiskUser, marginLevel float64) bool {
	key := fmt.Sprintf("cooldown:margin_warning:%s", user.UserID)

	// SetNX (Set if Not Exists)
	acquired, err := p.redisClient.SetNX(ctx, key, marginLevel, 24*time.Hour).Result()
	if err != nil {
		logger.Error.Error("failed to set margin_warning cooldown", zap.Error(err), zap.String("user_id", user.UserID))
		return false
	}

	if !acquired {
		// Cooldown is active; exit silently.
		return false
	}

	select {
	case p.notificationQueue <- NotificationTask{
		Template:      "margin_call",
		UserID:        user.UserID,
		AccountNumber: user.AccountNumber,
		Email:         user.Email,
		MarginLevel:   marginLevel,
	}:
		logger.Audit.Warn("margin call warning queued and cooldown activated",
			zap.String("user_id", user.UserID),
			zap.String("margin_level", fmt.Sprintf("%.2f%%", marginLevel)),
		)
		return true
	default:
		// Roll back the cooldown on failure so we can try again on the next tick
		p.redisClient.Del(ctx, key)
		logger.Error.Error("notification channel full — margin call dropped, cooldown rolled back",
			zap.String("user_id", user.UserID),
		)
		return false
	}
}

// TriggerAccountLiquidation fires liquidation tasks for a user's open positions
// and issues exactly one LIQUIDATION_DIGEST. It also resets their margin warning cooldown.
func (p *Processor) TriggerAccountLiquidation(ctx context.Context, user *model.RiskUser, marginLevel float64, equity float64) {
	reason := fmt.Sprintf("STOP_OUT:%.2f%% (balance=%.2f equity=%.2f used_margin=%.2f threshold=%.1f%%)",
		marginLevel, user.Balance, equity, user.UsedMargin, p.cfg.StopOutPct)

	closedCount := 0
	var totalLoss float64

	// Fire liquidation payload for open positions
	for ticketID, position := range user.Positions {
		if !position.PendingLiquidation {
			position.PendingLiquidation = true

			totalLoss += position.CurrentPnL
			closedCount++

			select {
			case p.LiquidationCh <- LiquidationTask{
				TicketID: ticketID,
				UserID:   user.UserID,
				Reason:   reason,
			}:
				logger.Audit.Warn("stop-out triggered — liquidation queued",
					zap.String("ticket_id", ticketID),
					zap.String("user_id", user.UserID),
					zap.String("margin_level", fmt.Sprintf("%.2f%%", marginLevel)),
				)
			default:
				// If channel is full, rollback position flag
				position.PendingLiquidation = false
				closedCount--
			}
		}
	}

	if closedCount > 0 {
		// One LIQUIDATION_DIGEST notification (mapped to "auto_cutoff" template)
		select {
		case p.notificationQueue <- NotificationTask{
			Template:      "auto_cutoff",
			UserID:        user.UserID,
			AccountNumber: user.AccountNumber,
			Email:         user.Email,
			MarginLevel:   marginLevel,
		}:
		default:
			logger.Error.Error("notification channel full — auto_cutoff digest dropped",
				zap.String("user_id", user.UserID),
			)
		}

		// The Reset Trigger: Delete the margin warning cooldown so they can be warned again!
		key := fmt.Sprintf("cooldown:margin_warning:%s", user.UserID)
		if err := p.redisClient.Del(ctx, key).Err(); err != nil {
			logger.Error.Error("failed to delete margin_warning cooldown after liquidation", zap.Error(err), zap.String("user_id", user.UserID))
		} else {
			logger.Audit.Info("margin warning cooldown reset after liquidation", zap.String("user_id", user.UserID))
		}
	}
}
