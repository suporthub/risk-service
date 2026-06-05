// ─────────────────────────────────────────────────────────────────────────────
// internal/engine/processor.go
//
// The Tick Processor — The Risk Engine Hot Path
//
// This is the most performance-critical file in the risk-service.
// It runs on every single market tick and determines whether any user needs
// to be liquidated.
//
// Payload format on tick:<SYMBOL>:
//   "Raw:1.10010,1.10020|Standard:1.09980,1.10050|VIP:1.09995,1.10035"
//
// Hot path algorithm per tick:
//
//   1. O(1) symbol lookup:
//        bucket := ledger.SymbolIndex[tick.Symbol]
//      If empty → return immediately. Zero work for unrelated symbols.
//
//   2. Snapshot position pointers while holding GlobalLedger.RLock:
//        for _, pos := range bucket → collect (pos, *RiskUser) pairs
//      RLock is released immediately after the snapshot.
//      Each pos.UserID is the back-reference to look up the owning RiskUser.
//
//   3. Per-position delta PnL (inside user.Lock()):
//        gp  := tick.GroupPrices[pos.Group]  (falls back to "Raw" if absent)
//        NewPnL = calcPnL(gp, pos)
//        Delta  = NewPnL - pos.CurrentPnL
//        user.TotalFloatingPnL += Delta     ← running sum — no full portfolio scan
//        pos.CurrentPnL = NewPnL
//
//   4. Equity / Margin Level check:
//        Equity      = Balance + TotalFloatingPnL
//        MarginLevel = (Equity / UsedMargin) * 100
//
//   5. Stop-out trigger (non-blocking channel push):
//        if MarginLevel <= cfg.StopOutPct → push LiquidationTask{}
//
// Lock discipline:
//   GlobalLedger.RLock → held only for snapshot phase (steps 1–2).
//   user.Lock()        → held only during PnL delta + margin check (steps 3–5).
//   These two locks are NEVER held simultaneously, eliminating deadlock risk
//   with the Kafka consumer (which holds GlobalLedger.Lock + user.Lock together).
// ─────────────────────────────────────────────────────────────────────────────

package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/livefxhub/risk-service/internal/config"
	"github.com/livefxhub/risk-service/internal/model"
	redisSub "github.com/livefxhub/risk-service/internal/redis"
	goredis "github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────────────────
// LiquidationTask — dispatched from processor to the gRPC dispatcher goroutine
// ─────────────────────────────────────────────────────────────────────────────

// LiquidationTask carries the minimal data needed to fire a ForceLiquidate
// gRPC call. Built in the hot path; consumed by the Dispatcher goroutine.
type LiquidationTask struct {
	TicketID string // UUID v4 — position to liquidate
	UserID   string // owning user — for audit logging in the execution-service
	Reason   string // e.g. "STOP_OUT:45.23%" — written to the ForceLiquidate request
}

// ─────────────────────────────────────────────────────────────────────────────
// Processor
// ─────────────────────────────────────────────────────────────────────────────

// Processor reads Tick values from the Redis subscriber channel and evaluates
// risk for every position on the ticking symbol.
type Processor struct {
	ledger            *model.GlobalLedger
	cfg               *config.Config
	fxConverter       *FxConverter
	LiquidationCh     chan LiquidationTask    // consumed by the gRPC liquidation dispatcher
	notificationQueue chan<- NotificationTask // consumed by the notification dispatcher
	redisClient       *goredis.ClusterClient  // injected Redis client for cooldowns
}

// NewProcessor creates a Processor with a pre-allocated liquidation channel.
//
// notificationQueue is the send-only channel from NotificationDispatcher.Queue().
// The channel buffer size of 1,000 lets the gRPC dispatcher lag by up to 1,000
// liquidation events without stalling the tick loop. In practice, stop-outs
// are rare; this buffer provides ample headroom for broker-level spike events.
func NewProcessor(ledger *model.GlobalLedger, cfg *config.Config, fxConverter *FxConverter, notificationQueue chan<- NotificationTask, redisClient *goredis.ClusterClient) *Processor {
	return &Processor{
		ledger:            ledger,
		cfg:               cfg,
		fxConverter:       fxConverter,
		LiquidationCh:     make(chan LiquidationTask, 1_000),
		notificationQueue: notificationQueue,
		redisClient:       redisClient,
	}
}

// Start launches the tick processing loop. Reads from tickCh until ctx is done.
// Call this in a dedicated goroutine from main():
//
//	go proc.Start(ctx, sub.TickCh)
func (p *Processor) Start(ctx context.Context, tickCh <-chan redisSub.Tick) {
	slog.Info("tick processor started")
	for {
		select {
		case <-ctx.Done():
			slog.Info("tick processor shutting down")
			return
		case tick, ok := <-tickCh:
			if !ok {
				return // channel closed
			}

			// Update FxConverter with the latest Raw or fallback tick
			gp, ok := tick.GroupPrices["Raw"]
			if !ok {
				// fallback to the first available group if Raw is missing
				for _, v := range tick.GroupPrices {
					gp = v
					break
				}
			}
			p.fxConverter.UpdateRate(tick.Symbol, gp.Bid, gp.Ask)

			p.processTick(tick)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// posSnapshot holds a position pointer and its owning user pointer,
// captured while the GlobalLedger read-lock is held.
// After the RLock is released, both pointers remain valid as long as
// we do not read/write the position or user without the user's own mutex.
// ─────────────────────────────────────────────────────────────────────────────
type posSnapshot struct {
	pos  *model.RiskPosition
	user *model.RiskUser
}

// processTick is the per-tick hot-path function.
// Inlining is intentional — no sub-function calls inside critical sections.
func (p *Processor) processTick(tick redisSub.Tick) {

	// ── Step 1 & 2: O(1) Symbol Lookup + Position Snapshot ───────────────────
	//
	// Hold GlobalLedger.RLock only while:
	//   a) checking if the symbol has any open positions.
	//   b) snapshotting (pos, user) pairs into a local slice.
	// The RLock is released BEFORE we acquire any per-user lock.
	//
	// Why snapshot? We cannot hold GlobalLedger.RLock while acquiring user.Lock()
	// because the Kafka consumer holds them in the opposite order on rare occasions
	// (it acquires GlobalLedger.Lock then user.Lock). To avoid lock-order inversion,
	// we release the global lock first, then acquire user locks one by one.
	p.ledger.RLock()

	bucket, exists := p.ledger.SymbolIndex[tick.Symbol]
	if !exists || len(bucket) == 0 {
		p.ledger.RUnlock()
		return // No positions open for this symbol — the overwhelmingly common case.
	}

	// Pre-allocate the snapshot slice with the exact bucket size.
	// Each element is two pointers = 16 bytes. For 500 positions: 8KB on stack.
	snapshots := make([]posSnapshot, 0, len(bucket))

	for _, pos := range bucket {
		// pos.UserID is the back-reference set when the position was registered.
		// O(1) map lookup — no iteration over all users.
		user, userFound := p.ledger.Users[pos.UserID]
		if !userFound {
			// Defensive: position exists in SymbolIndex but user was already
			// evicted or never existed. Skip silently.
			continue
		}
		snapshots = append(snapshots, posSnapshot{pos: pos, user: user})
	}

	p.ledger.RUnlock() // ← GlobalLedger lock released here. Per-user locks next.

	affectedUsers := make(map[*model.RiskUser]bool)

	// ── Steps 3: Per-Position PnL Delta ───────────────
	for _, snap := range snapshots {
		pos := snap.pos
		user := snap.user

		// Per-user exclusive lock. User A's tick processing never blocks User B.
		user.Lock()

		// ── Step 3: Delta PnL Computation ────────────────────────────────────
		gp, gpOK := tick.GroupPrices[pos.Group]
		if !gpOK {
			gp, gpOK = tick.GroupPrices["Raw"]
			if !gpOK {
				user.Unlock()
				continue
			}
		}
		bid, ask := gp.Bid, gp.Ask

		var newPnL float64
		if pos.OrderType == "BUY" {
			newPnL = (bid - pos.OpenPrice) * pos.ContractSize * pos.Volume
		} else { // "SELL"
			newPnL = (pos.OpenPrice - ask) * pos.ContractSize * pos.Volume
		}

		// Convert PnL to USD
		quote := extractQuoteCurrency(tick.Symbol)
		newPnL_USD := p.fxConverter.ConvertToUSD(newPnL, quote)

		pnlDelta := newPnL_USD - pos.CurrentPnL
		user.TotalFloatingPnL += pnlDelta // O(1) running total update
		pos.CurrentPnL = newPnL_USD       // cache for next tick's delta

		affectedUsers[user] = true
		user.Unlock()
	}

	// ── Steps 4 & 5: Equity & Margin Level Check (Exactly ONCE per user) ───────────────
	for user := range affectedUsers {
		user.Lock()

		equity := user.Equity()
		marginLevel := user.MarginLevel()
		// These are two float64 additions and one division — ~3ns total.
		// MarginLevel returns 1,000,000 (effectively ∞) when UsedMargin ≤ 0.

		// ── Step 5: Threshold Checks ──────────────────────────────────────────

		if marginLevel <= p.cfg.StopOutPct {
			if !user.IsLiquidating {
				user.IsLiquidating = true

				// Fire the delegated liquidation routine (spawns notifications + force-closes + clears cooldown)
				ctx := context.Background()
				p.TriggerAccountLiquidation(ctx, user, marginLevel, equity)
			}
		} else if marginLevel <= p.cfg.MarginCallPct {
			// ── MARGIN CALL WARNING ───────────────────────────────────────────
			// Uses the Redis-backed Cooldown pattern via SetNX
			ctx := context.Background()
			p.SendMarginWarning(ctx, user, marginLevel)
		} else if marginLevel >= (p.cfg.MarginCallPct + 10.0) {
			// ── RECOVERY RESET ────────────────────────────────────────────────
			if user.IsLiquidating {
				user.IsLiquidating = false
				slog.Info("liquidation flag reset — user margin recovered",
					"user_id", user.UserID,
					"margin_level", fmt.Sprintf("%.2f%%", marginLevel),
				)
			}
			if !user.LastMarginCall.IsZero() {
				user.LastMarginCall = time.Time{}
				slog.Info("in-memory margin call throttle reset — user recovered",
					"user_id", user.UserID,
					"margin_level", fmt.Sprintf("%.2f%%", marginLevel),
				)
			}
		}

		user.Unlock()
	}
}

// extractQuoteCurrency returns the 3-character quote currency from a symbol.
// For standard 6-character FX pairs (EURUSD, GBPJPY, AUDCAD, BTCUSD):
//
//	symbol[3:6] is the quote currency.
//
// For non-standard symbols, it returns "USD" as a fallback.
func extractQuoteCurrency(symbol string) string {
	if len(symbol) == 6 {
		return symbol[3:6]
	}
	return "USD"
}
