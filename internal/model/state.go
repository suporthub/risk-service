// ─────────────────────────────────────────────────────────────────────────────
// internal/model/state.go
//
// RAM Ledger models for the risk-service.
//
// Architecture overview:
//
//   GlobalLedger
//   ├── Users: map[userID] → *RiskUser
//   │         Hot path: O(1) lookup by user ID from Kafka event.
//   │
//   └── SymbolIndex: map[symbol] → map[ticketID] → *RiskPosition
//                   THE KEY OPTIMISATION. When a tick arrives for EURUSD,
//                   we look up SymbolIndex["EURUSD"] and iterate ONLY the
//                   positions on that specific symbol. If 10,000 users are
//                   connected but only 200 hold EURUSD, we do 200 iterations
//                   — not 10,000. O(affected_positions), not O(all_users).
//
// Concurrency model:
//   - GlobalLedger.mu (sync.RWMutex): protects Users map and SymbolIndex map.
//     Read-lock on tick path (we only read the map, not mutate it).
//     Write-lock only when adding/removing users or positions (Kafka consumer).
//
//   - RiskUser.mu (sync.RWMutex): per-user mutex protects Balance, UsedMargin,
//     TotalFloatingPnL, and Positions. The tick processor holds this lock for
//     the duration of PnL delta computation and margin level check.
//     Per-user locking means EURUSD ticks NEVER block GBPUSD tick processing
//     for a different user.
//
// Delta PnL caching:
//   RiskPosition.CurrentPnL caches the LAST computed PnL for that position.
//   On each tick, instead of recalculating the entire portfolio:
//     NewPnL   = f(currentPrice, openPrice, volume)
//     Delta    = NewPnL - position.CurrentPnL
//     user.TotalFloatingPnL += Delta
//     position.CurrentPnL   = NewPnL
//   This is O(1) per position, not O(positions) per user.
// ─────────────────────────────────────────────────────────────────────────────

package model

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// RiskPosition — one open trade tracked by the risk engine.
// ─────────────────────────────────────────────────────────────────────────────

// RiskPosition mirrors the open-trade data consumed from the Kafka
// orders.executed event. It is the fundamental unit of risk tracking.
type RiskPosition struct {
	TicketID     string  // UUID v4 — stable identifier across all services
	UserID       string  // back-reference to the owning RiskUser (for O(1) SymbolIndex routing)
	Symbol       string  // e.g. "EURUSD", "XAUUSD"
	Group        string  // spread-group name (e.g. "Standard", "VIP") — used to select the correct
	              //   bid/ask from the multi-group tick payload; must match the pricing-service key.
	OrderType    string  // "BUY" or "SELL"
	Volume       float64 // lot size (e.g. 0.01, 1.0)
	OpenPrice    float64 // execution fill price
	ContractSize float64 // units per lot (e.g. 100,000 for FX majors, 100 for XAUUSD)

	// CurrentPnL caches the most recently calculated floating PnL for this
	// position. It is updated on every tick that matches this symbol.
	// NEVER read this field directly from outside the engine hot-path —
	// it is only valid while the owning RiskUser's mutex is held.
	CurrentPnL float64

	// PendingLiquidation is the "double-tap" debounce flag.
	//
	// The race window it closes:
	//   A stop-out is detected on Tick N. The LiquidationTask is pushed to the
	//   gRPC dispatcher channel (<1µs). The network call to the execution-service
	//   takes 1–5ms. During those 5ms, Ticks N+1 through N+K continue arriving
	//   for the same symbol. Because the position is still in the RAM ledger
	//   (the Kafka orders.closed event hasn't arrived yet), the stop-out condition
	//   fires again on every subsequent tick — flooding the dispatcher channel
	//   with identical LiquidationTasks and hammering the execution-service with
	//   redundant ForceLiquidate RPCs it must reject.
	//
	// The fix:
	//   When the tick processor pushes a LiquidationTask, it sets this flag to
	//   true INSIDE the per-user mutex critical section (atomically with the push).
	//   All subsequent ticks skip the liquidation push for this position entirely.
	//   The flag is reset to false by the Kafka orders.closed consumer when the
	//   position is removed — but by then, the position itself is gone from the
	//   ledger, so the reset is a no-op in practice.
	//
	// Cost: 1 bool read per position per tick (~1ns). Savings: eliminates N-1
	// redundant gRPC calls during a flash crash spike.
	//
	// MUST only be read or written while holding the owning RiskUser's mutex.
	PendingLiquidation bool
}

// ─────────────────────────────────────────────────────────────────────────────
// RiskUser — in-RAM account state for a single trading account.
// ─────────────────────────────────────────────────────────────────────────────

// RiskUser holds the running financial state of one user.
//
// All fields MUST be read or written while holding mu.
// The risk engine never copies this struct — always operate on the pointer.
type RiskUser struct {
	mu sync.RWMutex // per-user lock: User A's PnL update never blocks User B

	UserID          string
	Email           string
	AccountNumber   string
	Balance         float64 // realised wallet balance (set from orders.executed, updated on orders.closed)
	UsedMargin      float64 // sum of margin locked across all open positions
	TotalFloatingPnL float64 // running sum of CurrentPnL across ALL open positions
	LastMarginCall  time.Time // timestamp of the last margin call notification sent

	// Positions maps ticketID → *RiskPosition for O(1) lookup on orders.closed.
	Positions map[string]*RiskPosition
}

// Lock acquires the exclusive write lock on this user's state.
// Use for: PnL delta updates, margin level checks, position add/remove.
func (u *RiskUser) Lock() { u.mu.Lock() }

// Unlock releases the write lock.
func (u *RiskUser) Unlock() { u.mu.Unlock() }

// RLock acquires a read lock. Used when only reading Equity/MarginLevel
// without modifying — saves contention in read-heavy monitoring paths.
func (u *RiskUser) RLock() { u.mu.RLock() }

// RUnlock releases the read lock.
func (u *RiskUser) RUnlock() { u.mu.RUnlock() }

// Equity returns the current account equity.
// Formula: Equity = Balance + TotalFloatingPnL
// MUST be called while holding at least a read lock on mu.
func (u *RiskUser) Equity() float64 {
	return u.Balance + u.TotalFloatingPnL
}

// MarginLevel returns the current margin level as a percentage.
// Formula: MarginLevel = (Equity / UsedMargin) * 100
//
// Returns +Inf when UsedMargin is 0 (no open positions — cannot stop out).
// The tick processor checks: if MarginLevel <= StopOutPct → liquidate.
// MUST be called while holding at least a read lock on mu.
func (u *RiskUser) MarginLevel() float64 {
	if u.UsedMargin <= 0 {
		return 1_000_000.0 // effectively infinite — no positions open
	}
	return (u.Equity() / u.UsedMargin) * 100.0
}

// ─────────────────────────────────────────────────────────────────────────────
// GlobalLedger — the process-wide singleton registry.
// ─────────────────────────────────────────────────────────────────────────────

// GlobalLedger is the top-level in-RAM state store for the risk-service.
// There is exactly ONE instance per process, created in main() and passed as
// a pointer to all subsystems (consumer, processor, dispatcher).
type GlobalLedger struct {
	mu sync.RWMutex

	// Users maps userID → *RiskUser.
	// Written by the Kafka consumer on orders.executed (new user).
	// Read by the tick processor and liquidation dispatcher.
	Users map[string]*RiskUser

	// SymbolIndex maps symbol → (ticketID → *RiskPosition).
	//
	// This is the O(1) tick routing structure.
	// When a tick for "EURUSD" arrives:
	//   positions := ledger.SymbolIndex["EURUSD"]
	//   for ticketID, pos := range positions { ... }  // only EURUSD holders
	//
	// A position is ADDED to this index on orders.executed.
	// A position is REMOVED from this index on orders.closed.
	//
	// The *RiskPosition pointers are SHARED between Users[userID].Positions
	// and SymbolIndex[symbol] — same pointer, not a copy.
	// This means updating position.CurrentPnL in the tick loop is immediately
	// visible from the user's Positions map. No synchronisation needed between
	// the two maps; user.mu protects the mutation.
	SymbolIndex map[string]map[string]*RiskPosition
}

// NewGlobalLedger creates an empty ledger ready for use.
func NewGlobalLedger() *GlobalLedger {
	return &GlobalLedger{
		Users:       make(map[string]*RiskUser),
		SymbolIndex: make(map[string]map[string]*RiskPosition),
	}
}

// Lock acquires the exclusive write lock on the global ledger.
// Use when mutating Users or SymbolIndex.
func (l *GlobalLedger) Lock() { l.mu.Lock() }

// Unlock releases the exclusive write lock.
func (l *GlobalLedger) Unlock() { l.mu.Unlock() }

// RLock acquires a shared read lock.
// Use when only reading Users or SymbolIndex (e.g. tick processor hot path).
func (l *GlobalLedger) RLock() { l.mu.RLock() }

// RUnlock releases the shared read lock.
func (l *GlobalLedger) RUnlock() { l.mu.RUnlock() }

// UserDetails contains the results of a JIT user load
type UserDetails struct {
	Balance       float64
	AccountNumber string
	Email         string
}

type UserLoader func(ctx context.Context, userID string) (*UserDetails, error)

// GetOrCreateUser returns the RiskUser for the given userID, creating a new
// one with the provided initial balance and margin if not yet present.
//
// Used by the Kafka consumer on orders.executed.
// Write-lock is held for the minimum duration — only for the map insertion.
func (l *GlobalLedger) GetOrCreateUser(ctx context.Context, userID string, initialMargin float64, loader UserLoader) (*RiskUser, error) {
	// Fast path — read lock only.
	l.mu.RLock()
	user, ok := l.Users[userID]
	l.mu.RUnlock()
	if ok {
		return user, nil
	}

	// Slow path — write lock for insertion.
	l.mu.Lock()
	defer l.mu.Unlock()

	// Double-check: another goroutine may have inserted it between RUnlock and Lock.
	if user, ok = l.Users[userID]; ok {
		return user, nil
	}

	// JIT Load User Details
	details, err := loader(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("jit user load failed for %q: %w", userID, err)
	}

	user = &RiskUser{
		UserID:        userID,
		Email:         details.Email,
		AccountNumber: details.AccountNumber,
		Balance:       details.Balance,
		UsedMargin:    initialMargin,
		Positions:     make(map[string]*RiskPosition),
	}
	l.Users[userID] = user
	return user, nil
}

// AddPosition registers a position in both the user's Positions map and the
// global SymbolIndex. Both maps hold the SAME pointer — no data duplication.
//
// Caller MUST hold the GlobalLedger write-lock (l.mu.Lock()) AND the user
// write-lock (user.mu.Lock()) before calling this method.
func (l *GlobalLedger) AddPosition(user *RiskUser, pos *RiskPosition) {
	// Add to user's per-ticket lookup map.
	user.Positions[pos.TicketID] = pos

	// Ensure the symbol bucket exists in the SymbolIndex.
	if _, ok := l.SymbolIndex[pos.Symbol]; !ok {
		l.SymbolIndex[pos.Symbol] = make(map[string]*RiskPosition)
	}
	// Add the shared pointer to the SymbolIndex.
	l.SymbolIndex[pos.Symbol][pos.TicketID] = pos
}

// RemovePosition removes a position from the user's Positions map and from the
// global SymbolIndex. Cleans up the symbol bucket if it becomes empty.
//
// Caller MUST hold the GlobalLedger write-lock (l.mu.Lock()) AND the user
// write-lock (user.mu.Lock()) before calling this method.
// Returns the removed position (or nil if not found) so callers can compute
// the final PnL delta before returning the margin.
func (l *GlobalLedger) RemovePosition(user *RiskUser, ticketID string) *RiskPosition {
	pos, ok := user.Positions[ticketID]
	if !ok {
		return nil
	}

	// Remove from user's per-ticket map.
	delete(user.Positions, ticketID)

	// Remove from the global SymbolIndex.
	if bucket, exists := l.SymbolIndex[pos.Symbol]; exists {
		delete(bucket, ticketID)
		// Prune the bucket entirely if it's now empty to keep the SymbolIndex lean.
		if len(bucket) == 0 {
			delete(l.SymbolIndex, pos.Symbol)
		}
	}

	return pos
}

// ── Eager Hydration ───────────────────────────────────────────────────────────

// HydrateResult summarises what was loaded, used for structured boot logging.
type HydrateResult struct {
	Users     int // number of distinct RiskUsers created
	Positions int // total RiskPositions registered
}

// HydrateFromSnapshot populates the GlobalLedger from an ActiveRiskSnapshot
// produced by db.Loader.LoadAllActiveRisk().
//
// This MUST be called synchronously before any goroutine (Kafka consumer,
// tick processor) is started. It holds the GlobalLedger write-lock for the
// full duration of the hydration pass, which is safe because nothing else
// is reading the ledger at this point.
//
// Lock ordering: l.mu (GlobalLedger) is acquired first, then user.mu for
// each RiskUser — same canonical order used everywhere else in the service.
//
// The Snapshot contains a db.ActiveRiskSnapshot interface via a thin
// adapter type below so that model/ does not import db/ (avoiding a cycle).
func (l *GlobalLedger) HydrateFromSnapshot(
	positions []SnapshotEntry,
	balances  map[string]float64,
	emails    map[string]string,
	accounts  map[string]string,
) HydrateResult {
	l.mu.Lock()
	defer l.mu.Unlock()

	result := HydrateResult{}

	for _, sp := range positions {
		// ── Get or create the RiskUser ─────────────────────────────────────────
		user, ok := l.Users[sp.UserID]
		if !ok {
			user = &RiskUser{
				UserID:        sp.UserID,
				Balance:       balances[sp.UserID],
				Email:         emails[sp.UserID],
				AccountNumber: accounts[sp.UserID],
				Positions:     make(map[string]*RiskPosition),
			}
			l.Users[sp.UserID] = user
			result.Users++
		}

		// ── Build the RiskPosition and register it ─────────────────────────────
		pos := &RiskPosition{
			TicketID:     sp.TicketID,
			UserID:       sp.UserID,
			Symbol:       sp.Symbol,
			Group:        sp.GroupName,
			OrderType:    sp.OrderSide,
			Volume:       sp.Volume,
			OpenPrice:    sp.OpenPrice,
			ContractSize: sp.ContractSize,
			CurrentPnL:   0.0, // will be computed on first tick
		}

		// Accumulate UsedMargin from the snapshot.
		user.UsedMargin += sp.MarginUsed

		// AddPosition acquires no additional locks because we already hold l.mu.
		// user.mu is NOT needed here — no other goroutine can be reading the
		// user while the global write-lock is held.
		l.AddPosition(user, pos)
		result.Positions++
	}

	return result
}

// SnapshotEntry is the position shape expected by HydrateFromSnapshot.
// It is defined here (in model/) rather than in db/ to avoid an import cycle:
// model must not import db, but db imports model.
// The db package fills this via a simple field copy.
type SnapshotEntry struct {
	TicketID     string
	UserID       string
	Symbol       string
	GroupName    string
	OrderSide    string
	Volume       float64
	OpenPrice    float64
	ContractSize float64
	MarginUsed   float64
}

