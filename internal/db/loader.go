// ─────────────────────────────────────────────────────────────────────────────
// internal/db/loader.go
//
// Database layer for the risk-service.
//
// Two responsibilities:
//
//   1. LoadAllActiveRisk() — Eager boot-time snapshot.
//      Called synchronously BEFORE the Kafka consumer or tick processor starts.
//      Queries order_db for all OPEN positions, then user_db for balances.
//      Completely hydrates the GlobalLedger in one shot, eliminating the
//      eventual-consistency window that caused false stop-outs with Kafka replay.
//
//   2. LoadUserDetails() — JIT delta loader.
//      Called by the Kafka live-delta consumer when a NEW user (one with no
//      previously open positions) places their first order after boot.
//      Still needed because the eager snapshot only covers users with positions
//      open AT boot time; users who first trade post-boot are loaded JIT.
//
// Connection pools:
//   pool      → user_db  (USER_DATABASE_URL) — live_users, user_profiles
//   orderPool → order_db (ORDER_DATABASE_URL) — orders table
//
// Why two separate DSNs?
//   The orders table lives in order_db (managed by the persistence worker).
//   Wallet balances and user contact details live in user_db.
//   Mixing them in a single connection string is not possible; they are
//   separate PostgreSQL databases on separate ports.
// ─────────────────────────────────────────────────────────────────────────────

package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/livefxhub/risk-service/internal/model"
)

// Loader provides eager boot-time snapshot loading and JIT delta loading
// for the risk ledger.
//
// Always construct via NewLoader — the zero value is unusable.
type Loader struct {
	pool      *pgxpool.Pool // user_db: wallet balances, user contact details
	orderPool *pgxpool.Pool // order_db: open/pending order positions
}

// NewLoader creates a Loader with connections to both databases.
//
//   userDSN  — PostgreSQL DSN for user_db  (live_users, user_profiles)
//   orderDSN — PostgreSQL DSN for order_db (orders table)
func NewLoader(ctx context.Context, userDSN, orderDSN string) (*Loader, error) {
	// ── user_db pool ──────────────────────────────────────────────────────────
	// Small pool: used only for JIT balance loads and the boot-time wallet
	// batch query. Never hit on the tick-processor hot path.
	userCfg, err := pgxpool.ParseConfig(userDSN)
	if err != nil {
		return nil, fmt.Errorf("parse user db dsn: %w", err)
	}
	userCfg.MaxConns          = 5
	userCfg.MinConns          = 1
	userCfg.MaxConnLifetime   = 30 * time.Minute
	userCfg.MaxConnIdleTime   = 5 * time.Minute
	userCfg.HealthCheckPeriod = 1 * time.Minute

	userPool, err := pgxpool.NewWithConfig(ctx, userCfg)
	if err != nil {
		return nil, fmt.Errorf("connect to user_db: %w", err)
	}
	if err := userPool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("user_db ping failed: %w", err)
	}

	// ── order_db pool ─────────────────────────────────────────────────────────
	// Used for the boot-time eager position scan. After boot, only the
	// Kafka live-delta consumer mutates the ledger, so this pool sees
	// near-zero traffic in steady state.
	orderCfg, err := pgxpool.ParseConfig(orderDSN)
	if err != nil {
		return nil, fmt.Errorf("parse order db dsn: %w", err)
	}
	orderCfg.MaxConns          = 3
	orderCfg.MinConns          = 1
	orderCfg.MaxConnLifetime   = 30 * time.Minute
	orderCfg.MaxConnIdleTime   = 5 * time.Minute
	orderCfg.HealthCheckPeriod = 1 * time.Minute

	orderPool, err := pgxpool.NewWithConfig(ctx, orderCfg)
	if err != nil {
		return nil, fmt.Errorf("connect to order_db: %w", err)
	}
	if err := orderPool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("order_db ping failed: %w", err)
	}

	return &Loader{pool: userPool, orderPool: orderPool}, nil
}

// Close shuts down both connection pools. Call during graceful shutdown.
func (l *Loader) Close() {
	l.pool.Close()
	l.orderPool.Close()
}

// ── Eager Boot-Time Snapshot ──────────────────────────────────────────────────

// ActiveRiskSnapshot is the full result of LoadAllActiveRisk().
// It is consumed by main.go to populate the GlobalLedger before any
// goroutine is allowed to start.
type ActiveRiskSnapshot struct {
	// Positions is every OPEN order across all users, keyed by ticketID.
	Positions []*SnapshotPosition

	// Balances maps userID → walletBalance fetched from live_users.
	// Only users who have at least one OPEN position are included.
	Balances map[string]float64

	// Emails maps userID → email (from user_profiles JOIN).
	Emails map[string]string

	// AccountNumbers maps userID → accountNumber (from live_users).
	AccountNumbers map[string]string
}

// SnapshotPosition is one row from the open-position query.
// Field names mirror RiskPosition exactly to simplify conversion.
type SnapshotPosition struct {
	TicketID     string
	UserID       string
	Symbol       string
	GroupName    string  // spread-group name from orders.groupName
	OrderSide    string  // "BUY" or "SELL"
	Volume       float64
	OpenPrice    float64
	ContractSize float64 // from orders.contractSize; defaults to 100,000 if NULL
	MarginUsed   float64 // from orders.marginUsed
}

// LoadAllActiveRisk performs the eager boot-time database snapshot.
//
// Algorithm (two sequential queries, one round-trip each):
//
//  Query 1 — OPEN positions (order_db):
//    Reads every order with status = 'OPEN'.
//    Collects the distinct set of userIDs encountered.
//
//  Query 2 — Wallet balances + contact details (user_db):
//    Reads walletBalance, accountNumber, and email for the collected userIDs.
//    Uses a single WHERE id = ANY($1) query — one round-trip regardless of
//    how many users have open positions.
//
// Both queries run with a 30-second timeout. If either fails, the error is
// returned and main.go must treat it as fatal (os.Exit(1)).
//
// Caller contract:
//   MUST be called synchronously before any goroutine is started.
//   The returned snapshot is used to populate the GlobalLedger and is then
//   discarded — it is never stored in persistent state.
func (l *Loader) LoadAllActiveRisk(ctx context.Context) (*ActiveRiskSnapshot, error) {
	// 30-second budget: covers cold DB caches and large position tables.
	// In production with ~10,000 open positions this query completes in <200ms.
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	snapshot := &ActiveRiskSnapshot{
		Balances:       make(map[string]float64),
		Emails:         make(map[string]string),
		AccountNumbers: make(map[string]string),
	}

	// ── Query 1: OPEN positions from order_db ─────────────────────────────────
	//
	// We select only the fields the risk engine needs for its PnL loop:
	//   id (ticketID), userId, symbol, groupName, orderType (side), volume,
	//   openPrice, contractSize, marginUsed.
	//
	// We do NOT select closePrice, netPnl, swap — irrelevant for risk.
	// COALESCE(contractSize, 100000) ensures the risk engine never divides by
	// zero for orders inserted before contractSize was a required field.
	//
	// Note: "orderType" in order_db stores the FULL order type string
	// (BUY, SELL, BUY_LIMIT…). For risk we only need BUY vs SELL (which open
	// positions always are), so we map BUY*/SELL* → BUY/SELL via CASE.
	slog.Info("eager snapshot: querying OPEN positions from order_db")
	posRows, err := l.orderPool.Query(qctx, `
		SELECT
			"orderId"                                                  AS ticket_id,
			"userId"                                                   AS user_id,
			symbol,
			COALESCE("groupName", '')                                  AS group_name,
			CASE WHEN "orderType"::text LIKE 'BUY%' THEN 'BUY' ELSE 'SELL' END AS order_side,
			volume::float8,
			COALESCE("openPrice",  0)::float8                         AS open_price,
			COALESCE("contractSize", 100000)::float8                  AS contract_size,
			COALESCE("marginUsed", 0)::float8                         AS margin_used
		FROM orders
		WHERE status = 'OPEN'
		ORDER BY "openedAt" ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("LoadAllActiveRisk: query OPEN positions: %w", err)
	}
	defer posRows.Close()

	// Collect positions and track distinct userIDs in one pass.
	userIDSet := make(map[string]struct{})

	for posRows.Next() {
		var p SnapshotPosition
		if err := posRows.Scan(
			&p.TicketID,
			&p.UserID,
			&p.Symbol,
			&p.GroupName,
			&p.OrderSide,
			&p.Volume,
			&p.OpenPrice,
			&p.ContractSize,
			&p.MarginUsed,
		); err != nil {
			slog.Warn("eager snapshot: skip malformed position row", "error", err)
			continue
		}
		snapshot.Positions = append(snapshot.Positions, &p)
		userIDSet[p.UserID] = struct{}{}
	}
	if err := posRows.Err(); err != nil {
		return nil, fmt.Errorf("LoadAllActiveRisk: iterate position rows: %w", err)
	}

	slog.Info("eager snapshot: OPEN positions loaded",
		"count",        len(snapshot.Positions),
		"unique_users", len(userIDSet),
	)

	// If there are no open positions, skip the second query — nothing to load.
	if len(userIDSet) == 0 {
		slog.Info("eager snapshot: no open positions found — ledger will start empty")
		return snapshot, nil
	}

	// ── Query 2: Wallet balances + contact details from user_db ───────────────
	//
	// Collect the distinct userIDs into a slice for the ANY($1) parameter.
	// pgx encodes []string as a PostgreSQL text array automatically.
	userIDs := make([]string, 0, len(userIDSet))
	for id := range userIDSet {
		userIDs = append(userIDs, id)
	}

	slog.Info("eager snapshot: querying wallet balances from user_db", "users", len(userIDs))
	balRows, err := l.pool.Query(qctx, `
		SELECT
			lu.id,
			lu."walletBalance"::float8,
			lu."accountNumber",
			COALESCE(up.email, '')  AS email
		FROM live_users lu
		LEFT JOIN user_profiles up ON up.id = lu."userProfileId"
		WHERE lu.id = ANY($1::uuid[])
	`, userIDs)
	if err != nil {
		return nil, fmt.Errorf("LoadAllActiveRisk: query wallet balances: %w", err)
	}
	defer balRows.Close()

	for balRows.Next() {
		var (
			userID        string
			walletBalance float64
			accountNumber string
			email         string
		)
		if err := balRows.Scan(&userID, &walletBalance, &accountNumber, &email); err != nil {
			slog.Warn("eager snapshot: skip malformed balance row", "error", err)
			continue
		}
		snapshot.Balances[userID]       = walletBalance
		snapshot.AccountNumbers[userID] = accountNumber
		snapshot.Emails[userID]         = email
	}
	if err := balRows.Err(); err != nil {
		return nil, fmt.Errorf("LoadAllActiveRisk: iterate balance rows: %w", err)
	}

	slog.Info("eager snapshot: wallet balances loaded", "count", len(snapshot.Balances))
	return snapshot, nil
}

// ── JIT Delta Loader ──────────────────────────────────────────────────────────

// LoadUserDetails fetches the wallet balance, account number, and email for a
// single user. Called by the Kafka live-delta consumer when a new user (not
// present in the eager snapshot) places their first order after boot.
//
// 500ms timeout is acceptable: this is a cold-path operation that happens at
// most once per user per process lifetime.
func (l *Loader) LoadUserDetails(ctx context.Context, userID string) (*model.UserDetails, error) {
	qctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	var details model.UserDetails

	err := l.pool.QueryRow(qctx, `
		SELECT lu."walletBalance", lu."accountNumber", up.email 
		FROM live_users lu
		JOIN user_profiles up ON lu."userProfileId" = up.id
		WHERE lu.id = $1
	`, userID).Scan(&details.Balance, &details.AccountNumber, &details.Email)

	if err != nil {
		if err == pgx.ErrNoRows {
			// Safe fallback: user may not yet have a profile row.
			return &model.UserDetails{Balance: 0.0}, nil
		}
		return nil, err
	}
	return &details, nil
}

