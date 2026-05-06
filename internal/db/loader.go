package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Loader provides JIT balance hydration for the risk ledger.
type Loader struct {
	pool *pgxpool.Pool
}

// NewLoader creates a Loader connected to the user database.
func NewLoader(ctx context.Context, dsn string) (*Loader, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse db dsn: %w", err)
	}

	// Read-only, extremely small pool — JIT loads are infrequent
	cfg.MaxConns = 3
	cfg.MinConns = 1

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("db ping failed: %w", err)
	}

	return &Loader{pool: pool}, nil
}

// Close shuts down the connection pool.
func (l *Loader) Close() {
	l.pool.Close()
}

// LoadInitialBalance fetches the user's wallet balance from Postgres.
// If the user doesn't exist yet, it safely returns 0.0.
func (l *Loader) LoadInitialBalance(ctx context.Context, userID string) (float64, error) {
	qctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	var balance float64
	err := l.pool.QueryRow(qctx, `
		SELECT "walletBalance" 
		FROM live_users 
		WHERE id = $1
	`, userID).Scan(&balance)

	if err != nil {
		if err == pgx.ErrNoRows {
			// Safe fallback if account record doesn't exist yet
			return 0.0, nil
		}
		return 0.0, err
	}
	return balance, nil
}
