package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/livefxhub/risk-service/internal/model"
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

// LoadUserDetails fetches the initial wallet balance, account number, and email
// via a JOIN between live_users and user_profiles.
// Returns the model.UserDetails struct used by the JIT UserLoader.
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
			// Safe fallback if account record doesn't exist yet
			return &model.UserDetails{Balance: 0.0}, nil
		}
		return nil, err
	}
	return &details, nil
}
