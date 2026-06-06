// ─────────────────────────────────────────────────────────────────────────────
// internal/redis/client.go
//
// Shared Redis cluster client builder.
//
// The Kubernetes Redis cluster is configured with cluster-announce-ip and
// broadcasts its Public IP directly. No NAT mapping is required — the driver
// connects straight to the announced addresses.
// ─────────────────────────────────────────────────────────────────────────────

package redis

import (
	"time"

	"github.com/livefxhub/risk-service/internal/logger"
	"go.uber.org/zap"

	goredis "github.com/redis/go-redis/v9"
)

// NewClusterClient creates a standard Redis cluster client.
// addrs should be the seed nodes read from REDIS_NODES (host:port pairs).
// The driver discovers the full cluster topology via CLUSTER SLOTS/SHARDS
// after connecting to any seed node.
func NewClusterClient(addrs []string, password string, poolSize int, readTimeout time.Duration) *goredis.ClusterClient {
	logger.Telemetry.Info("building Redis cluster client", zap.Strings("seed_nodes", addrs))

	opts := &goredis.ClusterOptions{
		Addrs:        addrs,
		Password:     password,
		PoolSize:     poolSize,
		MinIdleConns: 3,
		DialTimeout:  2000 * time.Millisecond,
		ReadTimeout:  readTimeout,
		WriteTimeout: 2000 * time.Millisecond,
	}

	return goredis.NewClusterClient(opts)
}
