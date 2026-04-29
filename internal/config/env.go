// ─────────────────────────────────────────────────────────────────────────────
// internal/config/env.go
//
// Global configuration loaded once at startup from .env / OS environment.
//
// Design decisions:
//   - godotenv loads .env ONLY if the file exists; in production (K8s) env vars
//     are injected directly by the container orchestrator — no .env file needed.
//   - All risk thresholds are float64 so comparisons in the tick processor
//     hot-path are a single float64 comparison (~1ns) with no type coercions.
//   - os.Getenv with a fallback pattern keeps this file free of reflection
//     and third-party config libraries — zero overhead, pure stdlib.
// ─────────────────────────────────────────────────────────────────────────────

package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration for the risk-service.
// There is exactly one instance, created at startup by Load() and shared
// as a pointer throughout the process.
type Config struct {
	// ── Risk Thresholds ────────────────────────────────────────────────────

	// StopOutPct is the margin level (%) at or below which a position is
	// force-liquidated. Default: 50.0
	//   MarginLevel = (Equity / UsedMargin) * 100
	//   Trigger when MarginLevel <= StopOutPct
	StopOutPct float64

	// MarginCallPct is the margin level (%) at or below which a margin-call
	// warning is issued. Default: 100.0
	//   The warning is informational — no position is closed.
	MarginCallPct float64

	// ── Infrastructure ─────────────────────────────────────────────────────

	// RedisNodes is the list of Redis cluster seed nodes (host:port).
	// Parsed from comma-separated REDIS_NODES env var.
	// The cluster client discovers all other nodes automatically.
	RedisNodes []string

	// RedisPassword is the AUTH password for Redis. Empty string = no auth.
	RedisPassword string

	// KafkaBrokers is the list of Kafka bootstrap servers.
	// Parsed from comma-separated env var: "host1:port,host2:port"
	KafkaBrokers []string

	// KafkaGroupID is the consumer group ID for this service.
	KafkaGroupID string

	// ExecutionGRPCAddr is the host:port of the execution-service gRPC server.
	// The risk-service dials this address to call ForceLiquidate.
	ExecutionGRPCAddr string
}

// Load reads configuration from the environment (and optionally .env file).
// Call this exactly once at process startup from main().
// Panics on missing required values so the process crashes fast with a clear
// error rather than silently misbehaving.
func Load() *Config {
	// Load .env file if it exists (development). In production, env vars are
	// injected by Kubernetes / Docker — the file won't be present and that is fine.
	if err := godotenv.Load(); err != nil {
		slog.Info("no .env file found — using OS environment variables only")
	}

	cfg := &Config{
		StopOutPct:        parseFloat("RISK_STOP_OUT_PCT", 50.0),
		MarginCallPct:     parseFloat("RISK_MARGIN_CALL_PCT", 100.0),
		RedisNodes:        parseStringSlice("REDIS_NODES", ","),
		RedisPassword:     os.Getenv("REDIS_PASSWORD"),
		KafkaBrokers:      parseStringSlice("KAFKA_BROKERS", ","),
		KafkaGroupID:      envOrDefault("KAFKA_GROUP_ID", "risk-service"),
		ExecutionGRPCAddr: requireEnv("EXECUTION_GRPC_ADDR"),
	}

	slog.Info("risk-service config loaded",
		"stop_out_pct",    cfg.StopOutPct,
		"margin_call_pct", cfg.MarginCallPct,
		"redis_nodes",    cfg.RedisNodes,
		"kafka_brokers",   cfg.KafkaBrokers,
		"grpc_addr",       cfg.ExecutionGRPCAddr,
	)

	return cfg
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// requireEnv returns the value of the environment variable or panics.
// Used for configuration that MUST be present — there is no safe default.
func requireEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		panic("risk-service: required environment variable not set: " + key)
	}
	return val
}

// envOrDefault returns the env var value or the provided fallback string.
func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// parseFloat parses a float64 from an env var, falling back to defaultVal.
func parseFloat(key string, defaultVal float64) float64 {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		slog.Warn("invalid float env var, using default",
			"key", key, "value", val, "default", defaultVal)
		return defaultVal
	}
	return f
}

// parseStringSlice splits an env var on sep and trims whitespace.
// Returns a nil slice (not empty) if the env var is unset — callers
// should check len > 0 if the value is required.
func parseStringSlice(key, sep string) []string {
	val := os.Getenv(key)
	if val == "" {
		return nil
	}
	parts := strings.Split(val, sep)
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
