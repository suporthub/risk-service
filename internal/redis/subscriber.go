// ─────────────────────────────────────────────────────────────────────────────
// internal/redis/subscriber.go
//
// Redis Tick Firehose Subscriber
//
// The pricing-service publishes market ticks to Redis pub/sub channels using
// the pattern: tick:<SYMBOL>
//
// Cluster mode:
//   Production Redis is a 3-master cluster behind a NAT.
//   PSubscribe on a ClusterClient broadcasts the subscription to ALL master
//   nodes — necessary because any node can receive a PUBLISH command and
//   pub/sub is NOT automatically forwarded across cluster shards in Redis <7.
//   go-redis ClusterClient handles this transparently via PSubscribe.
//
// Payload format: "<BID>,<ASK>" — e.g. "1.10010,1.10020"
// ─────────────────────────────────────────────────────────────────────────────

package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Tick carries the parsed market data for a single symbol update.
type Tick struct {
	Symbol string
	Bid    float64
	Ask    float64
}

// Subscriber connects to the Redis cluster and listens on the pattern tick:*.
// It parses incoming messages and pushes Tick values into the TickCh channel.
type Subscriber struct {
	client *goredis.ClusterClient
	TickCh chan Tick // buffered channel read by the tick processor
}

// NewSubscriber creates a Subscriber connected to the Redis cluster.
//
// addrs is a slice of seed node addresses (host:port).
// password is the Redis AUTH password; pass "" if no auth is configured.
// The TickCh buffer size of 50,000 provides ~5 seconds of burst headroom
// at 1,000 symbols × 10 ticks/sec peak load.
func NewSubscriber(addrs []string, password string) *Subscriber {
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    addrs,
		Password: password,

		PoolSize:     5,
		MinIdleConns: 2,

		DialTimeout:  2 * time.Second,
		ReadTimeout:  0, // no timeout on reads — pub/sub blocks until data arrives
		WriteTimeout: 2 * time.Second,
	})

	return &Subscriber{
		client: client,
		TickCh: make(chan Tick, 50_000),
	}
}

// Start subscribes to the Redis pattern tick:* and begins forwarding ticks
// to TickCh. Runs until ctx is cancelled.
// On Redis connection loss, Start reconnects after a 2s back-off.
func (s *Subscriber) Start(ctx context.Context) {
	for {
		if err := s.run(ctx); err != nil {
			if ctx.Err() != nil {
				slog.Info("redis subscriber shutting down")
				return
			}
			slog.Error("redis subscriber error, reconnecting in 2s", "error", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func (s *Subscriber) run(ctx context.Context) error {
	pubsub := s.client.PSubscribe(ctx, "tick:*")
	defer pubsub.Close()

	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("redis PSubscribe confirmation: %w", err)
	}

	slog.Info("redis tick subscriber active", "pattern", "tick:*")

	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return nil

		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("redis pubsub channel closed")
			}

			tick, err := parseTick(msg.Channel, msg.Payload)
			if err != nil {
				slog.Warn("malformed tick payload",
					"channel", msg.Channel,
					"payload", msg.Payload,
					"error",   err,
				)
				continue
			}

			select {
			case s.TickCh <- tick:
			default:
				slog.Warn("tick channel full — dropping tick (processor lag?)",
					"symbol", tick.Symbol,
				)
			}
		}
	}
}

// parseTick extracts Symbol, Bid, and Ask from a Redis pub/sub message.
// Channel format: "tick:EURUSD"  → Symbol = "EURUSD"
// Payload format: "1.10010,1.10020"
func parseTick(channel, payload string) (Tick, error) {
	chanParts := strings.SplitN(channel, ":", 2)
	if len(chanParts) != 2 || chanParts[1] == "" {
		return Tick{}, fmt.Errorf("invalid channel format: %q (expected tick:<SYMBOL>)", channel)
	}
	symbol := chanParts[1]

	parts := strings.SplitN(payload, ",", 2)
	if len(parts) != 2 {
		return Tick{}, fmt.Errorf("invalid tick payload: %q (expected BID,ASK)", payload)
	}

	bid, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return Tick{}, fmt.Errorf("parse bid from %q: %w", payload, err)
	}

	ask, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return Tick{}, fmt.Errorf("parse ask from %q: %w", payload, err)
	}

	if bid <= 0 || ask <= 0 || ask < bid {
		return Tick{}, fmt.Errorf("invalid tick values: bid=%.5f ask=%.5f", bid, ask)
	}

	return Tick{Symbol: symbol, Bid: bid, Ask: ask}, nil
}

// Close cleanly disconnects the Redis cluster client.
func (s *Subscriber) Close() error {
	return s.client.Close()
}
