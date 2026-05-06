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
// Payload format: multi-group string
//   "Raw:1.10010,1.10020|Standard:1.09980,1.10050|VIP:1.09995,1.10035"
//
// Parse strategy (zero-allocation hot path):
//   1. Split by "|"	→ group chunks
//   2. SplitN by ":" 2 → groupName + "BID,ASK"
//   3. SplitN by "," 2 → bid + ask strings
//   4. ParseFloat x2   → done
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

// GroupPrice holds the bid/ask for one spread group as parsed from a tick payload.
type GroupPrice struct {
	Bid float64
	Ask float64
}

// Tick carries the parsed market data for a single symbol update.
// GroupPrices maps group name → GroupPrice for every spread group the
// pricing-service calculated (e.g. "Raw", "Standard", "VIP").
type Tick struct {
	Symbol      string
	GroupPrices map[string]GroupPrice
}

// Subscriber connects to the Redis cluster and listens on the pattern tick:*.
// It parses incoming messages and pushes Tick values into the TickCh channel.
type Subscriber struct {
	client *goredis.ClusterClient
	TickCh chan Tick // buffered channel read by the tick processor
}

// NewSubscriber creates a Subscriber connected to the Redis cluster.
func NewSubscriber(addrs []string, password string) *Subscriber {
	// Use NAT-aware client builder with 0 read timeout (pub/sub blocks until data arrives)
	client := NewClusterClient(addrs, password, 5, 0)

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
					"error", err,
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

// parseTick extracts Symbol and per-group prices from a Redis pub/sub message.
//
// Channel format: "tick:EURUSD"  → Symbol = "EURUSD"
// Payload format: "Raw:1.10010,1.10020|Standard:1.09980,1.10050|VIP:1.09995,1.10035"
func parseTick(channel, payload string) (Tick, error) {
	chanParts := strings.SplitN(channel, ":", 2)
	if len(chanParts) != 2 || chanParts[1] == "" {
		return Tick{}, fmt.Errorf("invalid channel format: %q (expected tick:<SYMBOL>)", channel)
	}
	symbol := chanParts[1]

	groupChunks := strings.Split(payload, "|")
	if len(groupChunks) == 0 {
		return Tick{}, fmt.Errorf("empty tick payload for symbol %q", symbol)
	}

	groupPrices := make(map[string]GroupPrice, len(groupChunks))

	for _, chunk := range groupChunks {
		// chunk = "GroupName:BID,ASK"
		gp := strings.SplitN(chunk, ":", 2)
		if len(gp) != 2 || gp[0] == "" || gp[1] == "" {
			slog.Warn("skipping malformed group chunk in tick",
				"symbol", symbol,
				"chunk", chunk,
			)
			continue
		}
		groupName := gp[0]

		// Split all comma-separated values. The pricing-service sends 5 fields:
		// "Bid,Ask,High,Low,PctChange"
		// We only need Bid (index 0) and Ask (index 1) for risk calculations.
		// High, Low, PctChange are for the frontend Fat Tick — ignored here.
		prices := strings.Split(gp[1], ",")
		if len(prices) < 2 {
			slog.Warn("skipping malformed prices in group chunk",
				"symbol", symbol,
				"group", groupName,
				"chunk", chunk,
			)
			continue
		}

		bid, err := strconv.ParseFloat(strings.TrimSpace(prices[0]), 64)
		if err != nil {
			slog.Warn("skipping group: cannot parse bid",
				"symbol", symbol, "group", groupName, "raw", prices[0])
			continue
		}
		ask, err := strconv.ParseFloat(strings.TrimSpace(prices[1]), 64)
		if err != nil {
			slog.Warn("skipping group: cannot parse ask",
				"symbol", symbol, "group", groupName, "raw", prices[1])
			continue
		}
		if bid <= 0 || ask <= 0 || ask < bid {
			slog.Warn("skipping group: invalid bid/ask values",
				"symbol", symbol, "group", groupName, "bid", bid, "ask", ask)
			continue
		}

		groupPrices[groupName] = GroupPrice{Bid: bid, Ask: ask}
	}

	if len(groupPrices) == 0 {
		return Tick{}, fmt.Errorf("no valid group prices parsed from payload %q for symbol %q", payload, symbol)
	}

	return Tick{Symbol: symbol, GroupPrices: groupPrices}, nil
}

// Close cleanly disconnects the Redis cluster client.
func (s *Subscriber) Close() error {
	return s.client.Close()
}
