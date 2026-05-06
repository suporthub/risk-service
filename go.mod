// ─────────────────────────────────────────────────────────────────────────────
// LiveFXHub risk-service — Go module definition
//
// Module: github.com/livefxhub/risk-service
//
// Key dependencies:
//   - google.golang.org/grpc        : gRPC client (calls execution-service ForceLiquidate)
//   - google.golang.org/protobuf    : protobuf runtime for generated types
//   - github.com/redis/go-redis/v9  : Redis pub/sub subscriber for tick firehose
//   - github.com/segmentio/kafka-go : Kafka consumer (orders.executed, orders.closed)
//   - github.com/joho/godotenv      : .env loader for global risk config
//
// Intentionally EXCLUDED:
//   - github.com/jackc/pgx          : NO DATABASE — this service is fully stateless
//   - github.com/google/uuid         : not needed — ticket IDs come from Kafka events
// ─────────────────────────────────────────────────────────────────────────────

module github.com/livefxhub/risk-service

go 1.25.0

require (
	github.com/joho/godotenv v1.5.1
	github.com/redis/go-redis/v9 v9.7.3
	github.com/segmentio/kafka-go v0.4.47
	google.golang.org/grpc v1.69.4
	google.golang.org/protobuf v1.36.5
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.9.2 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
)
