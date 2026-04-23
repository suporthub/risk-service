# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile — risk-service
#
# Multi-stage build:
#   Stage 1 (builder): compiles the Go binary with CGO disabled for a
#     fully static binary — no libc dependency, smallest possible image.
#   Stage 2 (runtime): copies the binary into a minimal distroless image.
#     No shell, no package manager, no attack surface.
#
# Image size: ~8MB (distroless base + Go binary)
# Build time: ~30s (layer caching makes subsequent builds ~5s)
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy dependency manifests first — Docker layer cache means this layer is only
# rebuilt when go.mod or go.sum changes, not on every source file change.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build. CGO_ENABLED=0 produces a fully static binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /risk-service ./cmd/server

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

# Copy the compiled binary from the builder stage.
COPY --from=builder /risk-service /risk-service

# The risk-service exposes no HTTP or gRPC server port itself —
# it is a pure consumer + gRPC client. No EXPOSE directive needed.

# Run as non-root (uid 65532) — distroless nonroot image default.
USER nonroot:nonroot

ENTRYPOINT ["/risk-service"]
