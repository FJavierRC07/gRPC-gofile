# ── Build stage ───────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY gen/ ./gen/
COPY internal/ ./internal/
COPY cmd/ ./cmd/

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /file-service ./cmd/server

# Build grpc_health_probe for the Docker Compose / Kubernetes health check.
RUN go install github.com/grpc-ecosystem/grpc-health-probe@v0.4.28

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /file-service       /file-service
COPY --from=builder /go/bin/grpc-health-probe /grpc_health_probe

USER nonroot:nonroot

ENTRYPOINT ["/file-service"]
