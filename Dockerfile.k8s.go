# Dockerfile for Kubernetes load-runner (Go implementation; image includes app; no volume mount needed)
# Build from repo root with benchmark-go as context: docker build -f Dockerfile.k8s.go -t load-runner-go:latest benchmark-go
# Exec in and run: /app/loadrunner --database postgres --duration 60
# Or: /app/loadrunner --database clickhouse --duration 60

FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY benchmark-go/ .
RUN CGO_ENABLED=0 go build -o loadrunner ./benchmark-go/cmd/loadrunner

FROM alpine:3.19
RUN apk add --no-cache ca-certificates procps
WORKDIR /app
COPY --from=builder /app/loadrunner .
CMD ["sleep", "infinity"]
