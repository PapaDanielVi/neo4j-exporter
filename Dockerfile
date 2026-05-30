# Build stage
FROM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=$(git describe --tags --always 2>/dev/null || echo dev) \
    -X main.commit=$(git rev-parse --short HEAD 2>/dev/null || echo none) \
    -X main.buildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o /neo4j-exporter ./cmd/neo4j-exporter

# Runtime stage
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /neo4j-exporter /neo4j-exporter
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

USER nonroot:nonroot
EXPOSE 9121

ENTRYPOINT ["/neo4j-exporter"]
