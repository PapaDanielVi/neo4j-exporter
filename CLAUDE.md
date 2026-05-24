# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Project Overview

**neo4j-exporter** is a Prometheus exporter for Neo4j graph databases. It queries Neo4j via the Bolt protocol and exposes metrics in Prometheus format. Written in Go 1.22+.

Three operational modes:
- **Standalone** (`/metrics`) — persistent driver, pre-registered collector
- **Proxy** (`/scrape?target=bolt://host:7687`) — ephemeral registry per request
- **Service Discovery** (`/sd`) — returns Prometheus HTTP_SD JSON for cluster databases

## Build, Test, Lint

```bash
go build -o neo4j-exporter ./cmd/neo4j-exporter
go test -race ./...
go test -race -run TestFoo ./pkg/collector   # single test
golangci-lint run
```

## Architecture

### Entry Point

`cmd/neo4j-exporter/main.go` — parses CLI flags, creates driver pool, optionally loads Lua engine and custom YAML queries, registers HTTP handlers, starts server (default `:9121`).

### Package Layout

| Package | Purpose |
|---------|---------|
| `pkg/collector/` | Core Prometheus collector (`Describe` + `Collect`). Spawns ~20 goroutines in parallel per scrape — one per metric group (JMX, NIO, Bolt, page cache, transactions, etc.). 10s scrape timeout, 2s transaction timeout. |
| `pkg/config/` | CLI flag and env var parsing via kingpin |
| `pkg/driverpool/` | Thread-safe cached Neo4j driver pool with double-checked locking. Background reaper evicts idle drivers after 5 min. Max 5 connections per driver. |
| `pkg/discovery/` | Runs `SHOW DATABASES` on system DB, returns HTTP_SD targets |
| `pkg/luaengine/` | Loads `.lua` scripts, exposes `neo4j.query()` and `prometheus_record_gauge()` to Lua state |

### Key Patterns

- **JMX queries**: `jmxQuery()` (single attribute) and `jmxQueryMulti()` (multiple attributes) via `dbms.queryJmx()` Cypher procedure. `jmxValue()` safely extracts float64 from int64, float64, or nested `{"value": ...}` maps.
- **Custom YAML metrics**: Defined in a YAML file with query, metric_name, type, help, labels. Loaded via `LoadCustomQueries()` in `pkg/collector/custom.go`.
- **Password handling**: Passwords are read from files (`--neo4j.password-file`), never from CLI flags or env vars directly.

### CI/CD

- **CI** (`.github/workflows/ci.yml`): `golangci-lint`, `go test -race -cover`, `gosec` security scan, `go build`
- **Release** (`.github/workflows/release.yml`): GoReleaser on `v*` tag — multi-arch binaries + multi-arch Docker images to GHCR
- **Helm chart**: `charts/neo4j-exporter/` for Kubernetes deployment

### Examples

- `examples/custom_queries.yaml` — example YAML custom queries
- `examples/custom_logic.lua` — example Lua custom metric script
