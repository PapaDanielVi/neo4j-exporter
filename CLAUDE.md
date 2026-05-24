# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

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

| Package           | Purpose                                                                                                                                                                                                               |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pkg/collector/`  | Core Prometheus collector (`Describe` + `Collect`). Spawns ~20 goroutines in parallel per scrape — one per metric group (JMX, NIO, Bolt, page cache, transactions, etc.). 10s scrape timeout, 2s transaction timeout. |
| `pkg/config/`     | CLI flag and env var parsing via kingpin                                                                                                                                                                              |
| `pkg/driverpool/` | Thread-safe cached Neo4j driver pool with double-checked locking. Background reaper evicts idle drivers after 5 min. Max 5 connections per driver.                                                                    |
| `pkg/discovery/`  | Runs `SHOW DATABASES` on system DB, returns HTTP_SD targets                                                                                                                                                           |
| `pkg/luaengine/`  | Loads `.lua` scripts, exposes `neo4j.query()` and `prometheus_record_gauge()` to Lua state                                                                                                                            |

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



# CLAUDE.md

This file provides high-level guidance to Claude Code.

## Project Overview

Money is Snapp's main backend service that handles passengers/drivers wallets, money transfers, and tracks all financial transactions. It's built with the Echo web framework and integrates with multiple banks and internal Snapp services.

Key integrations: MySQL, PostgreSQL, Redis, NATS, RabbitMQ, and various bank SDKs.

## Persistent Context & Rules Update Protocol

When new information or project requirements are established, proactively update the relevant Markdown files in `.claude/contexts/` and `.claude/rules/` to ensure long-term persistence and alignment across sessions.
