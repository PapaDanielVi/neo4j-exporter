# neo4j-exporter

[![CI](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/test.yml/badge.svg)](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/test.yml)
[![Lint](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/lint.yml/badge.svg)](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/lint.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/PapaDanielVi/neo4j-exporter)](https://goreportcard.com/report/github.com/PapaDanielVi/neo4j-exporter)
[![GitHub release](https://img.shields.io/github/v/release/PapaDanielVi/neo4j-exporter)](https://github.com/PapaDanielVi/neo4j-exporter/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Reference](https://pkg.go.dev/badge/github.com/PapaDanielVi/neo4j-exporter.svg)](https://pkg.go.dev/github.com/PapaDanielVi/neo4j-exporter)

Prometheus exporter for Neo4j databases. Exposes metrics from Neo4j JMX beans,
transaction state, page cache, JVM, and custom Cypher queries.

## Features

- **Standalone mode** — scrape a single local Neo4j instance via `/metrics`
- **Proxy mode** — scrape any Neo4j instance on-the-fly via `/scrape?target=bolt://host:7687`
- **Service discovery** — `/sd` returns Prometheus HTTP_SD JSON for all databases on a cluster
- **Concurrent collection** — all queries run in parallel with read-only routing and 2s transaction timeouts
- **Cached driver pool** — connections are reused and idle drivers reaped after 5 minutes
- **Custom metrics** — define metrics via YAML or Lua scripts
- **Zero-secret flags** — passwords read from files, never from process lists

## Quick Start

### Docker

```bash
docker run -p 9121:9121 \
  -e NEO4J_PASSWORD=secret \
  ghcr.io/PapaDanielVi/neo4j-exporter:latest \
  --neo4j.uri=bolt://neo4j-host:7687
```

### Binary

```bash
go build -o neo4j-exporter ./cmd/neo4j-exporter
./neo4j-exporter --neo4j.uri=bolt://localhost:7687 --neo4j.password-file=/run/secrets/neo4j-password
```

### Docker Compose

```yaml
services:
  neo4j:
    image: neo4j:5
    environment:
      NEO4J_AUTH: neo4j/secret
    ports:
      - "7687:7687"

  exporter:
    image: ghcr.io/PapaDanielVi/neo4j-exporter:latest
    command:
      - "--neo4j.uri=bolt://neo4j:7687"
    environment:
      NEO4J_PASSWORD: secret
    ports:
      - "9121:9121"
```

## Configuration

| Flag                    | Environment Variable            | Default                 | Description                 |
| ----------------------- | ------------------------------- | ----------------------- | --------------------------- |
| `--web.listen-address`  | `NEO4J_EXPORTER_LISTEN_ADDRESS` | `:9121`                 | HTTP listen address         |
| `--neo4j.uri`           | `NEO4J_URI`                     | `bolt://localhost:7687` | Neo4j bolt URI              |
| `--neo4j.user`          | `NEO4J_USER`                    | `neo4j`                 | Neo4j username              |
| `--neo4j.password`      | `NEO4J_PASSWORD`                |                         | Password (prefer file)      |
| `--neo4j.password-file` | `NEO4J_PASSWORD_FILE`           |                         | Path to password file       |
| `--sd.primary-uri`      | `NEO4J_SD_PRIMARY_URI`          |                         | Primary URI for `/sd`       |
| `--custom-queries-file` | `NEO4J_EXPORTER_CUSTOM_QUERIES` | `custom_queries.yaml`   | YAML custom metrics         |
| `--lua-scripts-dir`     | `NEO4J_EXPORTER_LUA_DIR`        |                         | Directory of `.lua` scripts |
| `--log.json`            |                                 | `false`                 | JSON log output             |

## Endpoints

| Endpoint                          | Description                                                 |
| --------------------------------- | ----------------------------------------------------------- |
| `/metrics`                        | Standalone mode metrics                                     |
| `/scrape?target=bolt://host:7687` | Proxy mode — scrape a specific target                       |
| `/sd`                             | Service discovery — returns all databases as scrape targets |
| `/healthz`                        | Liveness probe                                              |
| `/readyz`                         | Readiness probe                                             |

## Metrics

### Exporter Self-Metrics

| Metric                                   | Type  | Description              |
| ---------------------------------------- | ----- | ------------------------ |
| `neo4j_exporter_up`                      | Gauge | 1 if target is reachable |
| `neo4j_exporter_scrape_duration_seconds` | Gauge | Scrape latency by phase  |
| `neo4j_exporter_driver_pool_active`      | Gauge | Cached driver count      |

### Core Database Metrics

| Metric                                       | Type    | Source               |
| -------------------------------------------- | ------- | -------------------- |
| `neo4j_database_count_node`                  | Gauge   | JMX Primitive count  |
| `neo4j_database_count_relationship`          | Gauge   | JMX Primitive count  |
| `neo4j_database_count_property`              | Gauge   | JMX Primitive count  |
| `neo4j_database_transaction_committed_total` | Counter | JMX Transactions     |
| `neo4j_database_transaction_rollbacks_total` | Counter | JMX Transactions     |
| `neo4j_database_transaction_active`          | Gauge   | JMX Transactions     |
| `neo4j_dbms_page_cache_hits_total`           | Counter | JMX Page cache       |
| `neo4j_dbms_page_cache_faults_total`         | Counter | JMX Page cache       |
| `neo4j_dbms_page_cache_flushes_total`        | Counter | JMX Page cache       |
| `neo4j_database_store_size_bytes_total`      | Gauge   | JMX Store file sizes |
| `neo4j_jvm_memory_pool_used_bytes`           | Gauge   | JMX MemoryPool       |
| `neo4j_jvm_gc_collection_seconds_total`      | Counter | JMX GarbageCollector |
| `neo4j_jvm_cpu_load_process`                 | Gauge   | JMX OperatingSystem  |
| `neo4j_os_open_file_descriptors`             | Gauge   | JMX OperatingSystem  |

### Advanced Metrics

| Metric                                   | Type  | Source              |
| ---------------------------------------- | ----- | ------------------- |
| `neo4j_dbms_heavy_queries_active`        | Gauge | `SHOW TRANSACTIONS` |
| `neo4j_dbms_heavy_queries_page_faults`   | Gauge | `SHOW TRANSACTIONS` |
| `neo4j_synthetic_query_duration_seconds` | Gauge | Canary `RETURN 1`   |

### QPS (PromQL)

```
rate(neo4j_database_transaction_committed_total[1m]) + rate(neo4j_database_transaction_rollbacks_total[1m])
```

## Custom Metrics (YAML)

Create a `custom_queries.yaml`:

```yaml
custom_queries:
  - query: "MATCH (u:User {status: 'suspended'}) RETURN count(u) as count"
    metric_name: "neo4j_custom_suspended_users_total"
    type: "gauge"
    help: "Total suspended user profiles"
```

## Custom Metrics (Lua)

Place `.lua` files in the directory specified by `--lua-scripts-dir`:

```lua
local records = neo4j.query([[
  MATCH (o:Order)
  WHERE o.created_at > timestamp() - 60000
  RETURN o.payment_method, sum(o.amount) as total
]])

for _, row in ipairs(records) do
    prometheus_record_gauge("neo4j_sales_volume_bytes", row["total"], {
        method = row["payment_method"]
    })
end
```

Two functions are available in Lua:
- `neo4j.query(cypher)` — executes a read-only query, returns a table of rows
- `prometheus_record_gauge(name, value, labels)` — records a gauge metric

## Prometheus Configuration

### Standalone

```yaml
scrape_configs:
  - job_name: neo4j
    static_configs:
      - targets: ['localhost:9121']
```

### Proxy with Service Discovery

```yaml
scrape_configs:
  - job_name: neo4j-sd
    http_sd_configs:
      - url: http://localhost:9121/sd
```

## Helm Chart

```bash
helm install neo4j-exporter ./charts/neo4j-exporter \
  --set neo4j.uri=bolt://neo4j:7687 \
  --set neo4j.existingSecret=neo4j-credentials
```

## Building

```bash
go build -o neo4j-exporter ./cmd/neo4j-exporter
go test -race ./...
```

## License

MIT License
