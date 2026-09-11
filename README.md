# neo4j-exporter

[![CI](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/test.yml/badge.svg)](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/test.yml)
[![Lint](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/lint.yml/badge.svg)](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/lint.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/PapaDanielVi/neo4j-exporter)](https://goreportcard.com/report/github.com/PapaDanielVi/neo4j-exporter)
[![GitHub release](https://img.shields.io/github/v/release/PapaDanielVi/neo4j-exporter)](https://github.com/PapaDanielVi/neo4j-exporter/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Reference](https://pkg.go.dev/badge/github.com/PapaDanielVi/neo4j-exporter.svg)](https://pkg.go.dev/github.com/PapaDanielVi/neo4j-exporter)

Prometheus exporter for Neo4j graph databases. Exposes metrics from Neo4j over the
Bolt protocol: JVM (memory, GC, threads, buffer pools), operating system, database
topology and health, memory pools, index and constraint state, Graph Data Science
(GDS), optional APOC store/id/transaction metrics, and custom Cypher queries. Works
against Community Edition.

## Features

- **Standalone mode** — scrape a single local Neo4j instance via `/metrics`
- **Proxy mode** — scrape any Neo4j instance on-the-fly via `/scrape?target=bolt://host:7687`
- **Service discovery** — `/sd` returns Prometheus HTTP_SD JSON for all databases on a cluster
- **Concurrent collection** — all metric groups run in parallel goroutines per scrape
- **Cached driver pool** — Bolt connections are reused and idle drivers reaped after 5 minutes
- **Custom metrics** — define metrics via YAML
- **GDS monitoring** — Graph Data Science JVM heap, CPU, memory, and ongoing procedures
- **Zero-secret flags** — passwords read from files, never from process lists
- **Multi-platform** — Linux, macOS, Windows on amd64 and arm64; distroless Docker images

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

### Automated Installation Scripts

Pre-built installation scripts are available for Linux and Windows that automatically detect your system architecture and install the latest release.

#### Linux (Alpine, Debian/Ubuntu, RHEL/CentOS/Fedora)

```bash
# Download and run the installation script
curl -sL https://raw.githubusercontent.com/PapaDanielVi/neo4j-exporter/main/scripts/install-linux.sh | sudo bash

# Or with custom install directory
curl -sL https://raw.githubusercontent.com/PapaDanielVi/neo4j-exporter/main/scripts/install-linux.sh | sudo INSTALL_DIR=/opt/neo4j-exporter bash
```

The script automatically:
- Detects your architecture (x86_64 or arm64)
- Detects your package manager (apk, apt, dnf/yum)
- Downloads the appropriate package format
- Installs or upgrades the binary

#### Windows

```powershell
# Download and run the installation script
iex (new-object net.webclient).DownloadString('https://raw.githubusercontent.com/PapaDanielVi/neo4j-exporter/main/scripts/install-windows.ps1')

# Or with custom install directory
$env:INSTALL_DIR = "C:\tools\neo4j-exporter"
iex (new-object net.webclient).DownloadString('https://raw.githubusercontent.com/PapaDanielVi/neo4j-exporter/main/scripts/install-windows.ps1')
```

The script automatically:
- Detects your architecture (x86_64 or arm64)
- Downloads the tar.gz archive
- Extracts and installs the binary

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

All metrics are collected over Bolt, so the exporter works against Community
Edition. It pulls from the standard `java.lang:*` JMX beans, Cypher `SHOW`
commands, `dbms.listPools()`, GDS procedures, and (when installed) APOC monitor
procedures.

Some Neo4j telemetry is produced only by the Enterprise metrics subsystem and
has no Community equivalent over Bolt: page cache hit/fault/eviction counters,
checkpoint and log-rotation timings, Cypher replan stats, Bolt connection
counters, query latency histograms, and clustering/raft metrics. Those are not
exposed here. Store size, entity counts, and cumulative transaction counters are
available on Community only when APOC is installed.

### Exporter Self-Metrics

| Metric                                   | Type  | Description              |
| ---------------------------------------- | ----- | ------------------------ |
| `neo4j_exporter_up`                      | Gauge | 1 if target is reachable |
| `neo4j_exporter_scrape_duration_seconds` | Gauge | Scrape latency by phase  |
| `neo4j_exporter_driver_pool_active`      | Gauge | Cached driver count      |

### NIO Buffer Pools

| Metric                                 | Type  | Labels | Description                     |
| -------------------------------------- | ----- | ------ | ------------------------------- |
| `neo4j_jvm_buffer_pool_used_bytes`     | Gauge | pool   | Off-heap buffer pool used bytes |
| `neo4j_jvm_buffer_pool_capacity_bytes` | Gauge | pool   | Off-heap buffer pool capacity   |
| `neo4j_jvm_buffer_pool_count`          | Gauge | pool   | Number of buffers in pool       |

### JVM Threading

| Metric                     | Type  | Description         |
| -------------------------- | ----- | ------------------- |
| `neo4j_jvm_threads_peak`   | Gauge | Peak thread count   |
| `neo4j_jvm_threads_daemon` | Gauge | Daemon thread count |
| `neo4j_jvm_threads_total`  | Gauge | Total live threads  |

### JVM Class Loading

| Metric                             | Type    | Description              |
| ---------------------------------- | ------- | ------------------------ |
| `neo4j_jvm_classes_loaded`         | Gauge   | Currently loaded classes |
| `neo4j_jvm_classes_unloaded_total` | Counter | Total unloaded classes   |

### JVM Runtime

| Metric                     | Type  | Description        |
| -------------------------- | ----- | ------------------ |
| `neo4j_jvm_uptime_seconds` | Gauge | JVM uptime seconds |

### JVM Memory

| Metric                              | Type  | Labels | Description                  |
| ----------------------------------- | ----- | ------ | ---------------------------- |
| `neo4j_jvm_heap_used_bytes`         | Gauge |        | Used heap memory             |
| `neo4j_jvm_heap_committed_bytes`    | Gauge |        | Committed heap memory        |
| `neo4j_jvm_heap_max_bytes`          | Gauge |        | Maximum heap memory          |
| `neo4j_jvm_heap_init_bytes`         | Gauge |        | Initial heap memory          |
| `neo4j_jvm_nonheap_used_bytes`      | Gauge |        | Used non-heap memory         |
| `neo4j_jvm_nonheap_committed_bytes` | Gauge |        | Committed non-heap memory    |
| `neo4j_jvm_nonheap_max_bytes`       | Gauge |        | Maximum non-heap memory      |
| `neo4j_jvm_memory_pool_used_bytes`  | Gauge | pool   | Used memory per memory pool  |
| `neo4j_jvm_memory_pool_committed_bytes` | Gauge | pool | Committed memory per pool   |
| `neo4j_jvm_memory_pool_max_bytes`   | Gauge | pool   | Max memory per memory pool   |

### JVM Garbage Collection

| Metric                                  | Type    | Labels | Description                 |
| --------------------------------------- | ------- | ------ | --------------------------- |
| `neo4j_jvm_gc_collection_count_total`   | Counter | gc     | Total GC collections        |
| `neo4j_jvm_gc_collection_seconds_total` | Counter | gc     | Total time spent in GC      |

### Operating System

| Metric                                   | Type  | Description                          |
| ---------------------------------------- | ----- | ------------------------------------ |
| `neo4j_jvm_process_cpu_load`             | Gauge | Process CPU load (0..1)              |
| `neo4j_jvm_system_cpu_load`              | Gauge | Host CPU load (0..1)                 |
| `neo4j_jvm_open_file_descriptors`        | Gauge | Open file descriptors                |
| `neo4j_jvm_max_file_descriptors`         | Gauge | Maximum allowed file descriptors     |
| `neo4j_jvm_free_physical_memory_bytes`   | Gauge | Free physical memory on the host     |
| `neo4j_jvm_committed_virtual_memory_bytes` | Gauge | Committed virtual memory           |
| `neo4j_jvm_system_load_average`          | Gauge | System load average (1 min)          |
| `neo4j_jvm_available_processors`         | Gauge | Processors available to the JVM      |

### Database Topology & Health

| Metric                               | Type  | Labels           | Source             |
| ------------------------------------ | ----- | ---------------- | ------------------ |
| `neo4j_database_online`              | Gauge | database, role   | `SHOW DATABASES`   |
| `neo4j_database_transactions_active` | Gauge | database         | `SHOW TRANSACTIONS`|
| `neo4j_dbms_pool_used_heap_bytes`    | Gauge | pool, database   | `dbms.listPools()` |
| `neo4j_dbms_pool_used_native_bytes`  | Gauge | pool, database   | `dbms.listPools()` |
| `neo4j_indexes_total`                | Gauge |                  | `SHOW INDEXES`     |
| `neo4j_indexes_online`               | Gauge |                  | `SHOW INDEXES`     |
| `neo4j_indexes_failed`               | Gauge |                  | `SHOW INDEXES`     |
| `neo4j_constraints_total`            | Gauge |                  | `SHOW CONSTRAINTS` |

### APOC-Derived (optional)

These are emitted only when APOC is installed on the target. Detection is automatic.

| Metric                                  | Type    | Labels | Source                |
| --------------------------------------- | ------- | ------ | --------------------- |
| `neo4j_store_size_bytes`                | Gauge   | type   | `apoc.monitor.store()`|
| `neo4j_ids_in_use`                      | Gauge   | kind   | `apoc.monitor.ids()`  |
| `neo4j_transactions_committed_total`    | Counter |        | `apoc.monitor.tx()`   |
| `neo4j_transactions_opened_total`       | Counter |        | `apoc.monitor.tx()`   |
| `neo4j_transactions_rolled_back_total`  | Counter |        | `apoc.monitor.tx()`   |
| `neo4j_transactions_open`               | Gauge   |        | `apoc.monitor.tx()`   |
| `neo4j_transactions_peak_concurrent`    | Gauge   |        | `apoc.monitor.tx()`   |
| `neo4j_last_committed_tx_id`            | Gauge   |        | `apoc.monitor.tx()`   |

### GDS — Graph Data Science

| Metric                                        | Type  | Labels | Description                            |
| --------------------------------------------- | ----- | ------ | -------------------------------------- |
| `neo4j_gds_jvm_free_heap_bytes`               | Gauge |        | Free JVM heap from GDS system monitor  |
| `neo4j_gds_jvm_total_heap_bytes`              | Gauge |        | Total JVM heap from GDS system monitor |
| `neo4j_gds_jvm_max_heap_bytes`                | Gauge |        | Max JVM heap from GDS system monitor   |
| `neo4j_gds_jvm_available_cpu_cores`           | Gauge |        | Logical CPU cores available to JVM     |
| `neo4j_gds_available_cpu_cores_not_requested` | Gauge |        | CPU cores not requested by GDS         |
| `neo4j_gds_ongoing_procedures`                | Gauge |        | Currently running GDS procedures       |
| `neo4j_gds_graph_memory_bytes`                | Gauge |        | Memory used by GDS projected graphs    |
| `neo4j_gds_task_memory_bytes`                 | Gauge |        | Memory estimated for running GDS tasks |

### Advanced Metrics

| Metric                                   | Type  | Source              |
| ---------------------------------------- | ----- | ------------------- |
| `neo4j_dbms_heavy_queries_active`        | Gauge | `SHOW TRANSACTIONS` |
| `neo4j_dbms_heavy_queries_page_faults`   | Gauge | `SHOW TRANSACTIONS` |
| `neo4j_synthetic_query_duration_seconds` | Gauge | Canary query        |

### QPS (PromQL)

Requires APOC (for the cumulative transaction counters):

```
rate(neo4j_transactions_committed_total[1m]) + rate(neo4j_transactions_rolled_back_total[1m])
```

## Grafana Dashboard

A pre-built Grafana dashboard is included at [`examples/grafana-dashboard.json`](examples/grafana-dashboard.json).
Import it directly into Grafana to visualize Neo4j metrics out of the box.

## Custom Metrics (YAML)

Create a `custom_queries.yaml`:

```yaml
custom_queries:
  - query: "MATCH (u:User {status: 'suspended'}) RETURN count(u) as count"
    metric_name: "neo4j_custom_suspended_users_total"
    type: "gauge"
    help: "Total suspended user profiles"
```

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

The chart deploys a Deployment, Service, ServiceMonitor, and ConfigMap.
See [`charts/neo4j-exporter/values.yaml`](charts/neo4j-exporter/values.yaml) for all options.

## Ansible Playbook

Ansible support is provided for production-ready deployments on traditional infrastructure (systemd service or Docker container).

For full installation, configuration, inventory setup, and troubleshooting documentation, see the [Ansible Role Manual](ansible/roles/neo4j_exporter/README.md).


## Supported Neo4j Versions

Neo4j 5.x (Community and Enterprise). Some JMX-based metrics may require Neo4j Enterprise
edition. GDS metrics require the Graph Data Science plugin.

## Building

```bash
go build -o neo4j-exporter ./cmd/neo4j-exporter
go test -race ./...
golangci-lint run
```

Multi-arch binaries and Docker images (amd64, arm64) are published on every release
via [GoReleaser](.goreleaser.yaml).

## Contributing

Contributions are welcome! Please open an issue or pull request on GitHub.

## License

MIT License
