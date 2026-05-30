# neo4j-exporter

[![CI](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/test.yml/badge.svg)](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/test.yml)
[![Lint](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/lint.yml/badge.svg)](https://github.com/PapaDanielVi/neo4j-exporter/actions/workflows/lint.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/PapaDanielVi/neo4j-exporter)](https://goreportcard.com/report/github.com/PapaDanielVi/neo4j-exporter)
[![GitHub release](https://img.shields.io/github/v/release/PapaDanielVi/neo4j-exporter)](https://github.com/PapaDanielVi/neo4j-exporter/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Reference](https://pkg.go.dev/badge/github.com/PapaDanielVi/neo4j-exporter.svg)](https://pkg.go.dev/github.com/PapaDanielVi/neo4j-exporter)

Prometheus exporter for Neo4j graph databases. Exposes metrics from Neo4j via the
Bolt protocol — including JVM, NIO buffer pools, Bolt connections, page cache,
transactions, Graph Data Science (GDS), and custom Cypher queries.

## Features

- **Standalone mode** — scrape a single local Neo4j instance via `/metrics`
- **Proxy mode** — scrape any Neo4j instance on-the-fly via `/scrape?target=bolt://host:7687`
- **Service discovery** — `/sd` returns Prometheus HTTP_SD JSON for all databases on a cluster
- **Concurrent collection** — all metric groups run in parallel goroutines per scrape
- **Cached driver pool** — Bolt connections are reused and idle drivers reaped after 5 minutes
- **Custom metrics** — define metrics via YAML or Lua scripts
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

### NIO Buffer Pools

| Metric                                  | Type  | Labels | Description                     |
| --------------------------------------- | ----- | ------ | ------------------------------- |
| `neo4j_jvm_buffer_pool_used_bytes`      | Gauge | pool   | Off-heap buffer pool used bytes |
| `neo4j_jvm_buffer_pool_capacity_bytes`  | Gauge | pool   | Off-heap buffer pool capacity   |
| `neo4j_jvm_buffer_pool_count`           | Gauge | pool   | Number of buffers in pool       |

### JVM Threading

| Metric                        | Type  | Description          |
| ----------------------------- | ----- | -------------------- |
| `neo4j_jvm_threads_peak`      | Gauge | Peak thread count    |
| `neo4j_jvm_threads_daemon`    | Gauge | Daemon thread count  |
| `neo4j_jvm_threads_total`     | Gauge | Total live threads   |

### JVM Class Loading

| Metric                              | Type    | Description            |
| ----------------------------------- | ------- | ---------------------- |
| `neo4j_jvm_classes_loaded`          | Gauge   | Currently loaded classes |
| `neo4j_jvm_classes_unloaded_total`  | Counter | Total unloaded classes |

### JVM Runtime

| Metric                       | Type  | Description         |
| ---------------------------- | ----- | ------------------- |
| `neo4j_jvm_uptime_seconds`   | Gauge | JVM uptime seconds  |

### GDS — Graph Data Science

| Metric                                          | Type  | Labels | Description                            |
| ----------------------------------------------- | ----- | ------ | -------------------------------------- |
| `neo4j_gds_jvm_free_heap_bytes`                 | Gauge |        | Free JVM heap from GDS system monitor  |
| `neo4j_gds_jvm_total_heap_bytes`                | Gauge |        | Total JVM heap from GDS system monitor |
| `neo4j_gds_jvm_max_heap_bytes`                  | Gauge |        | Max JVM heap from GDS system monitor   |
| `neo4j_gds_jvm_available_cpu_cores`             | Gauge |        | Logical CPU cores available to JVM     |
| `neo4j_gds_available_cpu_cores_not_requested`   | Gauge |        | CPU cores not requested by GDS         |
| `neo4j_gds_ongoing_procedures`                  | Gauge |        | Currently running GDS procedures       |
| `neo4j_gds_graph_memory_bytes`                  | Gauge |        | Memory used by GDS projected graphs    |
| `neo4j_gds_task_memory_bytes`                   | Gauge |        | Memory estimated for running GDS tasks |

### Advanced Metrics

| Metric                                   | Type  | Source              |
| ---------------------------------------- | ----- | ------------------- |
| `neo4j_dbms_heavy_queries_active`        | Gauge | `SHOW TRANSACTIONS` |
| `neo4j_dbms_heavy_queries_page_faults`   | Gauge | `SHOW TRANSACTIONS` |
| `neo4j_synthetic_query_duration_seconds` | Gauge | Canary query        |

### QPS (PromQL)

```
rate(neo4j_database_transaction_committed_total[1m]) + rate(neo4j_database_transaction_rollbacks_total[1m])
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

The chart deploys a Deployment, Service, ServiceMonitor, ConfigMap, and ServiceAccount.
See [`charts/neo4j-exporter/values.yaml`](charts/neo4j-exporter/values.yaml) for all options.

## Ansible Playbook

Ansible support is provided for production-ready deployments on traditional infrastructure.

### Prerequisites

- Ansible 2.10 or later
- Target hosts running Linux (Alpine, Debian, Ubuntu, RHEL, CentOS, or compatible)
- Password for Neo4j database (handled securely via Ansible Vault or environment)
- Network connectivity to Neo4j Bolt endpoint (default: `bolt://localhost:7687`)

### Installation

```bash
# Clone the repository
git clone https://github.com/PapaDanielVi/neo4j-exporter.git
cd neo4j-exporter/ansible

# Install required collections (if not already installed)
ansible-galaxy collection install community.docker geerlingguy.docker

# Verify Ansible is working
ansible --version
```

### Inventory Configuration

Edit `inventory.ini` to define your target hosts:

```ini
[neo4j_exporter]
# Add target hostnames or IP addresses
exporter01 ansible_host=192.168.1.10
exporter02 ansible_host=192.168.1.11

[neo4j_exporter:vars]
# Deployment method: binary or docker
neo4j_exporter_deployment_method=binary

# Neo4j connection settings
neo4j_exporter_neo4j_uri=bolt://neo4j-server:7687
neo4j_exporter_neo4j_user=neo4j

# Optional settings
neo4j_exporter_listen_address=:9121
neo4j_exporter_sd_primary_uri=bolt://neo4j-cluster:7687
neo4j_exporter_log_json=false
```

### Secrets Handling

**Recommended: Use Ansible Vault for passwords**

```bash
# Create vault file
ansible-vault create group_vars/neo4j_exporter/vault.yml

# Add the following content:
neo4j_exporter_neo4j_password: your-secure-password
```

**Alternative: Environment variables**

```bash
export NEO4J_EXPORTER_NEO4J_PASSWORD=your-secure-password
ansible-playbook playbook.yml --extra-vars "neo4j_exporter_neo4j_password=$NEO4J_EXPORTER_NEO4J_PASSWORD"
```

### Running the Playbook

Binary deployment (default):

```bash
# Basic run
ansible-playbook playbook.yml

# With vault password file
ansible-playbook playbook.yml --ask-vault-pass

# For a specific host
ansible-playbook playbook.yml --limit exporter01

# With custom variables
ansible-playbook playbook.yml -e "neo4j_exporter_neo4j_uri=bolt://remote-neo4j:7687 neo4j_exporter_neo4j_password-file=/run/secrets/neo4j-password"
```

Docker deployment:

```bash
ansible-playbook playbook.yml -e "neo4j_exporter_deployment_method=docker"
```

### Configuration Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `neo4j_exporter_deployment_method` | `binary` | Deployment method: `binary` or `docker` |
| `neo4j_exporter_install_dir` | `/opt/neo4j-exporter` | Installation directory for binary |
| `neo4j_exporter_binary_url` | GitHub releases URL | URL to download the binary |
| `neo4j_exporter_listen_address` | `:9121` | HTTP listen address |
| `neo4j_exporter_neo4j_uri` | `bolt://localhost:7687` | Neo4j Bolt URI |
| `neo4j_exporter_neo4j_user` | `neo4j` | Neo4j username |
| `neo4j_exporter_neo4j_password` | `null` | Neo4j password (use vault) |
| `neo4j_exporter_neo4j_password_file` | `/etc/neo4j-exporter/password` | Password file path |
| `neo4j_exporter_sd_primary_uri` | `null` | Primary URI for service discovery |
| `neo4j_exporter_log_json` | `false` | Enable JSON logging |
| `neo4j_exporter_docker_image` | `ghcr.io/PapaDanielVi/neo4j-exporter:latest` | Docker image for container deployment |

### Expected Outcomes

After successful deployment:

```bash
# Check service status (binary)
systemctl status neo4j-exporter

# Test health endpoint
curl http://localhost:9121/healthz
# Expected: ok

# Test metrics endpoint
curl http://localhost:9121/metrics
# Expected: Prometheus metrics output with neo4j_ prefix

# Check Docker container (docker method)
docker ps -a --filter name=neo4j-exporter
docker logs neo4j-exporter
```

### Troubleshooting

**Service fails to start:**

```bash
# Check service logs
journalctl -u neo4j-exporter -n 50 --no-pager

# Verify Neo4j connectivity from target host
telnet neo4j-server 7687

# Check password file permissions
ls -la /etc/neo4j-exporter/password
```

**Permission denied errors:**

```bash
# Ensure binary has execute permissions
ls -la /opt/neo4j-exporter/neo4j-exporter

# Check user exists
id neo4jexporter
```

**Docker container won't start:**

```bash
# Check Docker logs
docker logs neo4j-exporter

# Verify image was pulled
docker images neo4j-exporter

# Check port binding
docker port neo4j-exporter
```

**Metrics not returning data:**

```bash
# Test Neo4j connectivity manually
# Verify credentials and URI in the configuration

# Check Neo4j version compatibility
curl http://localhost:9121/metrics | grep neo4j_
```

### Directory Structure

```
ansible/
├── playbook.yml              # Main playbook entry point
├── inventory.ini             # Host inventory
├── group_vars/
│   └── neo4j_exporter/
│       └── vault.yml         # Encrypted secrets (create with ansible-vault)
└── roles/
    └── neo4j_exporter/
        ├── defaults/
        │   └── main.yml        # Default variables
        ├── handlers/
        │   └── main.yml        # Service restart handlers
        ├── tasks/
        │   ├── main.yml
        │   ├── deploy-binary.yml
        │   └── deploy-docker.yml
        └── templates/
            └── neo4j-exporter.service.j2  # Systemd unit template
```

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
