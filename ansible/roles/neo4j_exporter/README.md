
# Ansible Playbook

Ansible support is provided for production-ready deployments on traditional infrastructure.

## Prerequisites

- Ansible 2.10 or later
- Target hosts running Linux (Alpine, Debian, Ubuntu, RHEL, CentOS, or compatible)
- Password for Neo4j database (handled securely via Ansible Vault or environment)
- Network connectivity to Neo4j Bolt endpoint (default: `bolt://localhost:7687`)

## Installation

```bash
# Clone the repository
git clone https://github.com/PapaDanielVi/neo4j-exporter.git
cd neo4j-exporter/ansible

# Install required collections (if not already installed)
ansible-galaxy collection install community.docker geerlingguy.docker

# Verify Ansible is working
ansible --version
```

## Inventory Configuration

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

| Variable                             | Default                                      | Description                             |
| ------------------------------------ | -------------------------------------------- | --------------------------------------- |
| `neo4j_exporter_deployment_method`   | `binary`                                     | Deployment method: `binary` or `docker` |
| `neo4j_exporter_install_dir`         | `/opt/neo4j-exporter`                        | Installation directory for binary       |
| `neo4j_exporter_binary_url`          | GitHub releases URL                          | URL to download the binary              |
| `neo4j_exporter_listen_address`      | `:9121`                                      | HTTP listen address                     |
| `neo4j_exporter_neo4j_uri`           | `bolt://localhost:7687`                      | Neo4j Bolt URI                          |
| `neo4j_exporter_neo4j_user`          | `neo4j`                                      | Neo4j username                          |
| `neo4j_exporter_neo4j_password`      | `null`                                       | Neo4j password (use vault)              |
| `neo4j_exporter_neo4j_password_file` | `/etc/neo4j-exporter/password`               | Password file path                      |
| `neo4j_exporter_sd_primary_uri`      | `null`                                       | Primary URI for service discovery       |
| `neo4j_exporter_log_json`            | `false`                                      | Enable JSON logging                     |
| `neo4j_exporter_docker_image`        | `ghcr.io/PapaDanielVi/neo4j-exporter:latest` | Docker image for container deployment   |

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
