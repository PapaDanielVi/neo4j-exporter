# Neo4j Exporter Ansible Deployment

This directory contains an Ansible playbook for deploying neo4j-exporter to production systems.

## Quick Start

```bash
# Install dependencies
ansible-galaxy collection install community.docker -p ./collections

# Configure inventory
cp inventory.ini inventory.ini.local
# Edit inventory.ini.local with your target hosts

# Create vault for secrets
ansible-vault create group_vars/neo4j_exporter/vault.yml
# Add: neo4j_exporter_neo4j_password: your-password

# Run the playbook
ansible-playbook -i inventory.ini.local playbook.yml --ask-vault-pass
```

For full documentation, see the [main README.md](../README.md) Ansible section.