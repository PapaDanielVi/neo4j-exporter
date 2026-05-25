#!/usr/bin/env bash
# scrape_all_metrics.sh — Start Neo4j via Docker Compose, run the exporter,
# scrape all metrics, and save them to a timestamped output file.
#
# Usage:
#   ./scrape_all_metrics.sh [--keep]   # --keep prevents tearing down Neo4j afterwards
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.neo4j.yml"
OUTPUT_FILE="$SCRIPT_DIR/all_metrics_$(date +%Y%m%d_%H%M%S).txt"
EXPORTER_PID=""
KEEP=false

for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=true ;;
    *) echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

cleanup() {
  echo ""
  echo "=== Cleaning up ==="
  if [ -n "$EXPORTER_PID" ] && kill -0 "$EXPORTER_PID" 2>/dev/null; then
    echo "Stopping exporter (PID $EXPORTER_PID)..."
    kill "$EXPORTER_PID" 2>/dev/null || true
    wait "$EXPORTER_PID" 2>/dev/null || true
  fi
  if ! $KEEP; then
    echo "Stopping Neo4j..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
  else
    echo "Neo4j left running (docker compose -f $COMPOSE_FILE down -v to stop)."
  fi
}
trap cleanup EXIT

echo "=== Step 1: Starting Neo4j via Docker Compose ==="
docker compose -f "$COMPOSE_FILE" up -d

echo "=== Step 2: Waiting for Neo4j to be healthy ==="
echo -n "Waiting"
for i in $(seq 1 60); do
  if docker inspect --format='{{.State.Health.Status}}' neo4j-exporter-test 2>/dev/null | grep -q "healthy"; then
    echo " healthy!"
    break
  fi
  echo -n "."
  sleep 2
done

# Extra wait for Bolt to be fully ready
echo "Waiting additional 10s for Bolt protocol to be ready..."
sleep 10

echo "=== Step 3: Detecting Neo4j version ==="
NEO4J_VERSION=$(docker exec neo4j-exporter-test cypher-shell -u neo4j -p testpassword123 "CALL dbms.components() YIELD versions, edition RETURN versions[0] AS version, edition" --format plain 2>/dev/null | tail -1 | tr -d '[:space:]' || echo "unknown")
echo "Detected Neo4j: $NEO4J_VERSION"

echo "=== Step 4: Starting neo4j-exporter ==="
cd "$PROJECT_DIR"
./neo4j-exporter \
  --neo4j.uri=bolt://localhost:7687 \
  --neo4j.user=neo4j \
  --neo4j.password=testpassword123 \
  --web.listen-address=9121 &
EXPORTER_PID=$!
echo "Exporter started with PID $EXPORTER_PID"

echo "Waiting 3s for exporter to start..."
sleep 3

echo "=== Step 5: Scraping /metrics (standalone mode) ==="
{
  echo "=============================================="
  echo "  neo4j-exporter — All Metrics Snapshot"
  echo "  Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo "  Neo4j: ${NEO4J_VERSION}"
  echo "  Mode: standalone (/metrics)"
  echo "=============================================="
  echo ""
  curl -s http://localhost:9121/metrics
} > "$OUTPUT_FILE"

echo ""
echo "=== Step 6: Scraping /scrape (proxy mode) ==="
{
  echo ""
  echo "=============================================="
  echo "  Proxy Mode (/scrape?target=bolt://localhost:7687)"
  echo "=============================================="
  echo ""
  curl -s "http://localhost:9121/scrape?target=bolt://localhost:7687"
} >> "$OUTPUT_FILE"

METRIC_COUNT=$(grep -c "^neo4j_" "$OUTPUT_FILE" || true)
echo ""
echo "=== Done ==="
echo "Output file: $OUTPUT_FILE"
echo "Total neo4j_ metric lines: $METRIC_COUNT"
echo ""
echo "=== Metric families summary ==="
grep "^# HELP neo4j_" "$OUTPUT_FILE" | sort -u | while read -r line; do
  family=$(echo "$line" | sed 's/# HELP //')
  count=$(grep -c "^${family}" "$OUTPUT_FILE" || true)
  echo "  $family  ($count lines)"
done
