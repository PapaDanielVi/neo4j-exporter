//go:build integration

// Package collector_test integration suite. Connects to a running Neo4j
// instance (via GitHub Actions service or examples/docker-compose.neo4j.yml)
// and asserts the collector produces a known set of metric families.
// Run with: go test -tags integration ./pkg/collector/
package collector_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/PapaDanielVi/neo4j-exporter/pkg/collector"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const defaultIntegrationPassword = "testpassword123"

func TestIntegrationCommunityMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	boltURL := os.Getenv("NEO4J_BOLT_URL")
	if boltURL == "" {
		boltURL = "bolt://localhost:7687"
	}
	password := os.Getenv("NEO4J_PASSWORD")
	if password == "" {
		password = defaultIntegrationPassword
	}

	driver, err := neo4j.NewDriverWithContext(boltURL,
		neo4j.BasicAuth("neo4j", password, ""))
	if err != nil {
		t.Fatalf("creating driver: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })

	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := driver.VerifyConnectivity(connCtx); err != nil {
		t.Fatalf("verifying connectivity: %v", err)
	}

	c := collector.New(boltURL, driver)
	mfs := gather(t, c)
	labels := map[string]string{"target": boltURL}

	// These families come from the always-available java.lang:* JMX beans and
	// the exporter self-metrics, so they must be present on Community.
	wantFamilies := []string{
		"neo4j_exporter_up",
		"neo4j_jvm_uptime_seconds",
		"neo4j_jvm_threads_total",
		"neo4j_jvm_classes_loaded",
		"neo4j_jvm_heap_used_bytes",
		"neo4j_jvm_heap_max_bytes",
		"neo4j_jvm_gc_collection_count_total",
		"neo4j_jvm_open_file_descriptors",
	}
	for _, name := range wantFamilies {
		assertMetricPresent(t, mfs, name, labels)
	}

	assertMetric(t, mfs, "neo4j_exporter_up", labels, 1)
}

