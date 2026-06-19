//go:build integration

// Package collector_test integration suite. Boots a real Neo4j Community
// container and asserts the collector produces a known set of metric families.
// Run with: go test -tags integration ./pkg/collector/
package collector_test

import (
	"context"
	"testing"
	"time"

	"github.com/PapaDanielVi/neo4j-exporter/pkg/collector"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
)

const (
	integrationImage    = "neo4j:5-community"
	integrationPassword = "testpassword123"
)

func TestIntegrationCommunityMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	ctr, err := tcneo4j.Run(ctx, integrationImage,
		tcneo4j.WithAdminPassword(integrationPassword))
	if err != nil {
		t.Fatalf("starting neo4j container: %v", err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Logf("terminating container: %v", err)
		}
	})

	boltURL, err := ctr.BoltUrl(ctx)
	if err != nil {
		t.Fatalf("getting bolt url: %v", err)
	}

	driver, err := neo4j.NewDriverWithContext(boltURL,
		neo4j.BasicAuth("neo4j", integrationPassword, ""))
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
		if _, ok := metricValue(mfs, name, labels); !ok {
			t.Errorf("expected metric family %q not emitted", name)
		}
	}

	if up, ok := metricValue(mfs, "neo4j_exporter_up", labels); !ok || up != 1 {
		t.Errorf("neo4j_exporter_up = %v ok=%v, want 1", up, ok)
	}
}
