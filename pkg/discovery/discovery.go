// Package discovery provides functionality to discover Neo4j databases and generate Prometheus scrape targets for them.
package discovery

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Target represents a single Prometheus HTTP_SD target entry.
type Target struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// Discover queries the primary Neo4j instance for all databases and returns
// a list of scrape targets — one per database.
func Discover(ctx context.Context, driver neo4j.DriverWithContext, exporterAddr string) ([]Target, error) {
	result, err := neo4j.ExecuteQuery[*neo4j.EagerResult](ctx, driver,
		"SHOW DATABASES YIELD name, currentStatus",
		nil,
		neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase("system"),
	)
	if err != nil {
		return nil, fmt.Errorf("running SHOW DATABASES: %w", err)
	}

	var targets []Target
	for _, rec := range result.Records {
		nameVal, _ := rec.Get("name")
		statusVal, _ := rec.Get("currentStatus")
		dbName, ok := nameVal.(string)
		if !ok || dbName == "" {
			continue
		}
		status, _ := statusVal.(string)

		slog.Debug("discovered database", "name", dbName, "status", status)

		targets = append(targets, Target{
			Targets: []string{exporterAddr},
			Labels: map[string]string{
				"__metrics_path__": "/scrape",
				"__param_target":   "bolt://localhost:7687",
				"neo4j_database":   dbName,
			},
		})
	}

	return targets, nil
}
