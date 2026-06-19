package collector_test

import (
	"testing"

	"github.com/PapaDanielVi/neo4j-exporter/pkg/collector"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func TestCustomCollectorScalar(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{match: "suspended", records: []*neo4j.Record{rec([]string{"count"}, int64(7))}},
		},
	}
	cq := &collector.CustomQueries{Queries: []collector.CustomQuery{
		{
			Query:      "MATCH (u:User {status:'suspended'}) RETURN count(u) AS count",
			MetricName: "neo4j_custom_suspended_users_total",
			Type:       "gauge",
			Help:       "Suspended users",
			Labels:     map[string]string{"group": "topology"},
		},
	}}
	cc := collector.NewCustomCollectorWithRunner(cq, r)
	mfs := gather(t, cc)

	if v, ok := metricValue(mfs, "neo4j_custom_suspended_users_total", map[string]string{"group": "topology"}); !ok || v != 7 {
		t.Errorf("custom metric = %v ok=%v, want 7", v, ok)
	}
}

func TestCustomCollectorExplicitValueColumn(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{match: "RETURN", records: []*neo4j.Record{rec([]string{"label", "total"}, "User", int64(42))}},
		},
	}
	cq := &collector.CustomQueries{Queries: []collector.CustomQuery{
		{
			Query:      "MATCH (n) RETURN 'User' AS label, count(n) AS total",
			MetricName: "neo4j_custom_node_total",
			Type:       "gauge",
			Help:       "Node total",
			Value:      "total",
		},
	}}
	cc := collector.NewCustomCollectorWithRunner(cq, r)
	mfs := gather(t, cc)

	if v, ok := metricValue(mfs, "neo4j_custom_node_total", nil); !ok || v != 42 {
		t.Errorf("custom metric = %v ok=%v, want 42", v, ok)
	}
}
