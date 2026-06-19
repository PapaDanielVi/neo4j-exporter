package collector

import (
	"context"
	"log/slog"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/prometheus/client_golang/prometheus"
)

// collectAPOC emits metrics derived from APOC monitor procedures. It is a no-op
// when APOC was not detected on the target.
func (c *Collector) collectAPOC(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	if !c.apocAvailable {
		return
	}
	c.collectAPOCStore(ctx, ch, labels)
	c.collectAPOCIDs(ctx, ch, labels)
	c.collectAPOCTx(ctx, ch, labels)
}

func (c *Collector) collectAPOCStore(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	rec, ok := c.apocSingle(ctx, "CALL apoc.monitor.store()")
	if !ok {
		return
	}
	// Each APOC column maps to a store component reported via the "type" label.
	for col, typ := range map[string]string{
		"nodeStoreSize":   "node",
		"relStoreSize":    "relationship",
		"propStoreSize":   "property",
		"stringStoreSize": "string",
		"arrayStoreSize":  "array",
		"logSize":         "transaction_log",
		"totalStoreSize":  "total",
	} {
		if v, ok := jmxValue(recordValue(rec, col)); ok {
			ch <- prometheus.MustNewConstMetric(c.storeSize, prometheus.GaugeValue, v, append(append([]string{}, labels...), typ)...)
		}
	}
}

func (c *Collector) collectAPOCIDs(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	rec, ok := c.apocSingle(ctx, "CALL apoc.monitor.ids()")
	if !ok {
		return
	}
	for col, kind := range map[string]string{
		"nodeIds":    "node",
		"relIds":     "relationship",
		"propIds":    "property",
		"relTypeIds": "relationship_type",
	} {
		if v, ok := jmxValue(recordValue(rec, col)); ok {
			ch <- prometheus.MustNewConstMetric(c.idsInUse, prometheus.GaugeValue, v, append(append([]string{}, labels...), kind)...)
		}
	}
}

func (c *Collector) collectAPOCTx(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	rec, ok := c.apocSingle(ctx, "CALL apoc.monitor.tx()")
	if !ok {
		return
	}
	emit := func(col string, desc *prometheus.Desc, mtype prometheus.ValueType) {
		if v, ok := jmxValue(recordValue(rec, col)); ok {
			ch <- prometheus.MustNewConstMetric(desc, mtype, v, labels...)
		}
	}
	emit("totalTx", c.txCommitted, prometheus.CounterValue)
	emit("totalOpenedTx", c.txOpened, prometheus.CounterValue)
	emit("rolledBackTx", c.txRolledBack, prometheus.CounterValue)
	emit("currentOpenedTx", c.txOpen, prometheus.GaugeValue)
	emit("peakTx", c.txPeak, prometheus.GaugeValue)
	emit("lastTxId", c.lastCommittedTx, prometheus.GaugeValue)
}

// apocSingle runs an APOC monitor query against the default database and returns
// its single result row.
func (c *Collector) apocSingle(ctx context.Context, cypher string) (*neo4j.Record, bool) {
	records, err := c.run.Query(ctx, readSessionCfg(), cypher, nil)
	if err != nil {
		slog.Debug("APOC query failed", "query", cypher, "err", err)
		return nil, false
	}
	return single(records)
}
