package collector

import (
	"context"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/prometheus/client_golang/prometheus"
)

// ── GDS (Graph Data Science) ───────────────────────────────────────

func (c *Collector) collectGDS(ctx context.Context, ch chan<- prometheus.Metric) {
	// gds.systemMonitor() — returns heap, CPU, and ongoing procedures.
	records, err := c.run.Query(ctx, readSessionCfg(), "CALL gds.systemMonitor() YIELD freeHeap, totalHeap, maxHeap, jvmAvailableCpuCores, availableCpuCoresNotRequested, ongoingGdsProcedures RETURN freeHeap, totalHeap, maxHeap, jvmAvailableCpuCores, availableCpuCoresNotRequested, size(ongoingGdsProcedures) AS ongoingCount", nil)
	if err != nil {
		slog.Debug("gds.systemMonitor() not available (GDS plugin may not be installed)", "err", err)
		return
	}
	rec, ok := single(records)
	if !ok {
		return
	}

	metrics := []struct {
		key  string
		desc *prometheus.Desc
	}{
		{"freeHeap", c.gdsFreeHeap},
		{"totalHeap", c.gdsTotalHeap},
		{"maxHeap", c.gdsMaxHeap},
		{"jvmAvailableCpuCores", c.gdsJvmAvailableCPUCores},
		{"availableCpuCoresNotRequested", c.gdsAvailableCPUCoresNotRequested},
		{"ongoingCount", c.gdsOngoingProcedures},
	}
	for _, m := range metrics {
		if val, ok := rec.Get(m.key); ok && val != nil {
			if fval, ok := jmxValue(val); ok {
				ch <- prometheus.MustNewConstMetric(m.desc, prometheus.GaugeValue, fval)
			}
		}
	}

	// gds.memory.summary() — graph and task memory. Summing per-user rows
	// handles both single-user and multi-user instances uniformly.
	summaryRecords, err := c.run.Query(ctx, readSessionCfg(),
		"CALL gds.memory.summary() YIELD user, totalGraphsMemory, totalTasksMemory RETURN totalGraphsMemory, totalTasksMemory", nil)
	if err != nil {
		slog.Debug("gds.memory.summary() not available", "err", err)
		return
	}
	c.emitMemoryMetrics(ch, summaryRecords)
}

// ── Heavy transactions ─────────────────────────────────────────────

func (c *Collector) collectHeavyTransactions(ctx context.Context, ch chan<- prometheus.Metric) {
	heavyTxQuery := "SHOW TRANSACTIONS " +
		"YIELD transactionId, elapsedTime, pageFaults " +
		"WHERE elapsedTime.milliseconds > 5000 " +
		"RETURN count(*) AS heavy_count, sum(pageFaults) AS total_faults"
	records, err := c.run.Query(ctx, systemSessionCfg(), heavyTxQuery, nil)
	if err != nil {
		slog.Warn("heavy transactions query failed", "err", err)
		return
	}
	rec, ok := single(records)
	if !ok {
		return
	}

	metrics := []struct {
		key  string
		desc *prometheus.Desc
	}{
		{"heavy_count", c.heavyQueriesActive},
		{"total_faults", c.heavyQueriesFaults},
	}
	for _, m := range metrics {
		if val, ok := rec.Get(m.key); ok && val != nil {
			if f, ok := jmxValue(val); ok {
				ch <- prometheus.MustNewConstMetric(m.desc, prometheus.GaugeValue, f)
			}
		}
	}
}

// ── Synthetic canary ───────────────────────────────────────────────

func (c *Collector) collectSynthetic(ctx context.Context, ch chan<- prometheus.Metric) {
	start := time.Now()
	_, err := c.run.Query(ctx, systemSessionCfg(), "CALL dbms.components() YIELD name RETURN name LIMIT 1", nil)
	if err != nil {
		slog.Warn("synthetic query failed", "err", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.syntheticQueryDur, prometheus.GaugeValue, time.Since(start).Seconds())
}

func (c *Collector) emitMemoryMetrics(ch chan<- prometheus.Metric, records []*neo4j.Record) {
	var totalGraphMem, totalTaskMem float64
	for _, sr := range records {
		if v, ok := sr.Get("totalGraphsMemory"); ok && v != nil {
			if f, ok := jmxValue(v); ok {
				totalGraphMem += f
			}
		}
		if v, ok := sr.Get("totalTasksMemory"); ok && v != nil {
			if f, ok := jmxValue(v); ok {
				totalTaskMem += f
			}
		}
	}
	for _, m := range []struct {
		desc *prometheus.Desc
		val  float64
	}{
		{c.gdsGraphMemoryBytes, totalGraphMem},
		{c.gdsTaskMemoryBytes, totalTaskMem},
	} {
		ch <- prometheus.MustNewConstMetric(m.desc, prometheus.GaugeValue, m.val)
	}
}
