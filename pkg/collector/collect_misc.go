package collector

import (
	"context"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/prometheus/client_golang/prometheus"
)

// ── GDS (Graph Data Science) ───────────────────────────────────────

func (c *Collector) collectGDS(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
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

	heapMetrics := map[string]*prometheus.Desc{
		"freeHeap":                      c.gdsFreeHeap,
		"totalHeap":                     c.gdsTotalHeap,
		"maxHeap":                       c.gdsMaxHeap,
		"jvmAvailableCpuCores":          c.gdsJvmAvailableCPUCores,
		"availableCpuCoresNotRequested": c.gdsAvailableCPUCoresNotRequested,
	}
	for key, desc := range heapMetrics {
		if val, ok := rec.Get(key); ok && val != nil {
			if fval, ok := jmxValue(val); ok {
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, fval, labels...)
			}
		}
	}
	if val, ok := rec.Get("ongoingCount"); ok && val != nil {
		if fval, ok := jmxValue(val); ok {
			ch <- prometheus.MustNewConstMetric(c.gdsOngoingProcedures, prometheus.GaugeValue, fval, labels...)
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
	c.emitMemoryMetrics(ch, summaryRecords, labels)
}

// ── Heavy transactions ─────────────────────────────────────────────

func (c *Collector) collectHeavyTransactions(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
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
	countVal, _ := rec.Get("heavy_count")
	faultsVal, _ := rec.Get("total_faults")
	var count, faults float64
	if v, ok := countVal.(int64); ok {
		count = float64(v)
	}
	if v, ok := faultsVal.(int64); ok {
		faults = float64(v)
	}
	ch <- prometheus.MustNewConstMetric(c.heavyQueriesActive, prometheus.GaugeValue, count, labels...)
	ch <- prometheus.MustNewConstMetric(c.heavyQueriesFaults, prometheus.GaugeValue, faults, labels...)
}

// ── Synthetic canary ───────────────────────────────────────────────

func (c *Collector) collectSynthetic(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	start := time.Now()
	_, err := c.run.Query(ctx, systemSessionCfg(), "CALL dbms.components() YIELD name RETURN name LIMIT 1", nil)
	if err != nil {
		slog.Warn("synthetic query failed", "err", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.syntheticQueryDur, prometheus.GaugeValue, time.Since(start).Seconds(), labels...)
}

func (c *Collector) emitMemoryMetrics(ch chan<- prometheus.Metric, records []*neo4j.Record, labels []string) {
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
	ch <- prometheus.MustNewConstMetric(c.gdsGraphMemoryBytes, prometheus.GaugeValue, totalGraphMem, labels...)
	ch <- prometheus.MustNewConstMetric(c.gdsTaskMemoryBytes, prometheus.GaugeValue, totalTaskMem, labels...)
}
