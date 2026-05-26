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
	session := c.driver.NewSession(ctx, readSessionCfg())
	defer session.Close(ctx)

	// gds.systemMonitor() — returns heap, CPU, and ongoing procedures
	result, err := session.Run(ctx, "CALL gds.systemMonitor() YIELD freeHeap, totalHeap, maxHeap, jvmAvailableCpuCores, availableCpuCoresNotRequested, ongoingGdsProcedures RETURN freeHeap, totalHeap, maxHeap, jvmAvailableCpuCores, availableCpuCoresNotRequested, size(ongoingGdsProcedures) AS ongoingCount", nil)
	if err != nil {
		slog.Debug("gds.systemMonitor() not available (GDS plugin may not be installed)", "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
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

	// gds.memory.summary() — graph and task memory
	summaryResult, err := session.Run(ctx,
		"CALL gds.memory.summary() YIELD user, totalGraphsMemory, totalTasksMemory RETURN totalGraphsMemory, totalTasksMemory", nil)
	if err != nil {
		slog.Debug("gds.memory.summary() not available", "err", err)
		return
	}
	summaryRec, err := summaryResult.Single(ctx)
	if err != nil {
		c.emitMemoryMetrics(ch, ctx, summaryResult, labels)
		return
	}
	emitHeapMetric(ch, summaryRec, "totalGraphsMemory", c.gdsGraphMemoryBytes, labels)
	emitHeapMetric(ch, summaryRec, "totalTasksMemory", c.gdsTaskMemoryBytes, labels)
}

// ── Heavy transactions ─────────────────────────────────────────────

func (c *Collector) collectHeavyTransactions(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, systemSessionCfg())
	defer session.Close(ctx)

	heavyTxQuery := "SHOW TRANSACTIONS " +
		"YIELD transactionId, elapsedTime, pageFaults " +
		"WHERE elapsedTime.milliseconds > 5000 " +
		"RETURN count(*) AS heavy_count, sum(pageFaults) AS total_faults"
	result, err := session.Run(ctx, heavyTxQuery, nil)
	if err != nil {
		slog.Warn("heavy transactions query failed", "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
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
	session := c.driver.NewSession(ctx, systemSessionCfg())
	defer session.Close(ctx)
	_, err := session.Run(ctx, "CALL dbms.components() YIELD name RETURN name LIMIT 1", nil)
	if err != nil {
		slog.Warn("synthetic query failed", "err", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.syntheticQueryDur, prometheus.GaugeValue, time.Since(start).Seconds(), labels...)
}

func (c *Collector) emitMemoryMetrics(ch chan<- prometheus.Metric, ctx context.Context, result neo4j.ResultWithContext, labels []string) { //nolint:revive
	records, err := result.Collect(ctx)
	if err != nil {
		return
	}
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

func emitHeapMetric(ch chan<- prometheus.Metric, rec *neo4j.Record, key string, desc *prometheus.Desc, labels []string) {
	val, _ := rec.Get(key)
	if fval, ok := jmxValue(val); ok {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, fval, labels...)
	}
}
