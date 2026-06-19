package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	indexStateOnline = "ONLINE"
	indexStateFailed = "FAILED"
	statusOnline     = "online"
)

// ── Database topology (SHOW DATABASES) ──────────────────────────────

func (c *Collector) collectDatabases(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	records, err := c.run.Query(ctx, systemSessionCfg(),
		"SHOW DATABASES YIELD name, currentStatus, role RETURN name, currentStatus, role", nil)
	if err != nil {
		slog.Debug("SHOW DATABASES failed", "err", err)
		return
	}
	for _, rec := range records {
		name := recordString(rec, "name")
		if name == "" {
			continue
		}
		status := recordString(rec, "currentStatus")
		role := recordString(rec, "role")
		online := 0.0
		if status == statusOnline {
			online = 1
		}
		dbLabels := append(append([]string{}, labels...), name, role)
		ch <- prometheus.MustNewConstMetric(c.dbOnline, prometheus.GaugeValue, online, dbLabels...)
	}
}

// ── Active transactions per database (SHOW TRANSACTIONS) ─────────────

func (c *Collector) collectTransactionsByDatabase(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	records, err := c.run.Query(ctx, systemSessionCfg(),
		"SHOW TRANSACTIONS YIELD database RETURN database, count(*) AS active", nil)
	if err != nil {
		slog.Debug("SHOW TRANSACTIONS (per database) failed", "err", err)
		return
	}
	for _, rec := range records {
		db := recordString(rec, "database")
		if db == "" {
			continue
		}
		active, ok := jmxValue(recordValue(rec, "active"))
		if !ok {
			continue
		}
		dbLabels := append(append([]string{}, labels...), db)
		ch <- prometheus.MustNewConstMetric(c.dbTxActive, prometheus.GaugeValue, active, dbLabels...)
	}
}

// ── Memory pools (dbms.listPools) ───────────────────────────────────

func (c *Collector) collectPools(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	records, err := c.run.Query(ctx, readSessionCfg(), "CALL dbms.listPools()", nil)
	if err != nil {
		slog.Debug("dbms.listPools() failed", "err", err)
		return
	}
	for _, rec := range records {
		pool := recordString(rec, "pool")
		if pool == "" {
			continue
		}
		db := recordString(rec, "databaseName")
		poolLabels := append(append([]string{}, labels...), pool, db)
		// Only the *Bytes columns are numeric; human-readable columns are skipped by jmxValue.
		if v, ok := jmxValue(recordValue(rec, "heapMemoryUsedBytes")); ok {
			ch <- prometheus.MustNewConstMetric(c.poolUsedHeap, prometheus.GaugeValue, v, poolLabels...)
		}
		if v, ok := jmxValue(recordValue(rec, "nativeMemoryUsedBytes")); ok {
			ch <- prometheus.MustNewConstMetric(c.poolUsedNative, prometheus.GaugeValue, v, poolLabels...)
		}
	}
}

// ── Index health (SHOW INDEXES) ─────────────────────────────────────

func (c *Collector) collectIndexes(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	records, err := c.run.Query(ctx, readSessionCfg(), "SHOW INDEXES YIELD state RETURN state", nil)
	if err != nil {
		slog.Debug("SHOW INDEXES failed", "err", err)
		return
	}
	var total, online, failed float64
	for _, rec := range records {
		total++
		switch state := recordString(rec, "state"); state {
		case indexStateOnline:
			online++
		case indexStateFailed:
			failed++
		}
	}
	ch <- prometheus.MustNewConstMetric(c.indexesTotal, prometheus.GaugeValue, total, labels...)
	ch <- prometheus.MustNewConstMetric(c.indexesOnline, prometheus.GaugeValue, online, labels...)
	ch <- prometheus.MustNewConstMetric(c.indexesFailed, prometheus.GaugeValue, failed, labels...)
}

// ── Constraint count (SHOW CONSTRAINTS) ─────────────────────────────

func (c *Collector) collectConstraints(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	records, err := c.run.Query(ctx, readSessionCfg(), "SHOW CONSTRAINTS YIELD name RETURN name", nil)
	if err != nil {
		slog.Debug("SHOW CONSTRAINTS failed", "err", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.constraintsTotal, prometheus.GaugeValue, float64(len(records)), labels...)
}
