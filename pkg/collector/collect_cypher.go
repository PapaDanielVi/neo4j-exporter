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

func (c *Collector) collectDatabases(ctx context.Context, ch chan<- prometheus.Metric) {
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
		ch <- prometheus.MustNewConstMetric(c.dbOnline, prometheus.GaugeValue, online, name, role)
	}
}

// ── Active transactions per database (SHOW TRANSACTIONS) ─────────────

func (c *Collector) collectTransactionsByDatabase(ctx context.Context, ch chan<- prometheus.Metric) {
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
		ch <- prometheus.MustNewConstMetric(c.dbTxActive, prometheus.GaugeValue, active, db)
	}
}

// ── Memory pools (dbms.listPools) ───────────────────────────────────

func (c *Collector) collectPools(ctx context.Context, ch chan<- prometheus.Metric) {
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
		// Only the *Bytes columns are numeric; human-readable columns are skipped by jmxValue.
		if v, ok := jmxValue(recordValue(rec, "heapMemoryUsedBytes")); ok {
			ch <- prometheus.MustNewConstMetric(c.poolUsedHeap, prometheus.GaugeValue, v, pool, db)
		}
		if v, ok := jmxValue(recordValue(rec, "nativeMemoryUsedBytes")); ok {
			ch <- prometheus.MustNewConstMetric(c.poolUsedNative, prometheus.GaugeValue, v, pool, db)
		}
	}
}

// ── Index health (SHOW INDEXES) ─────────────────────────────────────

func (c *Collector) collectIndexes(ctx context.Context, ch chan<- prometheus.Metric) {
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
	ch <- prometheus.MustNewConstMetric(c.indexesTotal, prometheus.GaugeValue, total)
	ch <- prometheus.MustNewConstMetric(c.indexesOnline, prometheus.GaugeValue, online)
	ch <- prometheus.MustNewConstMetric(c.indexesFailed, prometheus.GaugeValue, failed)
}

// ── Constraint count (SHOW CONSTRAINTS) ─────────────────────────────

func (c *Collector) collectConstraints(ctx context.Context, ch chan<- prometheus.Metric) {
	records, err := c.run.Query(ctx, readSessionCfg(), "SHOW CONSTRAINTS YIELD name RETURN name", nil)
	if err != nil {
		slog.Debug("SHOW CONSTRAINTS failed", "err", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.constraintsTotal, prometheus.GaugeValue, float64(len(records)))
}
