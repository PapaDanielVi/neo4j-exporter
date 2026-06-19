package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// ── NIO Buffer Pools ───────────────────────────────────────────────

func (c *Collector) collectNIOBufferPools(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	records, err := c.run.Query(ctx, systemSessionCfg(),
		jmxQueryNameAttrs, map[string]any{jmxMBeanParam: nioBufferPoolMBean})
	if err != nil {
		slog.Warn("NIO buffer pool query failed", "err", err)
		return
	}
	for _, rec := range records {
		nameVal, _ := rec.Get("name")
		attrsVal, _ := rec.Get("attributes")
		poolName, ok := nameVal.(string)
		if !ok || poolName == "" {
			continue
		}
		attrsMap, ok := attrsVal.(map[string]any)
		if !ok {
			continue
		}
		poolLabels := make([]string, len(labels)+1)
		copy(poolLabels, labels)
		poolLabels[len(labels)] = poolName
		for attr, desc := range map[string]*prometheus.Desc{
			"MemoryUsed":    c.bufferPoolUsed,
			"TotalCapacity": c.bufferPoolCapacity,
			"Count":         c.bufferPoolCount,
		} {
			if v, ok := attrsMap[attr]; ok && v != nil {
				if fval, ok := jmxValue(v); ok {
					ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, fval, poolLabels...)
				}
			}
		}
	}
}

// ── Threading ──────────────────────────────────────────────────────

func (c *Collector) collectThreading(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	c.jmxQueryMulti(ctx, ch, labels, "java.lang:type=Threading", map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"PeakThreadCount":   {c.jvmThreadsPeak, prometheus.GaugeValue},
		"DaemonThreadCount": {c.jvmThreadsDaemon, prometheus.GaugeValue},
		"ThreadCount":       {c.jvmThreadsTotal, prometheus.GaugeValue},
	})
}

// ── Class Loading ──────────────────────────────────────────────────

func (c *Collector) collectClassLoading(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	c.jmxQueryMulti(ctx, ch, labels, "java.lang:type=ClassLoading", map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"LoadedClassCount":   {c.jvmClassesLoaded, prometheus.GaugeValue},
		"UnloadedClassCount": {c.jvmClassesUnloaded, prometheus.CounterValue},
	})
}

// ── Runtime (uptime) ───────────────────────────────────────────────

func (c *Collector) collectRuntime(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	records, err := c.run.Query(ctx, systemSessionCfg(),
		"CALL dbms.queryJmx($mbean) YIELD attributes RETURN attributes['Uptime'] AS uptime",
		map[string]any{jmxMBeanParam: "java.lang:type=Runtime"})
	if err != nil {
		return
	}
	rec, ok := single(records)
	if !ok {
		return
	}
	val, _ := rec.Get("uptime")
	if fval, ok := jmxValue(val); ok {
		ch <- prometheus.MustNewConstMetric(c.jvmUptime, prometheus.GaugeValue, fval/1000.0, labels...)
	}
}
