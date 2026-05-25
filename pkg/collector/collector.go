package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/prometheus/client_golang/prometheus"
)

const scrapeTimeout = 10 * time.Second

// Collector implements prometheus.Collector for Neo4j metrics.
type Collector struct {
	driver            neo4j.DriverWithContext
	target            string
	neo4jMajorVersion int
	neo4jMinorVersion int

	// Exporter self-metrics
	up             *prometheus.Desc
	scrapeDuration *prometheus.Desc

	// ── NIO Buffer Pools ─────────────────────────────────────────────
	bufferPoolUsed     *prometheus.Desc
	bufferPoolCapacity *prometheus.Desc
	bufferPoolCount    *prometheus.Desc

	// ── JVM ──────────────────────────────────────────────────────────
	jvmThreadsPeak     *prometheus.Desc
	jvmThreadsDaemon   *prometheus.Desc
	jvmThreadsTotal    *prometheus.Desc
	jvmClassesLoaded   *prometheus.Desc
	jvmClassesUnloaded *prometheus.Desc
	jvmUptime          *prometheus.Desc

	// ── GDS ──────────────────────────────────────────────────────────
	gdsFreeHeap                      *prometheus.Desc
	gdsTotalHeap                     *prometheus.Desc
	gdsMaxHeap                       *prometheus.Desc
	gdsJvmAvailableCPUCores          *prometheus.Desc
	gdsAvailableCPUCoresNotRequested *prometheus.Desc
	gdsOngoingProcedures             *prometheus.Desc
	gdsGraphMemoryBytes              *prometheus.Desc
	gdsTaskMemoryBytes               *prometheus.Desc

	// ── Advanced / internal ──────────────────────────────────────────
	heavyQueriesActive *prometheus.Desc
	heavyQueriesFaults *prometheus.Desc
	syntheticQueryDur  *prometheus.Desc
}

// New creates a Neo4jCollector with all metric descriptors.
func New(target string, driver neo4j.DriverWithContext) *Collector {
	const ns = "neo4j"
	labels := []string{"target"}

	c := &Collector{
		driver: driver,
		target: target,

		// ── Exporter self-metrics ─────────────────────────────────
		up: prometheus.NewDesc(ns+"_exporter_up",
			"1 if target Neo4j instance is reachable, else 0", labels, nil),
		scrapeDuration: prometheus.NewDesc(ns+"_exporter_scrape_duration_seconds",
			"Latency of scrape phases", append(labels, "phase"), nil),

		// ── NIO Buffer Pools ──────────────────────────────────────
		bufferPoolUsed:     prometheus.NewDesc(ns+"_jvm_buffer_pool_used_bytes", "Off-heap buffer pool used bytes", append(labels, "pool"), nil),
		bufferPoolCapacity: prometheus.NewDesc(ns+"_jvm_buffer_pool_capacity_bytes", "Off-heap buffer pool capacity bytes", append(labels, "pool"), nil),
		bufferPoolCount:    prometheus.NewDesc(ns+"_jvm_buffer_pool_count", "Number of buffers in pool", append(labels, "pool"), nil),

		// ── JVM ───────────────────────────────────────────────────
		jvmThreadsPeak:     prometheus.NewDesc(ns+"_jvm_threads_peak", "Peak thread count", labels, nil),
		jvmThreadsDaemon:   prometheus.NewDesc(ns+"_jvm_threads_daemon", "Daemon thread count", labels, nil),
		jvmThreadsTotal:    prometheus.NewDesc(ns+"_jvm_threads_total", "Total live threads", labels, nil),
		jvmClassesLoaded:   prometheus.NewDesc(ns+"_jvm_classes_loaded", "Currently loaded classes", labels, nil),
		jvmClassesUnloaded: prometheus.NewDesc(ns+"_jvm_classes_unloaded_total", "Total unloaded classes", labels, nil),
		jvmUptime:          prometheus.NewDesc(ns+"_jvm_uptime_seconds", "JVM uptime in seconds", labels, nil),

		// ── GDS ───────────────────────────────────────────────────
		gdsFreeHeap:                      prometheus.NewDesc(ns+"_gds_jvm_free_heap_bytes", "Free JVM heap bytes from GDS system monitor", labels, nil),
		gdsTotalHeap:                     prometheus.NewDesc(ns+"_gds_jvm_total_heap_bytes", "Total JVM heap bytes from GDS system monitor", labels, nil),
		gdsMaxHeap:                       prometheus.NewDesc(ns+"_gds_jvm_max_heap_bytes", "Max JVM heap bytes from GDS system monitor", labels, nil),
		gdsJvmAvailableCPUCores:          prometheus.NewDesc(ns+"_gds_jvm_available_cpu_cores", "Logical CPU cores available to JVM", labels, nil),
		gdsAvailableCPUCoresNotRequested: prometheus.NewDesc(ns+"_gds_available_cpu_cores_not_requested", "CPU cores not requested by GDS procedures", labels, nil),
		gdsOngoingProcedures:             prometheus.NewDesc(ns+"_gds_ongoing_procedures", "Number of currently running GDS procedures", labels, nil),
		gdsGraphMemoryBytes:              prometheus.NewDesc(ns+"_gds_graph_memory_bytes", "Memory used by GDS projected graphs", labels, nil),
		gdsTaskMemoryBytes:               prometheus.NewDesc(ns+"_gds_task_memory_bytes", "Memory estimated for running GDS tasks", labels, nil),

		// ── Advanced / internal ───────────────────────────────────
		heavyQueriesActive: prometheus.NewDesc(ns+"_dbms_heavy_queries_active", "Active queries with elapsed time > 5s", labels, nil),
		heavyQueriesFaults: prometheus.NewDesc(ns+"_dbms_heavy_queries_page_faults", "Page faults for heavy queries", labels, nil),
		syntheticQueryDur: prometheus.NewDesc(ns+"_synthetic_query_duration_seconds",
			"Latency of synthetic canary query", labels, prometheus.Labels{"query": "RETURN 1"}),
	}

	return c
}

// DetectVersion queries dbms.components() to determine the Neo4j version.
// Safe to call multiple times; subsequent calls are no-ops once detected.
func (c *Collector) DetectVersion(ctx context.Context) {
	if c.neo4jMajorVersion != 0 {
		return
	}

	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.components() YIELD versions RETURN versions", nil)
	if err != nil {
		slog.Warn("version detection failed", "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		slog.Warn("version detection: no result", "err", err)
		return
	}

	versionsVal, _ := rec.Get("versions")
	versionsList, ok := versionsVal.([]any)
	if !ok || len(versionsList) == 0 {
		slog.Warn("version detection: unexpected versions type", "type", fmt.Sprintf("%T", versionsVal))
		return
	}
	versionStr, _ := versionsList[0].(string)

	var major, minor int
	_, _ = fmt.Sscanf(versionStr, "%d.%d", &major, &minor)

	c.neo4jMajorVersion = major
	c.neo4jMinorVersion = minor

	slog.Info("detected Neo4j version",
		"version", versionStr,
		"major", major, "minor", minor)
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.allDescs() {
		ch <- d
	}
}

func (c *Collector) allDescs() []*prometheus.Desc {
	return []*prometheus.Desc{
		c.up, c.scrapeDuration,
		c.bufferPoolUsed, c.bufferPoolCapacity, c.bufferPoolCount,
		c.jvmThreadsPeak, c.jvmThreadsDaemon, c.jvmThreadsTotal,
		c.jvmClassesLoaded, c.jvmClassesUnloaded, c.jvmUptime,
		c.gdsFreeHeap, c.gdsTotalHeap, c.gdsMaxHeap,
		c.gdsJvmAvailableCPUCores, c.gdsAvailableCPUCoresNotRequested,
		c.gdsOngoingProcedures, c.gdsGraphMemoryBytes, c.gdsTaskMemoryBytes,
		c.heavyQueriesActive, c.heavyQueriesFaults, c.syntheticQueryDur,
	}
}

// Collect implements prometheus.Collector. All queries run concurrently.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	targetLabel := []string{c.target}
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()

	start := time.Now()
	err := c.driver.VerifyConnectivity(ctx)
	if err != nil {
		slog.Warn("target unreachable", "target", c.target, "err", err)
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0, targetLabel...)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1, targetLabel...)

	c.DetectVersion(ctx)

	var wg sync.WaitGroup

	type namedCollector struct {
		name string
		fn   func(context.Context, chan<- prometheus.Metric, []string)
	}

	collectors := []namedCollector{
		{"nio_buffers", c.collectNIOBufferPools},
		{"threading", c.collectThreading},
		{"classloading", c.collectClassLoading},
		{"runtime", c.collectRuntime},
		{"gds", c.collectGDS},
		{"heavy_tx", c.collectHeavyTransactions},
		{"synthetic", c.collectSynthetic},
	}

	for _, nc := range collectors {
		wg.Add(1)
		go func(nc namedCollector) {
			defer wg.Done()
			phaseStart := time.Now()
			nc.fn(ctx, ch, targetLabel)
			ch <- prometheus.MustNewConstMetric(
				c.scrapeDuration, prometheus.GaugeValue,
				time.Since(phaseStart).Seconds(), append(targetLabel, nc.name)...,
			)
		}(nc)
	}

	wg.Wait()

	ch <- prometheus.MustNewConstMetric(
		c.scrapeDuration, prometheus.GaugeValue,
		time.Since(start).Seconds(), append(targetLabel, "total")...,
	)
}

// jmxValue safely extracts a float64 from a JMX attribute value.
func jmxValue(raw any) (float64, bool) {
	switch v := raw.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case map[string]any:
		if inner, ok := v["value"]; ok {
			switch iv := inner.(type) {
			case int64:
				return float64(iv), true
			case float64:
				return iv, true
			}
		}
		return 0, false
	default:
		return 0, false
	}
}

// jmxQueryMulti runs a JMX query returning all attributes as a map, then emits multiple metrics.
func (c *Collector) jmxQueryMulti(ctx context.Context, ch chan<- prometheus.Metric, labels []string,
	mbean string, attrs map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	},
) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)

	result, err := session.Run(ctx,
		"CALL dbms.queryJmx($mbean) YIELD attributes RETURN attributes", map[string]any{"mbean": mbean})
	if err != nil {
		slog.Warn("JMX multi query failed", "mbean", mbean, "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		return
	}
	rawAttrs, _ := rec.Get("attributes")
	attrsMap, ok := rawAttrs.(map[string]any)
	if !ok {
		slog.Warn("JMX multi unexpected type", "mbean", mbean, "type", fmt.Sprintf("%T", rawAttrs))
		return
	}
	for attrName, cfg := range attrs {
		val, ok := attrsMap[attrName]
		if val == nil || !ok {
			continue
		}
		fval, ok := jmxValue(val)
		if !ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(cfg.desc, cfg.mtype, fval, labels...)
	}
}
