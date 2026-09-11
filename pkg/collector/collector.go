// Package collector implements a Prometheus collector for Neo4j metrics, including JVM and GDS stats via JMX.
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	scrapeTimeout      = 10 * time.Second
	systemDatabase     = "system"
	jmxQueryAllAttrs   = "CALL dbms.queryJmx($mbean) YIELD attributes RETURN attributes"
	jmxQueryNameAttrs  = "CALL dbms.queryJmx($mbean) YIELD name, attributes RETURN name, attributes"
	nioBufferPoolMBean = "java.nio:type=BufferPool,name=*"
	jmxMBeanParam      = "mbean"
	labelPool          = "pool"
	labelDatabase      = "database"
)

func readSessionCfg() neo4j.SessionConfig {
	return neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead}
}

func systemSessionCfg() neo4j.SessionConfig {
	return neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: systemDatabase}
}

// Runner is the minimal Neo4j surface the collector depends on. It is satisfied
// by driverRunner in production and by fakes in tests.
type Runner interface {
	VerifyConnectivity(ctx context.Context) error
	// Query runs cypher in a fresh session, collects all records, and closes the session.
	Query(ctx context.Context, cfg neo4j.SessionConfig, cypher string, params map[string]any) ([]*neo4j.Record, error)
}

// driverRunner adapts a real Neo4j driver to the Runner interface.
type driverRunner struct {
	driver neo4j.DriverWithContext
}

// VerifyConnectivity reports whether the underlying driver can reach the target.
func (d driverRunner) VerifyConnectivity(ctx context.Context) error {
	return d.driver.VerifyConnectivity(ctx)
}

// Query opens a session, runs the query to completion, and closes the session.
func (d driverRunner) Query(ctx context.Context, cfg neo4j.SessionConfig, cypher string, params map[string]any) ([]*neo4j.Record, error) {
	session := d.driver.NewSession(ctx, cfg)
	defer session.Close(ctx)
	result, err := session.Run(ctx, cypher, params)
	if err != nil {
		return nil, fmt.Errorf("running query: %w", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("collecting records: %w", err)
	}
	return records, nil
}

// sanitizeTarget removes any userinfo (credentials) from a target URI so it is
// safe to expose as a metric label.
func sanitizeTarget(target string) string {
	u, err := url.Parse(target)
	if err != nil || u.User == nil {
		return target
	}
	u.User = nil
	return u.String()
}

// single returns the sole record when the result has exactly one row.
func single(records []*neo4j.Record) (*neo4j.Record, bool) {
	if len(records) != 1 {
		return nil, false
	}
	return records[0], true
}

// recordValue returns the raw value for key, or nil if absent.
func recordValue(rec *neo4j.Record, key string) any {
	v, _ := rec.Get(key)
	return v
}

// recordString returns the string value for key, or "" if absent or not a string.
func recordString(rec *neo4j.Record, key string) string {
	s, _ := recordValue(rec, key).(string)
	return s
}

// Collector implements prometheus.Collector for Neo4j metrics.
type Collector struct {
	run           Runner
	target        string
	apocAvailable bool
	detected      bool

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

	// ── JVM memory ───────────────────────────────────────────────────
	jvmHeapUsed         *prometheus.Desc
	jvmHeapCommitted    *prometheus.Desc
	jvmHeapMax          *prometheus.Desc
	jvmHeapInit         *prometheus.Desc
	jvmNonHeapUsed      *prometheus.Desc
	jvmNonHeapCommitted *prometheus.Desc
	jvmNonHeapMax       *prometheus.Desc
	jvmMemoryPoolUsed   *prometheus.Desc
	jvmMemoryPoolCommit *prometheus.Desc
	jvmMemoryPoolMax    *prometheus.Desc

	// ── JVM garbage collection ───────────────────────────────────────
	jvmGCCount *prometheus.Desc
	jvmGCTime  *prometheus.Desc

	// ── Operating system ─────────────────────────────────────────────
	osProcessCPULoad   *prometheus.Desc
	osSystemCPULoad    *prometheus.Desc
	osOpenFDs          *prometheus.Desc
	osMaxFDs           *prometheus.Desc
	osFreePhysicalMem  *prometheus.Desc
	osCommittedVirtMem *prometheus.Desc
	osSystemLoadAvg    *prometheus.Desc
	osAvailableProcs   *prometheus.Desc

	// ── GDS ──────────────────────────────────────────────────────────
	gdsFreeHeap                      *prometheus.Desc
	gdsTotalHeap                     *prometheus.Desc
	gdsMaxHeap                       *prometheus.Desc
	gdsJvmAvailableCPUCores          *prometheus.Desc
	gdsAvailableCPUCoresNotRequested *prometheus.Desc
	gdsOngoingProcedures             *prometheus.Desc
	gdsGraphMemoryBytes              *prometheus.Desc
	gdsTaskMemoryBytes               *prometheus.Desc

	// ── Database topology / Cypher-derived ───────────────────────────
	dbOnline         *prometheus.Desc
	dbTxActive       *prometheus.Desc
	poolUsedHeap     *prometheus.Desc
	poolUsedNative   *prometheus.Desc
	indexesTotal     *prometheus.Desc
	indexesOnline    *prometheus.Desc
	indexesFailed    *prometheus.Desc
	constraintsTotal *prometheus.Desc

	// ── APOC-derived (optional) ──────────────────────────────────────
	storeSize       *prometheus.Desc
	idsInUse        *prometheus.Desc
	txCommitted     *prometheus.Desc
	txOpened        *prometheus.Desc
	txRolledBack    *prometheus.Desc
	txOpen          *prometheus.Desc
	txPeak          *prometheus.Desc
	lastCommittedTx *prometheus.Desc

	// ── Advanced / internal ──────────────────────────────────────────
	heavyQueriesActive *prometheus.Desc
	heavyQueriesFaults *prometheus.Desc
	syntheticQueryDur  *prometheus.Desc
}

// New creates a Collector backed by a real Neo4j driver.
func New(target string, driver neo4j.DriverWithContext) *Collector {
	return NewWithRunner(target, driverRunner{driver: driver})
}

// NewWithRunner creates a Collector backed by an arbitrary Runner. It exists so
// the collector can be driven by fakes in tests or alternative data sources.
func NewWithRunner(target string, r Runner) *Collector {
	const ns = "neo4j"
	sanitizedTarget := sanitizeTarget(target)
	constLabels := prometheus.Labels{"target": sanitizedTarget}

	c := &Collector{
		run:    r,
		target: sanitizedTarget,

		// ── Exporter self-metrics ─────────────────────────────────
		up: prometheus.NewDesc(ns+"_exporter_up",
			"1 if target Neo4j instance is reachable, else 0", nil, constLabels),
		scrapeDuration: prometheus.NewDesc(ns+"_exporter_scrape_duration_seconds",
			"Latency of scrape phases", []string{"phase"}, constLabels),

		// ── NIO Buffer Pools ──────────────────────────────────────
		bufferPoolUsed:     prometheus.NewDesc(ns+"_jvm_buffer_pool_used_bytes", "Off-heap buffer pool used bytes", []string{labelPool}, constLabels),
		bufferPoolCapacity: prometheus.NewDesc(ns+"_jvm_buffer_pool_capacity_bytes", "Off-heap buffer pool capacity bytes", []string{labelPool}, constLabels),
		bufferPoolCount:    prometheus.NewDesc(ns+"_jvm_buffer_pool_count", "Number of buffers in pool", []string{labelPool}, constLabels),

		// ── JVM ───────────────────────────────────────────────────
		jvmThreadsPeak:     prometheus.NewDesc(ns+"_jvm_threads_peak", "Peak thread count", nil, constLabels),
		jvmThreadsDaemon:   prometheus.NewDesc(ns+"_jvm_threads_daemon", "Daemon thread count", nil, constLabels),
		jvmThreadsTotal:    prometheus.NewDesc(ns+"_jvm_threads_total", "Total live threads", nil, constLabels),
		jvmClassesLoaded:   prometheus.NewDesc(ns+"_jvm_classes_loaded", "Currently loaded classes", nil, constLabels),
		jvmClassesUnloaded: prometheus.NewDesc(ns+"_jvm_classes_unloaded_total", "Total unloaded classes", nil, constLabels),
		jvmUptime:          prometheus.NewDesc(ns+"_jvm_uptime_seconds", "JVM uptime in seconds", nil, constLabels),

		// ── JVM memory ────────────────────────────────────────────
		jvmHeapUsed:         prometheus.NewDesc(ns+"_jvm_heap_used_bytes", "Used heap memory in bytes", nil, constLabels),
		jvmHeapCommitted:    prometheus.NewDesc(ns+"_jvm_heap_committed_bytes", "Committed heap memory in bytes", nil, constLabels),
		jvmHeapMax:          prometheus.NewDesc(ns+"_jvm_heap_max_bytes", "Maximum heap memory in bytes", nil, constLabels),
		jvmHeapInit:         prometheus.NewDesc(ns+"_jvm_heap_init_bytes", "Initial heap memory in bytes", nil, constLabels),
		jvmNonHeapUsed:      prometheus.NewDesc(ns+"_jvm_nonheap_used_bytes", "Used non-heap memory in bytes", nil, constLabels),
		jvmNonHeapCommitted: prometheus.NewDesc(ns+"_jvm_nonheap_committed_bytes", "Committed non-heap memory in bytes", nil, constLabels),
		jvmNonHeapMax:       prometheus.NewDesc(ns+"_jvm_nonheap_max_bytes", "Maximum non-heap memory in bytes", nil, constLabels),
		jvmMemoryPoolUsed:   prometheus.NewDesc(ns+"_jvm_memory_pool_used_bytes", "Used memory in pool", []string{labelPool}, constLabels),
		jvmMemoryPoolCommit: prometheus.NewDesc(ns+"_jvm_memory_pool_committed_bytes", "Committed memory in pool", []string{labelPool}, constLabels),
		jvmMemoryPoolMax:    prometheus.NewDesc(ns+"_jvm_memory_pool_max_bytes", "Maximum memory in pool", []string{labelPool}, constLabels),

		// ── JVM garbage collection ────────────────────────────────
		jvmGCCount: prometheus.NewDesc(ns+"_jvm_gc_collection_count_total", "Total GC collections", []string{"gc"}, constLabels),
		jvmGCTime:  prometheus.NewDesc(ns+"_jvm_gc_collection_seconds_total", "Total time spent in GC", []string{"gc"}, constLabels),

		// ── Operating system ──────────────────────────────────────
		osProcessCPULoad:   prometheus.NewDesc(ns+"_jvm_process_cpu_load", "Recent CPU load of the Neo4j process (0..1)", nil, constLabels),
		osSystemCPULoad:    prometheus.NewDesc(ns+"_jvm_system_cpu_load", "Recent CPU load of the host system (0..1)", nil, constLabels),
		osOpenFDs:          prometheus.NewDesc(ns+"_jvm_open_file_descriptors", "Number of open file descriptors", nil, constLabels),
		osMaxFDs:           prometheus.NewDesc(ns+"_jvm_max_file_descriptors", "Maximum allowed file descriptors", nil, constLabels),
		osFreePhysicalMem:  prometheus.NewDesc(ns+"_jvm_free_physical_memory_bytes", "Free physical memory on the host in bytes", nil, constLabels),
		osCommittedVirtMem: prometheus.NewDesc(ns+"_jvm_committed_virtual_memory_bytes", "Committed virtual memory in bytes", nil, constLabels),
		osSystemLoadAvg:    prometheus.NewDesc(ns+"_jvm_system_load_average", "System load average over the last minute", nil, constLabels),
		osAvailableProcs:   prometheus.NewDesc(ns+"_jvm_available_processors", "Processors available to the JVM", nil, constLabels),

		// ── GDS ───────────────────────────────────────────────────
		gdsFreeHeap:                      prometheus.NewDesc(ns+"_gds_jvm_free_heap_bytes", "Free JVM heap bytes from GDS system monitor", nil, constLabels),
		gdsTotalHeap:                     prometheus.NewDesc(ns+"_gds_jvm_total_heap_bytes", "Total JVM heap bytes from GDS system monitor", nil, constLabels),
		gdsMaxHeap:                       prometheus.NewDesc(ns+"_gds_jvm_max_heap_bytes", "Max JVM heap bytes from GDS system monitor", nil, constLabels),
		gdsJvmAvailableCPUCores:          prometheus.NewDesc(ns+"_gds_jvm_available_cpu_cores", "Logical CPU cores available to JVM", nil, constLabels),
		gdsAvailableCPUCoresNotRequested: prometheus.NewDesc(ns+"_gds_available_cpu_cores_not_requested", "CPU cores not requested by GDS procedures", nil, constLabels),
		gdsOngoingProcedures:             prometheus.NewDesc(ns+"_gds_ongoing_procedures", "Number of currently running GDS procedures", nil, constLabels),
		gdsGraphMemoryBytes:              prometheus.NewDesc(ns+"_gds_graph_memory_bytes", "Memory used by GDS projected graphs", nil, constLabels),
		gdsTaskMemoryBytes:               prometheus.NewDesc(ns+"_gds_task_memory_bytes", "Memory estimated for running GDS tasks", nil, constLabels),

		// ── Database topology / Cypher-derived ─────────────────────
		dbOnline:         prometheus.NewDesc(ns+"_database_online", "1 if the database currentStatus is online, else 0", []string{labelDatabase, "role"}, constLabels),
		dbTxActive:       prometheus.NewDesc(ns+"_database_transactions_active", "Currently active transactions per database", []string{labelDatabase}, constLabels),
		poolUsedHeap:     prometheus.NewDesc(ns+"_dbms_pool_used_heap_bytes", "Heap memory used by a memory pool", []string{labelPool, labelDatabase}, constLabels),
		poolUsedNative:   prometheus.NewDesc(ns+"_dbms_pool_used_native_bytes", "Native memory used by a memory pool", []string{labelPool, labelDatabase}, constLabels),
		indexesTotal:     prometheus.NewDesc(ns+"_indexes_total", "Total indexes in the default database", nil, constLabels),
		indexesOnline:    prometheus.NewDesc(ns+"_indexes_online", "Online indexes in the default database", nil, constLabels),
		indexesFailed:    prometheus.NewDesc(ns+"_indexes_failed", "Failed indexes in the default database", nil, constLabels),
		constraintsTotal: prometheus.NewDesc(ns+"_constraints_total", "Total constraints in the default database", nil, constLabels),

		// ── APOC-derived (optional) ────────────────────────────────
		storeSize:       prometheus.NewDesc(ns+"_store_size_bytes", "On-disk store size by component (requires APOC)", []string{"type"}, constLabels),
		idsInUse:        prometheus.NewDesc(ns+"_ids_in_use", "Entity IDs in use by kind (requires APOC)", []string{"kind"}, constLabels),
		txCommitted:     prometheus.NewDesc(ns+"_transactions_committed_total", "Total committed transactions (requires APOC)", nil, constLabels),
		txOpened:        prometheus.NewDesc(ns+"_transactions_opened_total", "Total opened transactions (requires APOC)", nil, constLabels),
		txRolledBack:    prometheus.NewDesc(ns+"_transactions_rolled_back_total", "Total rolled-back transactions (requires APOC)", nil, constLabels),
		txOpen:          prometheus.NewDesc(ns+"_transactions_open", "Currently open transactions (requires APOC)", nil, constLabels),
		txPeak:          prometheus.NewDesc(ns+"_transactions_peak_concurrent", "Peak concurrent transactions (requires APOC)", nil, constLabels),
		lastCommittedTx: prometheus.NewDesc(ns+"_last_committed_tx_id", "ID of the last committed transaction (requires APOC)", nil, constLabels),

		// ── Advanced / internal ───────────────────────────────────
		heavyQueriesActive: prometheus.NewDesc(ns+"_dbms_heavy_queries_active", "Active queries with elapsed time > 5s", nil, constLabels),
		heavyQueriesFaults: prometheus.NewDesc(ns+"_dbms_heavy_queries_page_faults", "Page faults for heavy queries", nil, constLabels),
		syntheticQueryDur: prometheus.NewDesc(ns+"_synthetic_query_duration_seconds",
			"Latency of synthetic canary query", nil, prometheus.Labels{"target": sanitizedTarget, "query": "RETURN 1"}),
	}

	return c
}

// probeAPOC reports whether APOC procedures are installed on the target. Safe
// to call repeatedly; it runs only once.
func (c *Collector) probeAPOC(ctx context.Context) {
	if c.detected {
		return
	}
	c.detected = true

	records, err := c.run.Query(ctx, systemSessionCfg(),
		"SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc.' RETURN count(*) AS c", nil)
	if err != nil {
		slog.Debug("APOC probe failed", "err", err)
		return
	}
	rec, ok := single(records)
	if !ok {
		return
	}
	val, _ := rec.Get("c")
	count, _ := jmxValue(val)
	c.apocAvailable = count > 0
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
		c.jvmHeapUsed, c.jvmHeapCommitted, c.jvmHeapMax, c.jvmHeapInit,
		c.jvmNonHeapUsed, c.jvmNonHeapCommitted, c.jvmNonHeapMax,
		c.jvmMemoryPoolUsed, c.jvmMemoryPoolCommit, c.jvmMemoryPoolMax,
		c.jvmGCCount, c.jvmGCTime,
		c.osProcessCPULoad, c.osSystemCPULoad, c.osOpenFDs, c.osMaxFDs,
		c.osFreePhysicalMem, c.osCommittedVirtMem, c.osSystemLoadAvg, c.osAvailableProcs,
		c.gdsFreeHeap, c.gdsTotalHeap, c.gdsMaxHeap,
		c.gdsJvmAvailableCPUCores, c.gdsAvailableCPUCoresNotRequested,
		c.gdsOngoingProcedures, c.gdsGraphMemoryBytes, c.gdsTaskMemoryBytes,
		c.dbOnline, c.dbTxActive, c.poolUsedHeap, c.poolUsedNative,
		c.indexesTotal, c.indexesOnline, c.indexesFailed, c.constraintsTotal,
		c.storeSize, c.idsInUse, c.txCommitted, c.txOpened, c.txRolledBack,
		c.txOpen, c.txPeak, c.lastCommittedTx,
		c.heavyQueriesActive, c.heavyQueriesFaults, c.syntheticQueryDur,
	}
}

// Collect implements prometheus.Collector. All queries run concurrently.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()

	start := time.Now()
	err := c.run.VerifyConnectivity(ctx)
	if err != nil {
		slog.Warn("target unreachable", "target", c.target, "err", err)
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)

	c.probeAPOC(ctx)

	var wg sync.WaitGroup

	type namedCollector struct {
		name string
		fn   func(context.Context, chan<- prometheus.Metric)
	}

	collectors := []namedCollector{
		{"nio_buffers", c.collectNIOBufferPools},
		{"threading", c.collectThreading},
		{"classloading", c.collectClassLoading},
		{"runtime", c.collectRuntime},
		{"memory", c.collectMemory},
		{"memory_pools", c.collectMemoryPools},
		{"gc", c.collectGC},
		{"os", c.collectOS},
		{"gds", c.collectGDS},
		{"databases", c.collectDatabases},
		{"transactions", c.collectTransactionsByDatabase},
		{"pools", c.collectPools},
		{"indexes", c.collectIndexes},
		{"constraints", c.collectConstraints},
		{"apoc", c.collectAPOC},
		{"heavy_tx", c.collectHeavyTransactions},
		{"synthetic", c.collectSynthetic},
	}

	for _, nc := range collectors {
		wg.Go(func() {
			phaseStart := time.Now()
			nc.fn(ctx, ch)
			ch <- prometheus.MustNewConstMetric(
				c.scrapeDuration, prometheus.GaugeValue,
				time.Since(phaseStart).Seconds(), nc.name,
			)
		})
	}

	wg.Wait()

	ch <- prometheus.MustNewConstMetric(
		c.scrapeDuration, prometheus.GaugeValue,
		time.Since(start).Seconds(), "total",
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

type jmxMetricConfig struct {
	desc  *prometheus.Desc
	mtype prometheus.ValueType
}

// jmxQueryMulti runs a JMX query returning all attributes as a map, then emits multiple metrics.
func (c *Collector) jmxQueryMulti(ctx context.Context, ch chan<- prometheus.Metric,
	mbean string, attrs map[string]jmxMetricConfig,
) {
	records, err := c.run.Query(ctx, systemSessionCfg(),
		jmxQueryAllAttrs, map[string]any{jmxMBeanParam: mbean})
	if err != nil {
		slog.Warn("JMX multi query failed", "mbean", mbean, "err", err)
		return
	}
	rec, ok := single(records)
	if !ok {
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
		ch <- prometheus.MustNewConstMetric(cfg.desc, cfg.mtype, fval)
	}
}
