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
	driver neo4j.DriverWithContext
	target string

	// Exporter self-metrics
	up                *prometheus.Desc
	scrapeDuration    *prometheus.Desc
	driverPoolActive  *prometheus.Desc

	// Core JMX metrics
	nodeCount         *prometheus.Desc
	relCount          *prometheus.Desc
	propCount         *prometheus.Desc
	txCommitted       *prometheus.Desc
	txRolledBack      *prometheus.Desc
	txActive          *prometheus.Desc
	pageCacheHits     *prometheus.Desc
	pageCacheFaults   *prometheus.Desc
	pageCacheFlushes  *prometheus.Desc
	storeSize         *prometheus.Desc

	// JVM metrics
	jvmMemoryPoolUsed *prometheus.Desc
	jvmGCTime         *prometheus.Desc
	jvmCPULoad        *prometheus.Desc
	jvmThreadsPeak    *prometheus.Desc
	jvmThreadsDaemon  *prometheus.Desc
	jvmClassesLoaded  *prometheus.Desc
	jvmClassesUnloaded *prometheus.Desc
	jvmUptime         *prometheus.Desc

	// OS metrics
	osOpenFDs         *prometheus.Desc
	osMaxFDs          *prometheus.Desc
	osPhysFreeBytes   *prometheus.Desc
	osSwapFreeBytes   *prometheus.Desc

	// NIO buffer pool metrics
	bufferPoolUsed     *prometheus.Desc
	bufferPoolCapacity *prometheus.Desc
	bufferPoolCount    *prometheus.Desc

	// Advanced metrics
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

		up: prometheus.NewDesc(
			ns+"_exporter_up",
			"1 if target Neo4j instance is reachable, else 0",
			labels, nil,
		),
		scrapeDuration: prometheus.NewDesc(
			ns+"_exporter_scrape_duration_seconds",
			"Latency of scrape phases",
			append(labels, "phase"), nil,
		),
		driverPoolActive: prometheus.NewDesc(
			ns+"_exporter_driver_pool_active",
			"Number of cached active database connection drivers",
			nil, nil,
		),

		// Core JMX
		nodeCount: prometheus.NewDesc(
			ns+"_database_count_node",
			"Number of node IDs in use",
			labels, nil,
		),
		relCount: prometheus.NewDesc(
			ns+"_database_count_relationship",
			"Number of relationship IDs in use",
			labels, nil,
		),
		propCount: prometheus.NewDesc(
			ns+"_database_count_property",
			"Number of property IDs in use",
			labels, nil,
		),
		txCommitted: prometheus.NewDesc(
			ns+"_database_transaction_committed_total",
			"Total number of committed transactions",
			labels, nil,
		),
		txRolledBack: prometheus.NewDesc(
			ns+"_database_transaction_rollbacks_total",
			"Total number of rolled back transactions",
			labels, nil,
		),
		txActive: prometheus.NewDesc(
			ns+"_database_transaction_active",
			"Number of currently open transactions",
			labels, nil,
		),
		pageCacheHits: prometheus.NewDesc(
			ns+"_dbms_page_cache_hits_total",
			"Total page cache hits",
			labels, nil,
		),
		pageCacheFaults: prometheus.NewDesc(
			ns+"_dbms_page_cache_faults_total",
			"Total page cache faults (disk reads)",
			labels, nil,
		),
		pageCacheFlushes: prometheus.NewDesc(
			ns+"_dbms_page_cache_flushes_total",
			"Total page cache flushes (disk writes)",
			labels, nil,
		),
		storeSize: prometheus.NewDesc(
			ns+"_database_store_size_bytes_total",
			"Total store size in bytes",
			labels, nil,
		),

		// JVM
		jvmMemoryPoolUsed: prometheus.NewDesc(
			ns+"_jvm_memory_pool_used_bytes",
			"JVM memory pool used bytes",
			append(labels, "pool"), nil,
		),
		jvmGCTime: prometheus.NewDesc(
			ns+"_jvm_gc_collection_seconds_total",
			"Total time spent in GC",
			append(labels, "gc"), nil,
		),
		jvmCPULoad: prometheus.NewDesc(
			ns+"_jvm_cpu_load_process",
			"Process CPU load",
			labels, nil,
		),
		jvmThreadsPeak: prometheus.NewDesc(
			ns+"_jvm_threads_peak",
			"Highest number of threads alive at the same time",
			labels, nil,
		),
		jvmThreadsDaemon: prometheus.NewDesc(
			"neo4j_jvm_threads_daemon",
			"Number of daemon threads (GC, internal Neo4j tasks)",
			labels, nil,
		),
		jvmClassesLoaded: prometheus.NewDesc(
			ns+"_jvm_classes_loaded",
			"Number of classes currently loaded in the JVM",
			labels, nil,
		),
		jvmClassesUnloaded: prometheus.NewDesc(
			ns+"_jvm_classes_unloaded_total",
			"Total number of classes unloaded by the JVM",
			labels, nil,
		),
		jvmUptime: prometheus.NewDesc(
			ns+"_jvm_uptime_seconds",
			"Uptime of the Java process in seconds",
			labels, nil,
		),

		// OS
		osOpenFDs: prometheus.NewDesc(
			ns+"_os_file_descriptors_open",
			"Number of open file descriptors (high counts indicate leaks)",
			labels, nil,
		),
		osMaxFDs: prometheus.NewDesc(
			ns+"_os_file_descriptors_max",
			"OS ulimit for file descriptors for the Neo4j process",
			labels, nil,
		),
		osPhysFreeBytes: prometheus.NewDesc(
			ns+"_os_memory_physical_free_bytes",
			"Free physical RAM on the host machine",
			labels, nil,
		),
		osSwapFreeBytes: prometheus.NewDesc(
			ns+"_os_memory_swap_free_bytes",
			"Free swap space on the host (low means OS is paging)",
			labels, nil,
		),

		// NIO buffer pools
		bufferPoolUsed: prometheus.NewDesc(
			ns+"_jvm_buffer_pool_used_bytes",
			"Off-heap memory used by the buffer pool (label: pool=\"direct\" or \"mapped\")",
			append(labels, "pool"), nil,
		),
		bufferPoolCapacity: prometheus.NewDesc(
			ns+"_jvm_buffer_pool_capacity_bytes",
			"Total capacity of buffers in the pool",
			append(labels, "pool"), nil,
		),
		bufferPoolCount: prometheus.NewDesc(
			ns+"_jvm_buffer_pool_count",
			"Number of buffers currently allocated in the pool",
			append(labels, "pool"), nil,
		),

		// Advanced
		heavyQueriesActive: prometheus.NewDesc(
			ns+"_dbms_heavy_queries_active",
			"Number of active queries with elapsed time > 5s",
			labels, nil,
		),
		heavyQueriesFaults: prometheus.NewDesc(
			ns+"_dbms_heavy_queries_page_faults",
			"Sum of page faults for heavy queries",
			labels, nil,
		),
		syntheticQueryDur: prometheus.NewDesc(
			ns+"_synthetic_query_duration_seconds",
			"Latency of synthetic canary query",
			labels,
			prometheus.Labels{"query": "RETURN 1"},
		),
	}

	return c
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.scrapeDuration
	ch <- c.driverPoolActive
	ch <- c.nodeCount
	ch <- c.relCount
	ch <- c.propCount
	ch <- c.txCommitted
	ch <- c.txRolledBack
	ch <- c.txActive
	ch <- c.pageCacheHits
	ch <- c.pageCacheFaults
	ch <- c.pageCacheFlushes
	ch <- c.storeSize
	ch <- c.jvmMemoryPoolUsed
	ch <- c.jvmGCTime
	ch <- c.jvmCPULoad
	ch <- c.jvmThreadsPeak
	ch <- c.jvmThreadsDaemon
	ch <- c.jvmClassesLoaded
	ch <- c.jvmClassesUnloaded
	ch <- c.jvmUptime
	ch <- c.osOpenFDs
	ch <- c.osMaxFDs
	ch <- c.osPhysFreeBytes
	ch <- c.osSwapFreeBytes
	ch <- c.bufferPoolUsed
	ch <- c.bufferPoolCapacity
	ch <- c.bufferPoolCount
	ch <- c.heavyQueriesActive
	ch <- c.heavyQueriesFaults
	ch <- c.syntheticQueryDur
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		phaseStart := time.Now()
		c.collectJMX(ctx, ch, targetLabel)
		ch <- prometheus.MustNewConstMetric(
			c.scrapeDuration, prometheus.GaugeValue,
			time.Since(phaseStart).Seconds(), append(targetLabel, "jmx")...,
		)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		phaseStart := time.Now()
		c.collectNIOBufferPools(ctx, ch, targetLabel)
		ch <- prometheus.MustNewConstMetric(
			c.scrapeDuration, prometheus.GaugeValue,
			time.Since(phaseStart).Seconds(), append(targetLabel, "nio_buffers")...,
		)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		phaseStart := time.Now()
		c.collectThreading(ctx, ch, targetLabel)
		ch <- prometheus.MustNewConstMetric(
			c.scrapeDuration, prometheus.GaugeValue,
			time.Since(phaseStart).Seconds(), append(targetLabel, "threading")...,
		)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		phaseStart := time.Now()
		c.collectClassLoading(ctx, ch, targetLabel)
		ch <- prometheus.MustNewConstMetric(
			c.scrapeDuration, prometheus.GaugeValue,
			time.Since(phaseStart).Seconds(), append(targetLabel, "classloading")...,
		)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		phaseStart := time.Now()
		c.collectRuntime(ctx, ch, targetLabel)
		ch <- prometheus.MustNewConstMetric(
			c.scrapeDuration, prometheus.GaugeValue,
			time.Since(phaseStart).Seconds(), append(targetLabel, "runtime")...,
		)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		phaseStart := time.Now()
		c.collectHeavyTransactions(ctx, ch, targetLabel)
		ch <- prometheus.MustNewConstMetric(
			c.scrapeDuration, prometheus.GaugeValue,
			time.Since(phaseStart).Seconds(), append(targetLabel, "heavy_tx")...,
		)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		phaseStart := time.Now()
		c.collectSynthetic(ctx, ch, targetLabel)
		ch <- prometheus.MustNewConstMetric(
			c.scrapeDuration, prometheus.GaugeValue,
			time.Since(phaseStart).Seconds(), append(targetLabel, "synthetic")...,
		)
	}()

	wg.Wait()

	ch <- prometheus.MustNewConstMetric(
		c.scrapeDuration, prometheus.GaugeValue,
		time.Since(start).Seconds(), append(targetLabel, "total")...,
	)
}

// jmxValue safely extracts a float64 from a JMX attribute value, handling
// the nested map structure returned by dbms.queryJmx for some beans.
func jmxValue(raw any) (float64, bool) {
	switch v := raw.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case map[string]any:
		// Nested JMX CompositeData: try to unwrap {"value": <number>}
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

func (c *Collector) collectJMX(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	jmxQueries := map[string]struct {
		desc  *prometheus.Desc
		mbean string
		attr  string
	}{
		"Primitive count":      {c.nodeCount, "org.neo4j:instance=0,name=Primitive count", "NumberOfNodeIdsInUse"},
		"Primitive count rel":  {c.relCount, "org.neo4j:instance=0,name=Primitive count", "NumberOfRelationshipIdsInUse"},
		"Primitive count prop": {c.propCount, "org.neo4j:instance=0,name=Primitive count", "NumberOfPropertyIdsInUse"},
		"Transactions":         {c.txCommitted, "org.neo4j:instance=0,name=Transactions", "NumberOfCommittedTransactions"},
		"Transactions rb":      {c.txRolledBack, "org.neo4j:instance=0,name=Transactions", "NumberOfRolledBackTransactions"},
		"Transactions active":  {c.txActive, "org.neo4j:instance=0,name=Transactions", "NumberOfOpenedTransactions"},
		"Page cache":           {c.pageCacheHits, "org.neo4j:instance=0,name=Page cache", "Hits"},
		"Page cache faults":    {c.pageCacheFaults, "org.neo4j:instance=0,name=Page cache", "Faults"},
		"Page cache flushes":   {c.pageCacheFlushes, "org.neo4j:instance=0,name=Page cache", "Flushes"},
		"Store sizes":          {c.storeSize, "org.neo4j:instance=0,name=Store file sizes", "TotalStoreSize"},
	}

	for key, q := range jmxQueries {
		session := c.driver.NewSession(ctx, neo4j.SessionConfig{
			AccessMode:   neo4j.AccessModeRead,
			DatabaseName: "system",
		})
		defer session.Close(ctx)

		result, err := session.Run(ctx, "CALL dbms.queryJmx($mbean) YIELD attributes RETURN attributes[$attr] AS value",
			map[string]any{"mbean": q.mbean, "attr": q.attr})
		if err != nil {
			slog.Warn("JMX query failed", "key", key, "err", err)
			continue
		}

		record, err := result.Single(ctx)
		if err != nil {
			slog.Warn("JMX result error", "key", key, "err", err)
			continue
		}

		val, _ := record.Get("value")
		if val == nil {
			continue
		}

		fval, ok := jmxValue(val)
		if !ok {
			slog.Warn("unexpected JMX value type", "key", key, "type", fmt.Sprintf("%T", val))
			continue
		}

		ch <- prometheus.MustNewConstMetric(q.desc, prometheus.GaugeValue, fval, labels...)
	}

	c.collectJVMPools(ctx, ch, labels)
	c.collectJVMGC(ctx, ch, labels)
	c.collectOS(ctx, ch, labels)
}

func (c *Collector) collectJVMPools(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.queryJmx('java.lang:type=MemoryPool,name=*') YIELD name, attributes RETURN name, attributes['Usage.used'] AS used", nil)
	if err != nil {
		slog.Warn("JVM pool query failed", "err", err)
		return
	}

	records, err := result.Collect(ctx)
	if err != nil {
		slog.Warn("JVM pool collect failed", "err", err)
		return
	}

	for _, rec := range records {
		name, _ := rec.Get("name")
		used, _ := rec.Get("used")
		poolName, _ := name.(string)
		if poolName == "" || used == nil {
			continue
		}
		fval, ok := jmxValue(used)
		if !ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.jvmMemoryPoolUsed, prometheus.GaugeValue, fval, append(labels, poolName)...)
	}
}

func (c *Collector) collectJVMGC(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.queryJmx('java.lang:type=GarbageCollector,name=*') YIELD name, attributes RETURN name, attributes['CollectionTime'] AS time", nil)
	if err != nil {
		slog.Warn("JVM GC query failed", "err", err)
		return
	}

	records, err := result.Collect(ctx)
	if err != nil {
		slog.Warn("JVM GC collect failed", "err", err)
		return
	}

	for _, rec := range records {
		name, _ := rec.Get("name")
		t, _ := rec.Get("time")
		gcName, _ := name.(string)
		if gcName == "" || t == nil {
			continue
		}
		fval, ok := jmxValue(t)
		if !ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.jvmGCTime, prometheus.CounterValue, fval/1000.0, append(labels, gcName)...)
	}

	// Process CPU load
	result2, err := session.Run(ctx, "CALL dbms.queryJmx('java.lang:type=OperatingSystem') YIELD attributes RETURN attributes['ProcessCpuLoad'] AS cpu", nil)
	if err != nil {
		slog.Warn("OS CPU query failed", "err", err)
		return
	}
	rec, err := result2.Single(ctx)
	if err != nil {
		return
	}
	val, _ := rec.Get("cpu")
	fval, ok := jmxValue(val)
	if ok {
		ch <- prometheus.MustNewConstMetric(c.jvmCPULoad, prometheus.GaugeValue, fval, labels...)
	}
}

func (c *Collector) collectOS(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	// Query all OS attributes in one shot
	osAttrs := map[string]*prometheus.Desc{
		"OpenFileDescriptorCount": c.osOpenFDs,
		"MaxFileDescriptorCount":  c.osMaxFDs,
		"FreePhysicalMemorySize":  c.osPhysFreeBytes,
		"FreeSwapSpaceSize":       c.osSwapFreeBytes,
	}

	result, err := session.Run(ctx, "CALL dbms.queryJmx('java.lang:type=OperatingSystem') YIELD attributes RETURN attributes", nil)
	if err != nil {
		slog.Warn("OS query failed", "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		return
	}
	rawAttrs, _ := rec.Get("attributes")

	// The attributes field may be a map[string]any or a nested CompositeData map
	attrs, ok := rawAttrs.(map[string]any)
	if !ok {
		slog.Warn("OS attributes unexpected type", "type", fmt.Sprintf("%T", rawAttrs))
		return
	}

	for attrName, desc := range osAttrs {
		val, ok := attrs[attrName]
		if val == nil || !ok {
			continue
		}
		fval, ok := jmxValue(val)
		if !ok {
			slog.Warn("OS attr unexpected type", "attr", attrName, "type", fmt.Sprintf("%T", val))
			continue
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, fval, labels...)
	}
}

// collectNIOBufferPools queries java.nio:type=BufferPool,name=* for off-heap memory metrics.
func (c *Collector) collectNIOBufferPools(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.queryJmx('java.nio:type=BufferPool,name=*') YIELD name, attributes RETURN name, attributes", nil)
	if err != nil {
		slog.Warn("NIO buffer pool query failed", "err", err)
		return
	}

	records, err := result.Collect(ctx)
	if err != nil {
		slog.Warn("NIO buffer pool collect failed", "err", err)
		return
	}

	for _, rec := range records {
		nameVal, _ := rec.Get("name")
		attrsVal, _ := rec.Get("attributes")
		poolName, ok := nameVal.(string)
		if !ok || poolName == "" {
			continue
		}
		attrs, ok := attrsVal.(map[string]any)
		if !ok {
			continue
		}
		poolLabels := append(labels, poolName)

		if memUsed, ok := attrs["MemoryUsed"]; ok {
			if fval, ok := jmxValue(memUsed); ok {
				ch <- prometheus.MustNewConstMetric(c.bufferPoolUsed, prometheus.GaugeValue, fval, poolLabels...)
			}
		}
		if capacity, ok := attrs["TotalCapacity"]; ok {
			if fval, ok := jmxValue(capacity); ok {
				ch <- prometheus.MustNewConstMetric(c.bufferPoolCapacity, prometheus.GaugeValue, fval, poolLabels...)
			}
		}
		if count, ok := attrs["Count"]; ok {
			if fval, ok := jmxValue(count); ok {
				ch <- prometheus.MustNewConstMetric(c.bufferPoolCount, prometheus.GaugeValue, fval, poolLabels...)
			}
		}
	}
}

// collectThreading queries java.lang:type=Threading for thread metrics.
func (c *Collector) collectThreading(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.queryJmx('java.lang:type=Threading') YIELD attributes RETURN attributes", nil)
	if err != nil {
		slog.Warn("threading query failed", "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		return
	}
	rawAttrs, _ := rec.Get("attributes")
	attrs, ok := rawAttrs.(map[string]any)
	if !ok {
		slog.Warn("threading attributes unexpected type", "type", fmt.Sprintf("%T", rawAttrs))
		return
	}

	if peak, ok := attrs["PeakThreadCount"]; ok {
		if fval, ok := jmxValue(peak); ok {
			ch <- prometheus.MustNewConstMetric(c.jvmThreadsPeak, prometheus.GaugeValue, fval, labels...)
		}
	}
	if daemon, ok := attrs["DaemonThreadCount"]; ok {
		if fval, ok := jmxValue(daemon); ok {
			ch <- prometheus.MustNewConstMetric(c.jvmThreadsDaemon, prometheus.GaugeValue, fval, labels...)
		}
	}
}

// collectClassLoading queries java.lang:type=ClassLoading for class loader metrics.
func (c *Collector) collectClassLoading(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.queryJmx('java.lang:type=ClassLoading') YIELD attributes RETURN attributes", nil)
	if err != nil {
		slog.Warn("class loading query failed", "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		return
	}
	rawAttrs, _ := rec.Get("attributes")
	attrs, ok := rawAttrs.(map[string]any)
	if !ok {
		slog.Warn("class loading attributes unexpected type", "type", fmt.Sprintf("%T", rawAttrs))
		return
	}

	if loaded, ok := attrs["LoadedClassCount"]; ok {
		if fval, ok := jmxValue(loaded); ok {
			ch <- prometheus.MustNewConstMetric(c.jvmClassesLoaded, prometheus.GaugeValue, fval, labels...)
		}
	}
	if unloaded, ok := attrs["UnloadedClassCount"]; ok {
		if fval, ok := jmxValue(unloaded); ok {
			ch <- prometheus.MustNewConstMetric(c.jvmClassesUnloaded, prometheus.CounterValue, fval, labels...)
		}
	}
}

// collectRuntime queries java.lang:type=Runtime for JVM uptime.
func (c *Collector) collectRuntime(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.queryJmx('java.lang:type=Runtime') YIELD attributes RETURN attributes['Uptime'] AS uptime", nil)
	if err != nil {
		slog.Warn("runtime query failed", "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		return
	}
	val, _ := rec.Get("uptime")
	fval, ok := jmxValue(val)
	if !ok {
		return
	}
	// JMX Uptime is in milliseconds → convert to seconds
	ch <- prometheus.MustNewConstMetric(c.jvmUptime, prometheus.GaugeValue, fval/1000.0, labels...)
}

func (c *Collector) collectHeavyTransactions(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		SHOW TRANSACTIONS
		WHERE elapsedTime.milliseconds > 5000
		RETURN count(*) AS heavy_count, sum(pageFaults) AS total_faults
	`, nil)
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

	var count float64
	switch v := countVal.(type) {
	case int64:
		count = float64(v)
	}

	var faults float64
	switch v := faultsVal.(type) {
	case int64:
		faults = float64(v)
	}

	ch <- prometheus.MustNewConstMetric(c.heavyQueriesActive, prometheus.GaugeValue, count, labels...)
	ch <- prometheus.MustNewConstMetric(c.heavyQueriesFaults, prometheus.GaugeValue, faults, labels...)
}

func (c *Collector) collectSynthetic(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	start := time.Now()
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	_, err := session.Run(ctx, "RETURN 1", nil)
	elapsed := time.Since(start).Seconds()

	if err != nil {
		slog.Warn("synthetic query failed", "err", err)
		return
	}

	ch <- prometheus.MustNewConstMetric(c.syntheticQueryDur, prometheus.GaugeValue, elapsed, labels...)
}
