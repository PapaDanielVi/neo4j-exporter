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
	up                   *prometheus.Desc
	scrapeDuration       *prometheus.Desc
	driverPoolActive     *prometheus.Desc

	// JMX metrics
	nodeCount            *prometheus.Desc
	relCount             *prometheus.Desc
	propCount            *prometheus.Desc
	txCommitted          *prometheus.Desc
	txRolledBack         *prometheus.Desc
	txActive             *prometheus.Desc
	pageCacheHits        *prometheus.Desc
	pageCacheFaults      *prometheus.Desc
	pageCacheFlushes     *prometheus.Desc
	storeSize            *prometheus.Desc
	jvmMemoryPoolUsed    *prometheus.Desc
	jvmGCTime            *prometheus.Desc
	jvmCPULoad           *prometheus.Desc
	osOpenFDs            *prometheus.Desc

	// Advanced metrics
	heavyQueriesActive   *prometheus.Desc
	heavyQueriesFaults   *prometheus.Desc
	syntheticQueryDur    *prometheus.Desc
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
		osOpenFDs: prometheus.NewDesc(
			ns+"_os_open_file_descriptors",
			"Number of open file descriptors",
			labels, nil,
		),
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
	ch <- c.osOpenFDs
	ch <- c.heavyQueriesActive
	ch <- c.heavyQueriesFaults
	ch <- c.syntheticQueryDur
}

// Collect implements prometheus.Collector. All queries run concurrently.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	targetLabel := []string{c.target}
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()

	// Verify connectivity
	start := time.Now()
	err := c.driver.VerifyConnectivity(ctx)
	if err != nil {
		slog.Warn("target unreachable", "target", c.target, "err", err)
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0, targetLabel...)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1, targetLabel...)

	var wg sync.WaitGroup

	// JMX metrics
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

	// Heavy transactions
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

	// Synthetic latency
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

	// Total scrape duration
	ch <- prometheus.MustNewConstMetric(
		c.scrapeDuration, prometheus.GaugeValue,
		time.Since(start).Seconds(), append(targetLabel, "total")...,
	)
}

func (c *Collector) collectJMX(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	jmxQueries := map[string]struct {
		desc    *prometheus.Desc
		mbean   string
		attr    string
	}{
		"Primitive count": {c.nodeCount, "org.neo4j:instance=0,name=Primitive count", "NumberOfNodeIdsInUse"},
		"Primitive count rel": {c.relCount, "org.neo4j:instance=0,name=Primitive count", "NumberOfRelationshipIdsInUse"},
		"Primitive count prop": {c.propCount, "org.neo4j:instance=0,name=Primitive count", "NumberOfPropertyIdsInUse"},
		"Transactions": {c.txCommitted, "org.neo4j:instance=0,name=Transactions", "NumberOfCommittedTransactions"},
		"Transactions rb": {c.txRolledBack, "org.neo4j:instance=0,name=Transactions", "NumberOfRolledBackTransactions"},
		"Transactions active": {c.txActive, "org.neo4j:instance=0,name=Transactions", "NumberOfOpenedTransactions"},
		"Page cache": {c.pageCacheHits, "org.neo4j:instance=0,name=Page cache", "Hits"},
		"Page cache faults": {c.pageCacheFaults, "org.neo4j:instance=0,name=Page cache", "Faults"},
		"Page cache flushes": {c.pageCacheFlushes, "org.neo4j:instance=0,name=Page cache", "Flushes"},
		"Store sizes": {c.storeSize, "org.neo4j:instance=0,name=Store file sizes", "TotalStoreSize"},
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

		var fval float64
		switch v := val.(type) {
		case int64:
			fval = float64(v)
		case float64:
			fval = v
		default:
			slog.Warn("unexpected JMX value type", "key", key, "type", fmt.Sprintf("%T", val))
			continue
		}

		ch <- prometheus.MustNewConstMetric(q.desc, prometheus.GaugeValue, fval, labels...)
	}

	// JVM memory pools
	c.collectJVMPools(ctx, ch, labels)
	// JVM GC
	c.collectJVMGC(ctx, ch, labels)
	// OS metrics
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
		var fval float64
		switch v := used.(type) {
		case int64:
			fval = float64(v)
		case float64:
			fval = v
		default:
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
		var fval float64
		switch v := t.(type) {
		case int64:
			fval = float64(v) / 1000.0 // ms → seconds
		case float64:
			fval = v / 1000.0
		default:
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.jvmGCTime, prometheus.CounterValue, fval, append(labels, gcName)...)
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
	switch v := val.(type) {
	case float64:
		ch <- prometheus.MustNewConstMetric(c.jvmCPULoad, prometheus.GaugeValue, v, labels...)
	}
}

func (c *Collector) collectOS(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.queryJmx('java.lang:type=OperatingSystem') YIELD attributes RETURN attributes['OpenFileDescriptorCount'] AS fds", nil)
	if err != nil {
		slog.Warn("OS FD query failed", "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		return
	}
	val, _ := rec.Get("fds")
	switch v := val.(type) {
	case int64:
		ch <- prometheus.MustNewConstMetric(c.osOpenFDs, prometheus.GaugeValue, float64(v), labels...)
	case float64:
		ch <- prometheus.MustNewConstMetric(c.osOpenFDs, prometheus.GaugeValue, v, labels...)
	}
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
