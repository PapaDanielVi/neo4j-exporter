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
	up             *prometheus.Desc
	scrapeDuration *prometheus.Desc

	// ── Core JMX (existing) ──────────────────────────────────────────
	nodeCount        *prometheus.Desc
	relCount         *prometheus.Desc
	propCount        *prometheus.Desc
	txCommitted      *prometheus.Desc
	txRolledBack     *prometheus.Desc
	txActive         *prometheus.Desc
	pageCacheHits    *prometheus.Desc
	pageCacheFaults  *prometheus.Desc
	pageCacheFlushes *prometheus.Desc
	storeSize        *prometheus.Desc

	// ── JVM ──────────────────────────────────────────────────────────
	jvmMemoryPoolUsed  *prometheus.Desc
	jvmGCTime          *prometheus.Desc
	jvmGCCount         *prometheus.Desc
	jvmCPULoad         *prometheus.Desc
	jvmThreadsPeak     *prometheus.Desc
	jvmThreadsDaemon   *prometheus.Desc
	jvmThreadsTotal    *prometheus.Desc
	jvmClassesLoaded   *prometheus.Desc
	jvmClassesUnloaded *prometheus.Desc
	jvmUptime          *prometheus.Desc
	jvmHeapCommitted   *prometheus.Desc
	jvmHeapUsed        *prometheus.Desc
	jvmHeapMax         *prometheus.Desc
	jvmPauseTime       *prometheus.Desc

	// ── OS ───────────────────────────────────────────────────────────
	osOpenFDs       *prometheus.Desc
	osMaxFDs        *prometheus.Desc
	osPhysFreeBytes *prometheus.Desc
	osSwapFreeBytes *prometheus.Desc

	// ── NIO Buffer Pools ─────────────────────────────────────────────
	bufferPoolUsed     *prometheus.Desc
	bufferPoolCapacity *prometheus.Desc
	bufferPoolCount    *prometheus.Desc

	// ── Bolt (2_NEW_PLAN) ────────────────────────────────────────────
	boltConnsOpened         *prometheus.Desc
	boltConnsClosed         *prometheus.Desc
	boltConnsRunning        *prometheus.Desc
	boltConnsIdle           *prometheus.Desc
	boltMsgsReceived        *prometheus.Desc
	boltMsgsStarted         *prometheus.Desc
	boltMsgsDone            *prometheus.Desc
	boltMsgsFailed          *prometheus.Desc
	boltAccumQueueTime      *prometheus.Desc
	boltAccumProcessingTime *prometheus.Desc

	// ── Checkpointing (2_NEW_PLAN) ───────────────────────────────────
	checkpointEvents       *prometheus.Desc
	checkpointTotalTime    *prometheus.Desc
	checkpointDuration     *prometheus.Desc
	checkpointFlushedBytes *prometheus.Desc
	checkpointLimitMillis  *prometheus.Desc
	checkpointLimitTimes   *prometheus.Desc
	checkpointPagesFlushed *prometheus.Desc
	checkpointIOPerformed  *prometheus.Desc
	checkpointIOLimit      *prometheus.Desc

	// ── Cypher (2_NEW_PLAN) ──────────────────────────────────────────
	cypherReplanEvents   *prometheus.Desc
	cypherReplanWaitTime *prometheus.Desc

	// ── Page Cache detailed (2_NEW_PLAN) ─────────────────────────────
	pcEvictions              *prometheus.Desc
	pcEvictionExceptions     *prometheus.Desc
	pcMerges                 *prometheus.Desc
	pcUnpins                 *prometheus.Desc
	pcPins                   *prometheus.Desc
	pcEvictionsCooperative   *prometheus.Desc
	pcEvictionFlushes        *prometheus.Desc
	pcEvictionCoopFlushes    *prometheus.Desc
	pcPageFaultFailures      *prometheus.Desc
	pcCancelledFaults        *prometheus.Desc
	pcVectoredFaults         *prometheus.Desc
	pcVectoredFaultsFailures *prometheus.Desc
	pcNoPinFaults            *prometheus.Desc
	pcHitRatio               *prometheus.Desc
	pcUsageRatio             *prometheus.Desc
	pcBytesRead              *prometheus.Desc
	pcBytesWritten           *prometheus.Desc
	pcIOPs                   *prometheus.Desc
	pcThrottledTimes         *prometheus.Desc
	pcThrottledMillis        *prometheus.Desc
	pcPagesCopied            *prometheus.Desc

	// ── Transaction detailed (2_NEW_PLAN) ────────────────────────────
	txStarted         *prometheus.Desc
	txPeakConcurrent  *prometheus.Desc
	txActiveRead      *prometheus.Desc
	txActiveWrite     *prometheus.Desc
	txCommittedRead   *prometheus.Desc
	txCommittedWrite  *prometheus.Desc
	txRollbacksRead   *prometheus.Desc
	txRollbacksWrite  *prometheus.Desc
	txTerminated      *prometheus.Desc
	txTerminatedRead  *prometheus.Desc
	txTerminatedWrite *prometheus.Desc
	txLastCommittedID *prometheus.Desc
	txLastClosedID    *prometheus.Desc

	// ── Transaction Log (2_NEW_PLAN) ─────────────────────────────────
	logRotationEvents    *prometheus.Desc
	logRotationTotalTime *prometheus.Desc
	logRotationDuration  *prometheus.Desc
	logAppendedBytes     *prometheus.Desc
	logFlushes           *prometheus.Desc
	logAppendBatchSize   *prometheus.Desc

	// ── Store Size detailed (2_NEW_PLAN) ─────────────────────────────
	storeSizeDatabase *prometheus.Desc
	storeSizeReserved *prometheus.Desc

	// ── Query Execution (2_NEW_PLAN) ─────────────────────────────────
	querySuccess          *prometheus.Desc
	queryFailure          *prometheus.Desc
	queryLatencyMillis    *prometheus.Desc
	queryParallelSuccess  *prometheus.Desc
	queryParallelFailure  *prometheus.Desc
	queryPipelinedSuccess *prometheus.Desc
	queryPipelinedFailure *prometheus.Desc
	querySlottedSuccess   *prometheus.Desc
	querySlottedFailure   *prometheus.Desc

	// ── Server (2_NEW_PLAN) ──────────────────────────────────────────
	serverThreadsJettyIdle *prometheus.Desc
	serverThreadsJettyAll  *prometheus.Desc

	// ── Index (2_NEW_PLAN) ───────────────────────────────────────────
	indexQueried   *prometheus.Desc
	indexPopulated *prometheus.Desc

	// ── Pools (2_NEW_PLAN) ───────────────────────────────────────────
	poolUsedHeap   *prometheus.Desc
	poolUsedNative *prometheus.Desc
	poolTotalUsed  *prometheus.Desc
	poolTotalSize  *prometheus.Desc
	poolFree       *prometheus.Desc

	// ── GDS (GDS_MONITORING_PLAN) ────────────────────────────────────
	gdsFreeHeap                      *prometheus.Desc
	gdsTotalHeap                     *prometheus.Desc
	gdsMaxHeap                       *prometheus.Desc
	gdsJvmAvailableCpuCores          *prometheus.Desc
	gdsAvailableCpuCoresNotRequested *prometheus.Desc
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

		// ── Core JMX ──────────────────────────────────────────────
		nodeCount:        prometheus.NewDesc(ns+"_database_count_node", "Number of node IDs in use", labels, nil),
		relCount:         prometheus.NewDesc(ns+"_database_count_relationship", "Number of relationship IDs in use", labels, nil),
		propCount:        prometheus.NewDesc(ns+"_database_count_property", "Number of property IDs in use", labels, nil),
		txCommitted:      prometheus.NewDesc(ns+"_database_transaction_committed_total", "Total committed transactions", labels, nil),
		txRolledBack:     prometheus.NewDesc(ns+"_database_transaction_rollbacks_total", "Total rolled back transactions", labels, nil),
		txActive:         prometheus.NewDesc(ns+"_database_transaction_active", "Currently active transactions", labels, nil),
		pageCacheHits:    prometheus.NewDesc(ns+"_dbms_page_cache_hits_total", "Total page cache hits", labels, nil),
		pageCacheFaults:  prometheus.NewDesc(ns+"_dbms_page_cache_faults_total", "Total page cache faults (disk reads)", labels, nil),
		pageCacheFlushes: prometheus.NewDesc(ns+"_dbms_page_cache_flushes_total", "Total page cache flushes (disk writes)", labels, nil),
		storeSize:        prometheus.NewDesc(ns+"_database_store_size_bytes_total", "Total store size in bytes", labels, nil),

		// ── JVM ───────────────────────────────────────────────────
		jvmMemoryPoolUsed:  prometheus.NewDesc(ns+"_jvm_memory_pool_used_bytes", "JVM memory pool used bytes", append(labels, "pool"), nil),
		jvmGCTime:          prometheus.NewDesc(ns+"_jvm_gc_collection_seconds_total", "Total time spent in GC", append(labels, "gc"), nil),
		jvmGCCount:         prometheus.NewDesc(ns+"_jvm_gc_collection_count_total", "Total number of GC collections", append(labels, "gc"), nil),
		jvmCPULoad:         prometheus.NewDesc(ns+"_jvm_cpu_load_process", "Process CPU load", labels, nil),
		jvmThreadsPeak:     prometheus.NewDesc(ns+"_jvm_threads_peak", "Peak thread count", labels, nil),
		jvmThreadsDaemon:   prometheus.NewDesc(ns+"_jvm_threads_daemon", "Daemon thread count", labels, nil),
		jvmThreadsTotal:    prometheus.NewDesc(ns+"_jvm_threads_total", "Total live threads", labels, nil),
		jvmClassesLoaded:   prometheus.NewDesc(ns+"_jvm_classes_loaded", "Currently loaded classes", labels, nil),
		jvmClassesUnloaded: prometheus.NewDesc(ns+"_jvm_classes_unloaded_total", "Total unloaded classes", labels, nil),
		jvmUptime:          prometheus.NewDesc(ns+"_jvm_uptime_seconds", "JVM uptime in seconds", labels, nil),
		jvmHeapCommitted:   prometheus.NewDesc(ns+"_jvm_heap_committed_bytes", "Heap memory committed bytes", labels, nil),
		jvmHeapUsed:        prometheus.NewDesc(ns+"_jvm_heap_used_bytes", "Heap memory used bytes", labels, nil),
		jvmHeapMax:         prometheus.NewDesc(ns+"_jvm_heap_max_bytes", "Heap memory max bytes", labels, nil),
		jvmPauseTime:       prometheus.NewDesc(ns+"_jvm_pause_time_seconds_total", "Accumulated VM pause time", labels, nil),

		// ── OS ────────────────────────────────────────────────────
		osOpenFDs:       prometheus.NewDesc(ns+"_os_file_descriptors_open", "Open file descriptors", labels, nil),
		osMaxFDs:        prometheus.NewDesc(ns+"_os_file_descriptors_max", "Max file descriptors (OS ulimit)", labels, nil),
		osPhysFreeBytes: prometheus.NewDesc(ns+"_os_memory_physical_free_bytes", "Free physical RAM bytes", labels, nil),
		osSwapFreeBytes: prometheus.NewDesc(ns+"_os_memory_swap_free_bytes", "Free swap space bytes", labels, nil),

		// ── NIO Buffer Pools ──────────────────────────────────────
		bufferPoolUsed:     prometheus.NewDesc(ns+"_jvm_buffer_pool_used_bytes", "Off-heap buffer pool used bytes", append(labels, "pool"), nil),
		bufferPoolCapacity: prometheus.NewDesc(ns+"_jvm_buffer_pool_capacity_bytes", "Off-heap buffer pool capacity bytes", append(labels, "pool"), nil),
		bufferPoolCount:    prometheus.NewDesc(ns+"_jvm_buffer_pool_count", "Number of buffers in pool", append(labels, "pool"), nil),

		// ── Bolt ──────────────────────────────────────────────────
		boltConnsOpened:         prometheus.NewDesc(ns+"_bolt_connections_opened_total", "Total Bolt connections opened", labels, nil),
		boltConnsClosed:         prometheus.NewDesc(ns+"_bolt_connections_closed_total", "Total Bolt connections closed", labels, nil),
		boltConnsRunning:        prometheus.NewDesc(ns+"_bolt_connections_running", "Currently executing Bolt connections", labels, nil),
		boltConnsIdle:           prometheus.NewDesc(ns+"_bolt_connections_idle", "Idle Bolt connections", labels, nil),
		boltMsgsReceived:        prometheus.NewDesc(ns+"_bolt_messages_received_total", "Total Bolt messages received", labels, nil),
		boltMsgsStarted:         prometheus.NewDesc(ns+"_bolt_messages_started_total", "Total Bolt messages started processing", labels, nil),
		boltMsgsDone:            prometheus.NewDesc(ns+"_bolt_messages_done_total", "Total Bolt messages completed", labels, nil),
		boltMsgsFailed:          prometheus.NewDesc(ns+"_bolt_messages_failed_total", "Total Bolt messages failed", labels, nil),
		boltAccumQueueTime:      prometheus.NewDesc(ns+"_bolt_accumulated_queue_time_ms_total", "Accumulated Bolt queue wait time ms", labels, nil),
		boltAccumProcessingTime: prometheus.NewDesc(ns+"_bolt_accumulated_processing_time_ms_total", "Accumulated Bolt processing time ms", labels, nil),

		// ── Checkpointing ─────────────────────────────────────────
		checkpointEvents:       prometheus.NewDesc(ns+"_check_point_events_total", "Total checkpoint events", labels, nil),
		checkpointTotalTime:    prometheus.NewDesc(ns+"_check_point_total_time_ms_total", "Total checkpoint time ms", labels, nil),
		checkpointDuration:     prometheus.NewDesc(ns+"_check_point_duration_ms", "Last checkpoint duration ms", labels, nil),
		checkpointFlushedBytes: prometheus.NewDesc(ns+"_check_point_flushed_bytes_total", "Total bytes flushed during checkpoints", labels, nil),
		checkpointLimitMillis:  prometheus.NewDesc(ns+"_check_point_limit_millis", "Checkpoint paused by IO limiter ms", labels, nil),
		checkpointLimitTimes:   prometheus.NewDesc(ns+"_check_point_limit_times", "Times checkpoint was paused by IO limiter", labels, nil),
		checkpointPagesFlushed: prometheus.NewDesc(ns+"_check_point_pages_flushed", "Pages flushed in last checkpoint", labels, nil),
		checkpointIOPerformed:  prometheus.NewDesc(ns+"_check_point_io_performed", "IOs performed in last checkpoint", labels, nil),
		checkpointIOLimit:      prometheus.NewDesc(ns+"_check_point_io_limit", "IO limit used in last checkpoint", labels, nil),

		// ── Cypher ────────────────────────────────────────────────
		cypherReplanEvents:   prometheus.NewDesc(ns+"_cypher_replan_events_total", "Total Cypher replan events", labels, nil),
		cypherReplanWaitTime: prometheus.NewDesc(ns+"_cypher_replan_wait_time_seconds_total", "Total seconds waited between replans", labels, nil),

		// ── Page Cache detailed ───────────────────────────────────
		pcEvictions:              prometheus.NewDesc(ns+"_page_cache_evictions_total", "Total page evictions", labels, nil),
		pcEvictionExceptions:     prometheus.NewDesc(ns+"_page_cache_eviction_exceptions_total", "Total eviction exceptions", labels, nil),
		pcMerges:                 prometheus.NewDesc(ns+"_page_cache_merges_total", "Total page merges", labels, nil),
		pcUnpins:                 prometheus.NewDesc(ns+"_page_cache_unpins_total", "Total page unpins", labels, nil),
		pcPins:                   prometheus.NewDesc(ns+"_page_cache_pins_total", "Total page pins", labels, nil),
		pcEvictionsCooperative:   prometheus.NewDesc(ns+"_page_cache_evictions_cooperative_total", "Cooperative page evictions", labels, nil),
		pcEvictionFlushes:        prometheus.NewDesc(ns+"_page_cache_eviction_flushes_total", "Pages flushed by eviction", labels, nil),
		pcEvictionCoopFlushes:    prometheus.NewDesc(ns+"_page_cache_eviction_cooperative_flushes_total", "Pages flushed by cooperative eviction", labels, nil),
		pcPageFaultFailures:      prometheus.NewDesc(ns+"_page_cache_page_fault_failures_total", "Failed page faults", labels, nil),
		pcCancelledFaults:        prometheus.NewDesc(ns+"_page_cache_page_cancelled_faults_total", "Cancelled page faults", labels, nil),
		pcVectoredFaults:         prometheus.NewDesc(ns+"_page_cache_page_vectored_faults_total", "Vectored page faults", labels, nil),
		pcVectoredFaultsFailures: prometheus.NewDesc(ns+"_page_cache_page_vectored_faults_failures_total", "Failed vectored page faults", labels, nil),
		pcNoPinFaults:            prometheus.NewDesc(ns+"_page_cache_page_no_pin_page_faults_total", "Page faults not caused by pins", labels, nil),
		pcHitRatio:               prometheus.NewDesc(ns+"_page_cache_hit_ratio", "Ratio of hits to total lookups (0.0-1.0)", labels, nil),
		pcUsageRatio:             prometheus.NewDesc(ns+"_page_cache_usage_ratio", "Ratio of used pages to available pages (0.0-1.0)", labels, nil),
		pcBytesRead:              prometheus.NewDesc(ns+"_page_cache_bytes_read_total", "Total bytes read by page cache", labels, nil),
		pcBytesWritten:           prometheus.NewDesc(ns+"_page_cache_bytes_written_total", "Total bytes written by page cache", labels, nil),
		pcIOPs:                   prometheus.NewDesc(ns+"_page_cache_iops_total", "Total IO operations by page cache", labels, nil),
		pcThrottledTimes:         prometheus.NewDesc(ns+"_page_cache_throttled_times_total", "Times page cache flush IO was throttled", labels, nil),
		pcThrottledMillis:        prometheus.NewDesc(ns+"_page_cache_throttled_millis_total", "Milliseconds page cache flush IO was throttled", labels, nil),
		pcPagesCopied:            prometheus.NewDesc(ns+"_page_cache_pages_copied_total", "Total page copies in page cache", labels, nil),

		// ── Transaction detailed ──────────────────────────────────
		txStarted:         prometheus.NewDesc(ns+"_transaction_started_total", "Total started transactions", labels, nil),
		txPeakConcurrent:  prometheus.NewDesc(ns+"_transaction_peak_concurrent", "Peak concurrent transactions", labels, nil),
		txActiveRead:      prometheus.NewDesc(ns+"_transaction_active_read", "Active read transactions", labels, nil),
		txActiveWrite:     prometheus.NewDesc(ns+"_transaction_active_write", "Active write transactions", labels, nil),
		txCommittedRead:   prometheus.NewDesc(ns+"_transaction_committed_read_total", "Committed read transactions", labels, nil),
		txCommittedWrite:  prometheus.NewDesc(ns+"_transaction_committed_write_total", "Committed write transactions", labels, nil),
		txRollbacksRead:   prometheus.NewDesc(ns+"_transaction_rollbacks_read_total", "Rolled back read transactions", labels, nil),
		txRollbacksWrite:  prometheus.NewDesc(ns+"_transaction_rollbacks_write_total", "Rolled back write transactions", labels, nil),
		txTerminated:      prometheus.NewDesc(ns+"_transaction_terminated_total", "Terminated transactions", labels, nil),
		txTerminatedRead:  prometheus.NewDesc(ns+"_transaction_terminated_read_total", "Terminated read transactions", labels, nil),
		txTerminatedWrite: prometheus.NewDesc(ns+"_transaction_terminated_write_total", "Terminated write transactions", labels, nil),
		txLastCommittedID: prometheus.NewDesc(ns+"_transaction_last_committed_tx_id", "ID of last committed transaction", labels, nil),
		txLastClosedID:    prometheus.NewDesc(ns+"_transaction_last_closed_tx_id", "ID of last closed transaction", labels, nil),

		// ── Transaction Log ───────────────────────────────────────
		logRotationEvents:    prometheus.NewDesc(ns+"_log_rotation_events_total", "Total transaction log rotations", labels, nil),
		logRotationTotalTime: prometheus.NewDesc(ns+"_log_rotation_total_time_ms_total", "Total log rotation time ms", labels, nil),
		logRotationDuration:  prometheus.NewDesc(ns+"_log_rotation_duration_ms", "Last log rotation duration ms", labels, nil),
		logAppendedBytes:     prometheus.NewDesc(ns+"_log_appended_bytes_total", "Total bytes appended to transaction log", labels, nil),
		logFlushes:           prometheus.NewDesc(ns+"_log_flushes_total", "Total transaction log flushes", labels, nil),
		logAppendBatchSize:   prometheus.NewDesc(ns+"_log_append_batch_size", "Size of last transaction append batch", labels, nil),

		// ── Store Size detailed ───────────────────────────────────
		storeSizeDatabase: prometheus.NewDesc(ns+"_store_size_database_bytes", "Database size in bytes", labels, nil),
		storeSizeReserved: prometheus.NewDesc(ns+"_store_size_available_reserved_bytes", "Reserved but available space in bytes", labels, nil),

		// ── Query Execution ───────────────────────────────────────
		querySuccess:          prometheus.NewDesc(ns+"_db_query_execution_success_total", "Successful queries executed", labels, nil),
		queryFailure:          prometheus.NewDesc(ns+"_db_query_execution_failure_total", "Failed queries executed", labels, nil),
		queryLatencyMillis:    prometheus.NewDesc(ns+"_db_query_execution_latency_millis", "Query execution latency ms", labels, nil),
		queryParallelSuccess:  prometheus.NewDesc(ns+"_db_query_execution_parallel_success_total", "Successful parallel runtime queries", labels, nil),
		queryParallelFailure:  prometheus.NewDesc(ns+"_db_query_execution_parallel_failure_total", "Failed parallel runtime queries", labels, nil),
		queryPipelinedSuccess: prometheus.NewDesc(ns+"_db_query_execution_pipelined_success_total", "Successful pipelined runtime queries", labels, nil),
		queryPipelinedFailure: prometheus.NewDesc(ns+"_db_query_execution_pipelined_failure_total", "Failed pipelined runtime queries", labels, nil),
		querySlottedSuccess:   prometheus.NewDesc(ns+"_db_query_execution_slotted_success_total", "Successful slotted runtime queries", labels, nil),
		querySlottedFailure:   prometheus.NewDesc(ns+"_db_query_execution_slotted_failure_total", "Failed slotted runtime queries", labels, nil),

		// ── Server ────────────────────────────────────────────────
		serverThreadsJettyIdle: prometheus.NewDesc(ns+"_server_threads_jetty_idle", "Idle Jetty threads", labels, nil),
		serverThreadsJettyAll:  prometheus.NewDesc(ns+"_server_threads_jetty_all", "Total Jetty threads", labels, nil),

		// ── Index ─────────────────────────────────────────────────
		indexQueried:   prometheus.NewDesc(ns+"_index_queried_total", "Total index queries", append(labels, "type"), nil),
		indexPopulated: prometheus.NewDesc(ns+"_index_populated_total", "Total index population jobs completed", append(labels, "type"), nil),

		// ── Pools ─────────────────────────────────────────────────
		poolUsedHeap:   prometheus.NewDesc(ns+"_pool_used_heap_bytes", "Used heap memory in pool", append(labels, "pool"), nil),
		poolUsedNative: prometheus.NewDesc(ns+"_pool_used_native_bytes", "Used native memory in pool", append(labels, "pool"), nil),
		poolTotalUsed:  prometheus.NewDesc(ns+"_pool_total_used_bytes", "Total used heap+native memory in pool", append(labels, "pool"), nil),
		poolTotalSize:  prometheus.NewDesc(ns+"_pool_total_size_bytes", "Total capacity of pool", append(labels, "pool"), nil),
		poolFree:       prometheus.NewDesc(ns+"_pool_free_bytes", "Free memory in pool", append(labels, "pool"), nil),

		// ── GDS ───────────────────────────────────────────────────
		gdsFreeHeap:                      prometheus.NewDesc(ns+"_gds_jvm_free_heap_bytes", "Free JVM heap bytes from GDS system monitor", labels, nil),
		gdsTotalHeap:                     prometheus.NewDesc(ns+"_gds_jvm_total_heap_bytes", "Total JVM heap bytes from GDS system monitor", labels, nil),
		gdsMaxHeap:                       prometheus.NewDesc(ns+"_gds_jvm_max_heap_bytes", "Max JVM heap bytes from GDS system monitor", labels, nil),
		gdsJvmAvailableCpuCores:          prometheus.NewDesc(ns+"_gds_jvm_available_cpu_cores", "Logical CPU cores available to JVM", labels, nil),
		gdsAvailableCpuCoresNotRequested: prometheus.NewDesc(ns+"_gds_available_cpu_cores_not_requested", "CPU cores not requested by GDS procedures", labels, nil),
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

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.allDescs() {
		ch <- d
	}
}

func (c *Collector) allDescs() []*prometheus.Desc {
	return []*prometheus.Desc{
		c.up, c.scrapeDuration,
		c.nodeCount, c.relCount, c.propCount,
		c.txCommitted, c.txRolledBack, c.txActive,
		c.pageCacheHits, c.pageCacheFaults, c.pageCacheFlushes, c.storeSize,
		c.jvmMemoryPoolUsed, c.jvmGCTime, c.jvmGCCount, c.jvmCPULoad,
		c.jvmThreadsPeak, c.jvmThreadsDaemon, c.jvmThreadsTotal,
		c.jvmClassesLoaded, c.jvmClassesUnloaded, c.jvmUptime,
		c.jvmHeapCommitted, c.jvmHeapUsed, c.jvmHeapMax, c.jvmPauseTime,
		c.osOpenFDs, c.osMaxFDs, c.osPhysFreeBytes, c.osSwapFreeBytes,
		c.bufferPoolUsed, c.bufferPoolCapacity, c.bufferPoolCount,
		c.boltConnsOpened, c.boltConnsClosed, c.boltConnsRunning, c.boltConnsIdle,
		c.boltMsgsReceived, c.boltMsgsStarted, c.boltMsgsDone, c.boltMsgsFailed,
		c.boltAccumQueueTime, c.boltAccumProcessingTime,
		c.checkpointEvents, c.checkpointTotalTime, c.checkpointDuration,
		c.checkpointFlushedBytes, c.checkpointLimitMillis, c.checkpointLimitTimes,
		c.checkpointPagesFlushed, c.checkpointIOPerformed, c.checkpointIOLimit,
		c.cypherReplanEvents, c.cypherReplanWaitTime,
		c.pcEvictions, c.pcEvictionExceptions, c.pcMerges, c.pcUnpins, c.pcPins,
		c.pcEvictionsCooperative, c.pcEvictionFlushes, c.pcEvictionCoopFlushes,
		c.pcPageFaultFailures, c.pcCancelledFaults, c.pcVectoredFaults,
		c.pcVectoredFaultsFailures, c.pcNoPinFaults,
		c.pcHitRatio, c.pcUsageRatio, c.pcBytesRead, c.pcBytesWritten,
		c.pcIOPs, c.pcThrottledTimes, c.pcThrottledMillis, c.pcPagesCopied,
		c.txStarted, c.txPeakConcurrent, c.txActiveRead, c.txActiveWrite,
		c.txCommittedRead, c.txCommittedWrite, c.txRollbacksRead, c.txRollbacksWrite,
		c.txTerminated, c.txTerminatedRead, c.txTerminatedWrite,
		c.txLastCommittedID, c.txLastClosedID,
		c.logRotationEvents, c.logRotationTotalTime, c.logRotationDuration,
		c.logAppendedBytes, c.logFlushes, c.logAppendBatchSize,
		c.storeSizeDatabase, c.storeSizeReserved,
		c.querySuccess, c.queryFailure, c.queryLatencyMillis,
		c.queryParallelSuccess, c.queryParallelFailure,
		c.queryPipelinedSuccess, c.queryPipelinedFailure,
		c.querySlottedSuccess, c.querySlottedFailure,
		c.serverThreadsJettyIdle, c.serverThreadsJettyAll,
		c.indexQueried, c.indexPopulated,
		c.poolUsedHeap, c.poolUsedNative, c.poolTotalUsed, c.poolTotalSize, c.poolFree,
		c.gdsFreeHeap, c.gdsTotalHeap, c.gdsMaxHeap,
		c.gdsJvmAvailableCpuCores, c.gdsAvailableCpuCoresNotRequested,
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

	var wg sync.WaitGroup

	type namedCollector struct {
		name string
		fn   func(context.Context, chan<- prometheus.Metric, []string)
	}

	collectors := []namedCollector{
		{"jmx", c.collectJMX},
		{"nio_buffers", c.collectNIOBufferPools},
		{"threading", c.collectThreading},
		{"classloading", c.collectClassLoading},
		{"runtime", c.collectRuntime},
		{"bolt", c.collectBolt},
		{"checkpointing", c.collectCheckpointing},
		{"cypher", c.collectCypher},
		{"page_cache", c.collectPageCacheDetailed},
		{"transactions", c.collectTransactionDetailed},
		{"log_rotation", c.collectLogRotation},
		{"store_size", c.collectStoreSizeDetailed},
		{"query_execution", c.collectQueryExecution},
		{"server", c.collectServer},
		{"indexes", c.collectIndexes},
		{"pools", c.collectPools},
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

// jmxQuery runs a single-attribute JMX query and emits the metric.
func (c *Collector) jmxQuery(ctx context.Context, ch chan<- prometheus.Metric, labels []string,
	key string, desc *prometheus.Desc, mbean string, attr string, mtype prometheus.ValueType) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)

	result, err := session.Run(ctx,
		"CALL dbms.queryJmx($mbean) YIELD attributes RETURN attributes[$attr] AS value",
		map[string]any{"mbean": mbean, "attr": attr})
	if err != nil {
		slog.Warn("JMX query failed", "key", key, "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		slog.Warn("JMX result error", "key", key, "err", err)
		return
	}
	val, _ := rec.Get("value")
	if val == nil {
		return
	}
	fval, ok := jmxValue(val)
	if !ok {
		slog.Warn("unexpected JMX value type", "key", key, "type", fmt.Sprintf("%T", val))
		return
	}
	ch <- prometheus.MustNewConstMetric(desc, mtype, fval, labels...)
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

// ── Core JMX (existing beans) ─────────────────────────────────────

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
		c.jmxQuery(ctx, ch, labels, key, q.desc, q.mbean, q.attr, prometheus.GaugeValue)
	}

	c.collectJVMPools(ctx, ch, labels)
	c.collectJVMGC(ctx, ch, labels)
	c.collectOS(ctx, ch, labels)
}

func (c *Collector) collectJVMPools(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)

	result, err := session.Run(ctx,
		"CALL dbms.queryJmx($mbean) YIELD name, attributes RETURN name, attributes['Usage.used'] AS used",
			map[string]any{"mbean": "java.lang:type=MemoryPool,name=*"})
	if err != nil {
		slog.Warn("JVM pool query failed", "err", err)
		return
	}
	records, err := result.Collect(ctx)
	if err != nil {
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
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)

	// GC time
	result, err := session.Run(ctx,
		"CALL dbms.queryJmx($mbean) YIELD name, attributes RETURN name, attributes['CollectionTime'] AS time, attributes['CollectionCount'] AS count",
			map[string]any{"mbean": "java.lang:type=GarbageCollector,name=*"})
	if err != nil {
		slog.Warn("JVM GC query failed", "err", err)
		return
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return
	}
	for _, rec := range records {
		name, _ := rec.Get("name")
		gcName, _ := name.(string)
		if gcName == "" {
			continue
		}
		gcLabels := append(labels, gcName)

		if t, ok := rec.Get("time"); ok && t != nil {
			if fval, ok := jmxValue(t); ok {
				ch <- prometheus.MustNewConstMetric(c.jvmGCTime, prometheus.CounterValue, fval/1000.0, gcLabels...)
			}
		}
		if cnt, ok := rec.Get("count"); ok && cnt != nil {
			if fval, ok := jmxValue(cnt); ok {
				ch <- prometheus.MustNewConstMetric(c.jvmGCCount, prometheus.CounterValue, fval, gcLabels...)
			}
		}
	}

	// Heap metrics
	heapResult, err := session.Run(ctx,
		"CALL dbms.queryJmx($mbean) YIELD attributes RETURN attributes['HeapMemoryUsage'] AS heap",
			map[string]any{"mbean": "java.lang:type=Memory"})
	if err != nil {
		slog.Warn("JVM heap query failed", "err", err)
		return
	}
	heapRec, err := heapResult.Single(ctx)
	if err != nil {
		return
	}
	heapRaw, _ := heapRec.Get("heap")
	heapMap, ok := heapRaw.(map[string]any)
	if !ok {
		return
	}
	for attr, desc := range map[string]*prometheus.Desc{
		"committed": c.jvmHeapCommitted,
		"used":      c.jvmHeapUsed,
		"max":       c.jvmHeapMax,
	} {
		if v, ok := heapMap[attr]; ok && v != nil {
			if fval, ok := jmxValue(v); ok {
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, fval, labels...)
			}
		}
	}

	// Process CPU load
	cpuResult, err := session.Run(ctx,
		"CALL dbms.queryJmx($mbean) YIELD attributes RETURN attributes['ProcessCpuLoad'] AS cpu",
			map[string]any{"mbean": "java.lang:type=OperatingSystem"})
	if err != nil {
		return
	}
	if cpuRec, err := cpuResult.Single(ctx); err == nil {
		if val, _ := cpuRec.Get("cpu"); val != nil {
			if fval, ok := jmxValue(val); ok {
				ch <- prometheus.MustNewConstMetric(c.jvmCPULoad, prometheus.GaugeValue, fval, labels...)
			}
		}
	}
}

func (c *Collector) collectOS(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	c.jmxQueryMulti(ctx, ch, labels, "java.lang:type=OperatingSystem", map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"OpenFileDescriptorCount": {c.osOpenFDs, prometheus.GaugeValue},
		"MaxFileDescriptorCount":  {c.osMaxFDs, prometheus.GaugeValue},
		"FreePhysicalMemorySize":  {c.osPhysFreeBytes, prometheus.GaugeValue},
		"FreeSwapSpaceSize":       {c.osSwapFreeBytes, prometheus.GaugeValue},
	})
}

// ── NIO Buffer Pools ───────────────────────────────────────────────

func (c *Collector) collectNIOBufferPools(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)

	result, err := session.Run(ctx,
		"CALL dbms.queryJmx($mbean) YIELD name, attributes RETURN name, attributes",
			map[string]any{"mbean": "java.nio:type=BufferPool,name=*"})
	if err != nil {
		slog.Warn("NIO buffer pool query failed", "err", err)
		return
	}
	records, err := result.Collect(ctx)
	if err != nil {
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
		poolLabels := append(labels, poolName)
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
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)
	result, err := session.Run(ctx,
		"CALL dbms.queryJmx($mbean) YIELD attributes RETURN attributes['Uptime'] AS uptime",
			map[string]any{"mbean": "java.lang:type=Runtime"})
	if err != nil {
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		return
	}
	val, _ := rec.Get("uptime")
	if fval, ok := jmxValue(val); ok {
		ch <- prometheus.MustNewConstMetric(c.jvmUptime, prometheus.GaugeValue, fval/1000.0, labels...)
	}
}

// ── Bolt ───────────────────────────────────────────────────────────

func (c *Collector) collectBolt(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	boltAttrs := map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"ConnectionsOpened":         {c.boltConnsOpened, prometheus.CounterValue},
		"ConnectionsClosed":         {c.boltConnsClosed, prometheus.CounterValue},
		"ConnectionsRunning":        {c.boltConnsRunning, prometheus.GaugeValue},
		"ConnectionsIdle":           {c.boltConnsIdle, prometheus.GaugeValue},
		"MessagesReceived":          {c.boltMsgsReceived, prometheus.CounterValue},
		"MessagesStarted":           {c.boltMsgsStarted, prometheus.CounterValue},
		"MessagesDone":              {c.boltMsgsDone, prometheus.CounterValue},
		"MessagesFailed":            {c.boltMsgsFailed, prometheus.CounterValue},
		"AccumulatedQueueTime":      {c.boltAccumQueueTime, prometheus.CounterValue},
		"AccumulatedProcessingTime": {c.boltAccumProcessingTime, prometheus.CounterValue},
	}
	c.jmxQueryMulti(ctx, ch, labels, "org.neo4j:instance=0,name=Bolt", boltAttrs)
}

// ── Checkpointing ──────────────────────────────────────────────────

func (c *Collector) collectCheckpointing(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	cpAttrs := map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"Events":       {c.checkpointEvents, prometheus.CounterValue},
		"TotalTime":    {c.checkpointTotalTime, prometheus.CounterValue},
		"Duration":     {c.checkpointDuration, prometheus.GaugeValue},
		"FlushedBytes": {c.checkpointFlushedBytes, prometheus.CounterValue},
		"LimitMillis":  {c.checkpointLimitMillis, prometheus.GaugeValue},
		"LimitTimes":   {c.checkpointLimitTimes, prometheus.GaugeValue},
		"PagesFlushed": {c.checkpointPagesFlushed, prometheus.GaugeValue},
		"IOPerformed":  {c.checkpointIOPerformed, prometheus.GaugeValue},
		"IOLimit":      {c.checkpointIOLimit, prometheus.GaugeValue},
	}
	c.jmxQueryMulti(ctx, ch, labels, "org.neo4j:instance=0,name=Check Pointing", cpAttrs)
}

// ── Cypher ─────────────────────────────────────────────────────────

func (c *Collector) collectCypher(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	c.jmxQuery(ctx, ch, labels, "cypher_replan_events", c.cypherReplanEvents,
		"org.neo4j:instance=0,name=Cypher", "ReplanEvents", prometheus.CounterValue)
	c.jmxQuery(ctx, ch, labels, "cypher_replan_wait_time", c.cypherReplanWaitTime,
		"org.neo4j:instance=0,name=Cypher", "ReplanWaitTime", prometheus.CounterValue)
}

// ── Page Cache detailed ────────────────────────────────────────────

func (c *Collector) collectPageCacheDetailed(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	pcAttrs := map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"Evictions":                  {c.pcEvictions, prometheus.CounterValue},
		"EvictionExceptions":         {c.pcEvictionExceptions, prometheus.CounterValue},
		"Merges":                     {c.pcMerges, prometheus.CounterValue},
		"Unpins":                     {c.pcUnpins, prometheus.CounterValue},
		"Pins":                       {c.pcPins, prometheus.CounterValue},
		"EvictionsCooperative":       {c.pcEvictionsCooperative, prometheus.CounterValue},
		"EvictionFlushes":            {c.pcEvictionFlushes, prometheus.CounterValue},
		"EvictionCooperativeFlushes": {c.pcEvictionCoopFlushes, prometheus.CounterValue},
		"PageFaultFailures":          {c.pcPageFaultFailures, prometheus.CounterValue},
		"CancelledFaults":            {c.pcCancelledFaults, prometheus.CounterValue},
		"VectoredFaults":             {c.pcVectoredFaults, prometheus.CounterValue},
		"VectoredFaultsFailures":     {c.pcVectoredFaultsFailures, prometheus.CounterValue},
		"NoPinPageFaults":            {c.pcNoPinFaults, prometheus.CounterValue},
		"HitRatio":                   {c.pcHitRatio, prometheus.GaugeValue},
		"UsageRatio":                 {c.pcUsageRatio, prometheus.GaugeValue},
		"BytesRead":                  {c.pcBytesRead, prometheus.CounterValue},
		"BytesWritten":               {c.pcBytesWritten, prometheus.CounterValue},
		"IOPs":                       {c.pcIOPs, prometheus.CounterValue},
		"ThrottledTimes":             {c.pcThrottledTimes, prometheus.CounterValue},
		"ThrottledMillis":            {c.pcThrottledMillis, prometheus.CounterValue},
		"PagesCopied":                {c.pcPagesCopied, prometheus.CounterValue},
	}
	c.jmxQueryMulti(ctx, ch, labels, "org.neo4j:instance=0,name=Page cache", pcAttrs)
}

// ── Transaction detailed ───────────────────────────────────────────

func (c *Collector) collectTransactionDetailed(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	txAttrs := map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"Started":           {c.txStarted, prometheus.CounterValue},
		"PeakConcurrent":    {c.txPeakConcurrent, prometheus.GaugeValue},
		"ActiveRead":        {c.txActiveRead, prometheus.GaugeValue},
		"ActiveWrite":       {c.txActiveWrite, prometheus.GaugeValue},
		"CommittedRead":     {c.txCommittedRead, prometheus.CounterValue},
		"CommittedWrite":    {c.txCommittedWrite, prometheus.CounterValue},
		"RollbacksRead":     {c.txRollbacksRead, prometheus.CounterValue},
		"RollbacksWrite":    {c.txRollbacksWrite, prometheus.CounterValue},
		"Terminated":        {c.txTerminated, prometheus.CounterValue},
		"TerminatedRead":    {c.txTerminatedRead, prometheus.CounterValue},
		"TerminatedWrite":   {c.txTerminatedWrite, prometheus.CounterValue},
		"LastCommittedTxId": {c.txLastCommittedID, prometheus.CounterValue},
		"LastClosedTxId":    {c.txLastClosedID, prometheus.CounterValue},
	}
	c.jmxQueryMulti(ctx, ch, labels, "org.neo4j:instance=0,name=Transactions", txAttrs)
}

// ── Log Rotation ───────────────────────────────────────────────────

func (c *Collector) collectLogRotation(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	logAttrs := map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"RotationEvents":    {c.logRotationEvents, prometheus.CounterValue},
		"RotationTotalTime": {c.logRotationTotalTime, prometheus.CounterValue},
		"RotationDuration":  {c.logRotationDuration, prometheus.GaugeValue},
		"AppendedBytes":     {c.logAppendedBytes, prometheus.CounterValue},
		"Flushes":           {c.logFlushes, prometheus.CounterValue},
		"AppendBatchSize":   {c.logAppendBatchSize, prometheus.GaugeValue},
	}
	c.jmxQueryMulti(ctx, ch, labels, "org.neo4j:instance=0,name=Transaction Logs", logAttrs)
}

// ── Store Size detailed ────────────────────────────────────────────

func (c *Collector) collectStoreSizeDetailed(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	storeAttrs := map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"DatabaseSize":          {c.storeSizeDatabase, prometheus.GaugeValue},
		"AvailableReservedSize": {c.storeSizeReserved, prometheus.GaugeValue},
	}
	c.jmxQueryMulti(ctx, ch, labels, "org.neo4j:instance=0,name=Store file sizes", storeAttrs)
}

// ── Query Execution ────────────────────────────────────────────────

func (c *Collector) collectQueryExecution(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	// Query metrics are per-database; query from system database
	queryCypher := `
		CALL dbms.queryJmx($mbean)
		YIELD attributes RETURN attributes
	`
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)

	result, err := session.Run(ctx, queryCypher, map[string]any{"mbean": "neo4j.metrics:database=system,name=Query Execution"})
	if err != nil {
		// Query execution metrics may not be available in all editions
		slog.Debug("query execution JMX query failed", "err", err)
		return
	}
	rec, err := result.Single(ctx)
	if err != nil {
		return
	}
	rawAttrs, _ := rec.Get("attributes")
	attrsMap, ok := rawAttrs.(map[string]any)
	if !ok {
		return
	}

	runtimeMap := map[string]*prometheus.Desc{
		"Success":          c.querySuccess,
		"Failure":          c.queryFailure,
		"ParallelSuccess":  c.queryParallelSuccess,
		"ParallelFailure":  c.queryParallelFailure,
		"PipelinedSuccess": c.queryPipelinedSuccess,
		"PipelinedFailure": c.queryPipelinedFailure,
		"SlottedSuccess":   c.querySlottedSuccess,
		"SlottedFailure":   c.querySlottedFailure,
	}
	for attr, desc := range runtimeMap {
		if v, ok := attrsMap[attr]; ok && v != nil {
			if fval, ok := jmxValue(v); ok {
				ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, fval, labels...)
			}
		}
	}
}

// ── Server ─────────────────────────────────────────────────────────

func (c *Collector) collectServer(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	serverAttrs := map[string]struct {
		desc  *prometheus.Desc
		mtype prometheus.ValueType
	}{
		"JettyIdleThreads": {c.serverThreadsJettyIdle, prometheus.GaugeValue},
		"JettyAllThreads":  {c.serverThreadsJettyAll, prometheus.GaugeValue},
	}
	c.jmxQueryMulti(ctx, ch, labels, "org.neo4j:instance=0,name=Server", serverAttrs)
}

// ── Indexes ────────────────────────────────────────────────────────

func (c *Collector) collectIndexes(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	indexTypes := map[string]string{
		"fulltext": "Fulltext Index",
		"lookup":   "Lookup Index",
		"text":     "Text Index",
		"range":    "Range Index",
		"point":    "Point Index",
		"vector":   "Vector Index",
	}
	for idxType, beanName := range indexTypes {
		c.jmxQuery(ctx, ch, labels, "index_queried_"+idxType, c.indexQueried,
			"org.neo4j:instance=0,name="+beanName, "Queried", prometheus.CounterValue)
		c.jmxQuery(ctx, ch, labels, "index_populated_"+idxType, c.indexPopulated,
			"org.neo4j:instance=0,name="+beanName, "Populated", prometheus.CounterValue)
	}
}

// ── Pools ──────────────────────────────────────────────────────────

func (c *Collector) collectPools(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)

	result, err := session.Run(ctx,
		"CALL dbms.queryJmx($mbean) YIELD name, attributes RETURN name, attributes",
			map[string]any{"mbean": "org.neo4j:instance=0,name=* Pool"})
	if err != nil {
		slog.Debug("pools query failed", "err", err)
		return
	}
	records, err := result.Collect(ctx)
	if err != nil {
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
		poolLabels := append(labels, poolName)
		for attr, desc := range map[string]*prometheus.Desc{
			"UsedHeap":   c.poolUsedHeap,
			"UsedNative": c.poolUsedNative,
			"TotalUsed":  c.poolTotalUsed,
			"TotalSize":  c.poolTotalSize,
			"Free":       c.poolFree,
		} {
			if v, ok := attrsMap[attr]; ok && v != nil {
				if fval, ok := jmxValue(v); ok {
					ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, fval, poolLabels...)
				}
			}
		}
	}
}

// ── GDS (Graph Data Science) ───────────────────────────────────────

func (c *Collector) collectGDS(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
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
		"jvmAvailableCpuCores":          c.gdsJvmAvailableCpuCores,
		"availableCpuCoresNotRequested": c.gdsAvailableCpuCoresNotRequested,
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
		// memory.summary returns one row per user; sum them up
		summaryRecords, err := summaryResult.Collect(ctx)
		if err != nil {
			return
		}
		var totalGraphMem, totalTaskMem float64
		for _, sr := range summaryRecords {
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
		return
	}
	if val, ok := summaryRec.Get("totalGraphsMemory"); ok && val != nil {
		if fval, ok := jmxValue(val); ok {
			ch <- prometheus.MustNewConstMetric(c.gdsGraphMemoryBytes, prometheus.GaugeValue, fval, labels...)
		}
	}
	if val, ok := summaryRec.Get("totalTasksMemory"); ok && val != nil {
		if fval, ok := jmxValue(val); ok {
			ch <- prometheus.MustNewConstMetric(c.gdsTaskMemoryBytes, prometheus.GaugeValue, fval, labels...)
		}
	}
}

// ── Heavy transactions ─────────────────────────────────────────────

func (c *Collector) collectHeavyTransactions(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
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
	var count, faults float64
	switch v := countVal.(type) {
	case int64:
		count = float64(v)
	}
	switch v := faultsVal.(type) {
	case int64:
		faults = float64(v)
	}
	ch <- prometheus.MustNewConstMetric(c.heavyQueriesActive, prometheus.GaugeValue, count, labels...)
	ch <- prometheus.MustNewConstMetric(c.heavyQueriesFaults, prometheus.GaugeValue, faults, labels...)
}

// ── Synthetic canary ───────────────────────────────────────────────

func (c *Collector) collectSynthetic(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	start := time.Now()
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: "system"})
	defer session.Close(ctx)
	_, err := session.Run(ctx, "CALL dbms.components() YIELD name RETURN name LIMIT 1", nil)
	if err != nil {
		slog.Warn("synthetic query failed", "err", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.syntheticQueryDur, prometheus.GaugeValue, time.Since(start).Seconds(), labels...)
}
