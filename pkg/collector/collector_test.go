package collector_test

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/PapaDanielVi/neo4j-exporter/pkg/collector"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const testTarget = "bolt://test:7687"

// stub maps a query substring to a canned result.
type stub struct {
	match   string
	records []*neo4j.Record
	err     error
}

// fakeRunner is a test double for collector.Runner driven by query substrings.
type fakeRunner struct {
	connErr error
	stubs   []stub
}

func (f *fakeRunner) VerifyConnectivity(_ context.Context) error {
	return f.connErr
}

func (f *fakeRunner) Query(_ context.Context, _ neo4j.SessionConfig, cypher string, params map[string]any) ([]*neo4j.Record, error) {
	// JMX bean names arrive as the $mbean parameter rather than in the query
	// text, so match against both the query and that parameter.
	mbean, _ := params["mbean"].(string)
	for _, s := range f.stubs {
		if strings.Contains(cypher, s.match) || strings.Contains(mbean, s.match) {
			return s.records, s.err
		}
	}
	return nil, nil
}

func rec(keys []string, vals ...any) *neo4j.Record {
	return &neo4j.Record{Keys: keys, Values: vals}
}

// gather collects all metrics emitted by c into metric families.
func gather(t *testing.T, c prometheus.Collector) []*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return mfs
}

// metricValue returns the value of the metric named name whose labels are a
// superset of want. The second return reports whether such a metric was found.
func metricValue(mfs []*dto.MetricFamily, name string, want map[string]string) (float64, bool) {
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if !labelsMatch(m, want) {
				continue
			}
			switch mf.GetType() {
			case dto.MetricType_GAUGE:
				return m.GetGauge().GetValue(), true
			case dto.MetricType_COUNTER:
				return m.GetCounter().GetValue(), true
			default:
				return 0, false
			}
		}
	}
	return 0, false
}

func labelsMatch(m *dto.Metric, want map[string]string) bool {
	have := map[string]string{}
	for _, lp := range m.GetLabel() {
		have[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func targetLabels(extra map[string]string) map[string]string {
	out := map[string]string{"target": testTarget}
	maps.Copy(out, extra)
	return out
}

func TestDetectVersionAndEdition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		edition     string
		apocCount   int64
		wantEdition string
		wantAPOC    bool
	}{
		{name: "community no apoc", edition: "community", apocCount: 0, wantEdition: "community", wantAPOC: false},
		{name: "community with apoc", edition: "community", apocCount: 25, wantEdition: "community", wantAPOC: true},
		{name: "enterprise with apoc", edition: "enterprise", apocCount: 25, wantEdition: "enterprise", wantAPOC: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &fakeRunner{
				stubs: []stub{
					{
						match:   "dbms.components",
						records: []*neo4j.Record{rec([]string{"versions", "edition"}, []any{"5.26.0"}, tt.edition)},
					},
					{
						match:   "SHOW PROCEDURES",
						records: []*neo4j.Record{rec([]string{"c"}, tt.apocCount)},
					},
				},
			}
			c := collector.NewWithRunner(testTarget, r)
			c.DetectVersion(context.Background())

			if got := c.Edition(); got != tt.wantEdition {
				t.Errorf("Edition() = %q, want %q", got, tt.wantEdition)
			}
			if got := c.APOCAvailable(); got != tt.wantAPOC {
				t.Errorf("APOCAvailable() = %v, want %v", got, tt.wantAPOC)
			}
		})
	}
}

func TestCollectUnreachable(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{connErr: errors.New("connection refused")}
	c := collector.NewWithRunner(testTarget, r)

	mfs := gather(t, c)

	up, ok := metricValue(mfs, "neo4j_exporter_up", targetLabels(nil))
	if !ok {
		t.Fatal("neo4j_exporter_up not emitted")
	}
	if up != 0 {
		t.Errorf("up = %v, want 0", up)
	}
	if _, ok := metricValue(mfs, "neo4j_jvm_threads_total", targetLabels(nil)); ok {
		t.Error("no JVM metrics should be emitted when target is unreachable")
	}
}

func TestCollectThreading(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "java.lang:type=Threading",
				records: []*neo4j.Record{rec([]string{"attributes"}, map[string]any{
					"PeakThreadCount":   int64(42),
					"DaemonThreadCount": int64(30),
					// Nested-map form exercises the jmxValue map branch.
					"ThreadCount": map[string]any{"value": int64(40)},
				})},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	cases := map[string]float64{
		"neo4j_jvm_threads_peak":   42,
		"neo4j_jvm_threads_daemon": 30,
		"neo4j_jvm_threads_total":  40,
	}
	for name, want := range cases {
		got, ok := metricValue(mfs, name, targetLabels(nil))
		if !ok {
			t.Errorf("%s not emitted", name)
			continue
		}
		if got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestCollectNIOBufferPools(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "java.nio:type=BufferPool",
				records: []*neo4j.Record{
					rec([]string{"name", "attributes"}, "direct", map[string]any{
						"MemoryUsed":    int64(1024),
						"TotalCapacity": int64(2048),
						"Count":         int64(3),
					}),
				},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	want := targetLabels(map[string]string{"pool": "direct"})
	if v, ok := metricValue(mfs, "neo4j_jvm_buffer_pool_used_bytes", want); !ok || v != 1024 {
		t.Errorf("buffer_pool_used_bytes = %v ok=%v, want 1024", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_jvm_buffer_pool_capacity_bytes", want); !ok || v != 2048 {
		t.Errorf("buffer_pool_capacity_bytes = %v ok=%v, want 2048", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_jvm_buffer_pool_count", want); !ok || v != 3 {
		t.Errorf("buffer_pool_count = %v ok=%v, want 3", v, ok)
	}
}

func TestCollectHeavyTransactions(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match:   "SHOW TRANSACTIONS",
				records: []*neo4j.Record{rec([]string{"heavy_count", "total_faults"}, int64(2), int64(15))},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	if v, ok := metricValue(mfs, "neo4j_dbms_heavy_queries_active", targetLabels(nil)); !ok || v != 2 {
		t.Errorf("heavy_queries_active = %v ok=%v, want 2", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_dbms_heavy_queries_page_faults", targetLabels(nil)); !ok || v != 15 {
		t.Errorf("heavy_queries_page_faults = %v ok=%v, want 15", v, ok)
	}
}

func TestCollectMemory(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "java.lang:type=Memory",
				records: []*neo4j.Record{rec([]string{"attributes"}, map[string]any{
					"HeapMemoryUsage": map[string]any{
						"used": int64(500), "committed": int64(800), "max": int64(1000), "init": int64(256),
					},
					"NonHeapMemoryUsage": map[string]any{
						"used": int64(100), "committed": int64(150), "max": int64(-1),
					},
				})},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	cases := map[string]float64{
		"neo4j_jvm_heap_used_bytes":         500,
		"neo4j_jvm_heap_committed_bytes":    800,
		"neo4j_jvm_heap_max_bytes":          1000,
		"neo4j_jvm_heap_init_bytes":         256,
		"neo4j_jvm_nonheap_used_bytes":      100,
		"neo4j_jvm_nonheap_committed_bytes": 150,
		"neo4j_jvm_nonheap_max_bytes":       -1,
	}
	for name, want := range cases {
		if got, ok := metricValue(mfs, name, targetLabels(nil)); !ok || got != want {
			t.Errorf("%s = %v ok=%v, want %v", name, got, ok, want)
		}
	}
}

func TestCollectMemoryWrappedComposite(t *testing.T) {
	t.Parallel()
	// dbms.queryJmx reports composite attributes as
	// {"value": {"properties": {...}}}, which is how a real Neo4j instance
	// returns HeapMemoryUsage.
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "java.lang:type=Memory",
				records: []*neo4j.Record{rec([]string{"attributes"}, map[string]any{
					"HeapMemoryUsage": map[string]any{
						"description": "HeapMemoryUsage",
						"value": map[string]any{
							"description": "java.lang.management.MemoryUsage",
							"properties":  map[string]any{"used": int64(600), "committed": int64(900), "max": int64(1200), "init": int64(300)},
						},
					},
				})},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	if v, ok := metricValue(mfs, "neo4j_jvm_heap_used_bytes", targetLabels(nil)); !ok || v != 600 {
		t.Errorf("heap_used_bytes = %v ok=%v, want 600", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_jvm_heap_max_bytes", targetLabels(nil)); !ok || v != 1200 {
		t.Errorf("heap_max_bytes = %v ok=%v, want 1200", v, ok)
	}
}

func TestCollectMemoryPools(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "java.lang:type=MemoryPool",
				records: []*neo4j.Record{
					rec([]string{"name", "attributes"}, "java.lang:type=MemoryPool,name=G1 Eden Space",
						map[string]any{"Usage": map[string]any{"used": int64(64), "committed": int64(128), "max": int64(256)}}),
				},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	want := targetLabels(map[string]string{"pool": "G1 Eden Space"})
	if v, ok := metricValue(mfs, "neo4j_jvm_memory_pool_used_bytes", want); !ok || v != 64 {
		t.Errorf("memory_pool_used_bytes = %v ok=%v, want 64", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_jvm_memory_pool_max_bytes", want); !ok || v != 256 {
		t.Errorf("memory_pool_max_bytes = %v ok=%v, want 256", v, ok)
	}
}

func TestCollectGC(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "java.lang:type=GarbageCollector",
				records: []*neo4j.Record{
					rec([]string{"name", "attributes"}, "java.lang:type=GarbageCollector,name=G1 Young Generation",
						map[string]any{"CollectionCount": int64(12), "CollectionTime": int64(3500)}),
				},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	want := targetLabels(map[string]string{"gc": "G1 Young Generation"})
	if v, ok := metricValue(mfs, "neo4j_jvm_gc_collection_count_total", want); !ok || v != 12 {
		t.Errorf("gc_collection_count_total = %v ok=%v, want 12", v, ok)
	}
	// 3500ms must be reported as 3.5 seconds.
	if v, ok := metricValue(mfs, "neo4j_jvm_gc_collection_seconds_total", want); !ok || v != 3.5 {
		t.Errorf("gc_collection_seconds_total = %v ok=%v, want 3.5", v, ok)
	}
}

func TestCollectOS(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "java.lang:type=OperatingSystem",
				records: []*neo4j.Record{rec([]string{"attributes"}, map[string]any{
					"ProcessCpuLoad":          0.25,
					"OpenFileDescriptorCount": int64(120),
					"MaxFileDescriptorCount":  int64(10240),
					"AvailableProcessors":     int64(8),
				})},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	cases := map[string]float64{
		"neo4j_jvm_process_cpu_load":      0.25,
		"neo4j_jvm_open_file_descriptors": 120,
		"neo4j_jvm_max_file_descriptors":  10240,
		"neo4j_jvm_available_processors":  8,
	}
	for name, want := range cases {
		if got, ok := metricValue(mfs, name, targetLabels(nil)); !ok || got != want {
			t.Errorf("%s = %v ok=%v, want %v", name, got, ok, want)
		}
	}
}

func TestCollectDatabases(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "SHOW DATABASES",
				records: []*neo4j.Record{
					rec([]string{"name", "currentStatus", "role"}, "neo4j", "online", "primary"),
					rec([]string{"name", "currentStatus", "role"}, "system", "online", "primary"),
					rec([]string{"name", "currentStatus", "role"}, "stopped", "offline", "primary"),
				},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	if v, ok := metricValue(mfs, "neo4j_database_online", targetLabels(map[string]string{"database": "neo4j", "role": "primary"})); !ok || v != 1 {
		t.Errorf("database_online{neo4j} = %v ok=%v, want 1", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_database_online", targetLabels(map[string]string{"database": "stopped", "role": "primary"})); !ok || v != 0 {
		t.Errorf("database_online{stopped} = %v ok=%v, want 0", v, ok)
	}
}

func TestCollectTransactionsByDatabase(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "SHOW TRANSACTIONS YIELD database",
				records: []*neo4j.Record{
					rec([]string{"database", "active"}, "neo4j", int64(3)),
					rec([]string{"database", "active"}, "system", int64(1)),
				},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	if v, ok := metricValue(mfs, "neo4j_database_transactions_active", targetLabels(map[string]string{"database": "neo4j"})); !ok || v != 3 {
		t.Errorf("transactions_active{neo4j} = %v ok=%v, want 3", v, ok)
	}
}

func TestCollectPools(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "dbms.listPools",
				records: []*neo4j.Record{
					rec([]string{"pool", "databaseName", "heapMemoryUsedBytes", "nativeMemoryUsedBytes", "freeMemory"},
						"transaction", "neo4j", int64(2048), int64(4096), "1.0 GiB"),
				},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	want := targetLabels(map[string]string{"pool": "transaction", "database": "neo4j"})
	if v, ok := metricValue(mfs, "neo4j_dbms_pool_used_heap_bytes", want); !ok || v != 2048 {
		t.Errorf("pool_used_heap_bytes = %v ok=%v, want 2048", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_dbms_pool_used_native_bytes", want); !ok || v != 4096 {
		t.Errorf("pool_used_native_bytes = %v ok=%v, want 4096", v, ok)
	}
}

func TestCollectIndexesAndConstraints(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "SHOW INDEXES",
				records: []*neo4j.Record{
					rec([]string{"state"}, "ONLINE"),
					rec([]string{"state"}, "ONLINE"),
					rec([]string{"state"}, "FAILED"),
					rec([]string{"state"}, "POPULATING"),
				},
			},
			{
				match: "SHOW CONSTRAINTS",
				records: []*neo4j.Record{
					rec([]string{"name"}, "c1"),
					rec([]string{"name"}, "c2"),
				},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	cases := map[string]float64{
		"neo4j_indexes_total":     4,
		"neo4j_indexes_online":    2,
		"neo4j_indexes_failed":    1,
		"neo4j_constraints_total": 2,
	}
	for name, want := range cases {
		if got, ok := metricValue(mfs, name, targetLabels(nil)); !ok || got != want {
			t.Errorf("%s = %v ok=%v, want %v", name, got, ok, want)
		}
	}
}

// apocStubs returns the detection and monitor stubs for an APOC-enabled instance.
func apocStubs() []stub {
	return []stub{
		{match: "dbms.components", records: []*neo4j.Record{rec([]string{"versions", "edition"}, []any{"5.26.0"}, "community")}},
		{match: "SHOW PROCEDURES", records: []*neo4j.Record{rec([]string{"c"}, int64(50))}},
		{match: "apoc.monitor.store", records: []*neo4j.Record{rec(
			[]string{"nodeStoreSize", "relStoreSize", "propStoreSize", "totalStoreSize"},
			int64(1000), int64(2000), int64(500), int64(3500))}},
		{match: "apoc.monitor.ids", records: []*neo4j.Record{rec(
			[]string{"nodeIds", "relIds", "propIds", "relTypeIds"},
			int64(40), int64(80), int64(120), int64(5))}},
		{match: "apoc.monitor.tx", records: []*neo4j.Record{rec(
			[]string{"totalTx", "totalOpenedTx", "rolledBackTx", "currentOpenedTx", "peakTx", "lastTxId"},
			int64(900), int64(950), int64(10), int64(3), int64(20), int64(12345))}},
	}
}

func TestCollectAPOCMetrics(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{stubs: apocStubs()}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	cases := []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"neo4j_store_size_bytes", map[string]string{"type": "node"}, 1000},
		{"neo4j_store_size_bytes", map[string]string{"type": "total"}, 3500},
		{"neo4j_ids_in_use", map[string]string{"kind": "relationship"}, 80},
		{"neo4j_ids_in_use", map[string]string{"kind": "relationship_type"}, 5},
		{"neo4j_transactions_committed_total", nil, 900},
		{"neo4j_transactions_opened_total", nil, 950},
		{"neo4j_transactions_rolled_back_total", nil, 10},
		{"neo4j_transactions_open", nil, 3},
		{"neo4j_transactions_peak_concurrent", nil, 20},
		{"neo4j_last_committed_tx_id", nil, 12345},
	}
	for _, tc := range cases {
		if got, ok := metricValue(mfs, tc.name, targetLabels(tc.labels)); !ok || got != tc.want {
			t.Errorf("%s%v = %v ok=%v, want %v", tc.name, tc.labels, got, ok, tc.want)
		}
	}
}

func TestCollectAPOCSkippedWhenAbsent(t *testing.T) {
	t.Parallel()
	// No SHOW PROCEDURES stub means APOC is not detected, even though the monitor
	// procedures would respond. The collector must not emit APOC metrics.
	stubs := apocStubs()
	filtered := stubs[:0]
	for _, s := range stubs {
		if s.match == "SHOW PROCEDURES" {
			continue
		}
		filtered = append(filtered, s)
	}
	r := &fakeRunner{stubs: filtered}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	if _, ok := metricValue(mfs, "neo4j_store_size_bytes", targetLabels(map[string]string{"type": "node"})); ok {
		t.Error("store_size_bytes should not be emitted when APOC is absent")
	}
	if _, ok := metricValue(mfs, "neo4j_transactions_committed_total", targetLabels(nil)); ok {
		t.Error("transactions_committed_total should not be emitted when APOC is absent")
	}
}

func TestCollectGDSMemorySumsMultipleUsers(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{
		stubs: []stub{
			{
				match: "gds.systemMonitor",
				records: []*neo4j.Record{rec(
					[]string{"freeHeap", "totalHeap", "maxHeap", "jvmAvailableCpuCores", "availableCpuCoresNotRequested", "ongoingCount"},
					int64(100), int64(200), int64(400), int64(8), int64(2), int64(1),
				)},
			},
			{
				// Two rows simulate a multi-user instance; values must be summed.
				match: "gds.memory.summary",
				records: []*neo4j.Record{
					rec([]string{"totalGraphsMemory", "totalTasksMemory"}, int64(10), int64(5)),
					rec([]string{"totalGraphsMemory", "totalTasksMemory"}, int64(20), int64(7)),
				},
			},
		},
	}
	c := collector.NewWithRunner(testTarget, r)
	mfs := gather(t, c)

	if v, ok := metricValue(mfs, "neo4j_gds_jvm_free_heap_bytes", targetLabels(nil)); !ok || v != 100 {
		t.Errorf("gds_jvm_free_heap_bytes = %v ok=%v, want 100", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_gds_ongoing_procedures", targetLabels(nil)); !ok || v != 1 {
		t.Errorf("gds_ongoing_procedures = %v ok=%v, want 1", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_gds_graph_memory_bytes", targetLabels(nil)); !ok || v != 30 {
		t.Errorf("gds_graph_memory_bytes = %v ok=%v, want 30 (10+20)", v, ok)
	}
	if v, ok := metricValue(mfs, "neo4j_gds_task_memory_bytes", targetLabels(nil)); !ok || v != 12 {
		t.Errorf("gds_task_memory_bytes = %v ok=%v, want 12 (5+7)", v, ok)
	}
}
