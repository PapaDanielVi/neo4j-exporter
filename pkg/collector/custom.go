package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"
)

const customScrapeTimeout = 10 * time.Second

// CustomQueries holds the YAML-defined custom metric queries.
type CustomQueries struct {
	Queries []CustomQuery `yaml:"custom_queries"`
}

// CustomQuery defines a single custom metric from YAML. The query must return a
// numeric column; Value names it, defaulting to "value" then the first numeric
// column found. Labels are emitted as constant labels on the metric.
type CustomQuery struct {
	Query      string            `yaml:"query"`
	MetricName string            `yaml:"metric_name"`
	Type       string            `yaml:"type"`
	Help       string            `yaml:"help"`
	Value      string            `yaml:"value"`
	Labels     map[string]string `yaml:"labels"`
}

// CustomCollector executes YAML-defined queries and emits their results.
type CustomCollector struct {
	run     Runner
	metrics []customMetric
}

type customMetric struct {
	desc     *prometheus.Desc
	query    string
	valueCol string
	mtype    prometheus.ValueType
}

// LoadCustomQueries reads and parses the YAML file.
func LoadCustomQueries(path string) (*CustomQueries, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &CustomQueries{}, nil
		}
		return nil, fmt.Errorf("reading custom queries file: %w", err)
	}
	var cq CustomQueries
	if err := yaml.Unmarshal(data, &cq); err != nil {
		return nil, fmt.Errorf("parsing custom queries YAML: %w", err)
	}
	return &cq, nil
}

// NewCustomCollector builds a CustomCollector from parsed YAML queries.
func NewCustomCollector(cq *CustomQueries, driver neo4j.DriverWithContext) *CustomCollector {
	return NewCustomCollectorWithRunner(cq, driverRunner{driver: driver})
}

// NewCustomCollectorWithRunner builds a CustomCollector backed by an arbitrary
// Runner. It exists so custom queries can be driven by fakes in tests.
func NewCustomCollectorWithRunner(cq *CustomQueries, r Runner) *CustomCollector {
	cc := &CustomCollector{run: r}
	for _, q := range cq.Queries {
		vt := prometheus.GaugeValue
		if q.Type == "counter" {
			vt = prometheus.CounterValue
		}
		cc.metrics = append(cc.metrics, customMetric{
			desc:     prometheus.NewDesc(q.MetricName, q.Help, nil, q.Labels),
			query:    q.Query,
			valueCol: q.Value,
			mtype:    vt,
		})
	}
	return cc
}

// Describe implements prometheus.Collector.
func (cc *CustomCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range cc.metrics {
		ch <- m.desc
	}
}

// Collect runs each custom query and emits its scalar result.
func (cc *CustomCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), customScrapeTimeout)
	defer cancel()

	for _, m := range cc.metrics {
		records, err := cc.run.Query(ctx, readSessionCfg(), m.query, nil)
		if err != nil {
			slog.Warn("custom query failed", "query", m.query, "err", err)
			continue
		}
		rec, ok := single(records)
		if !ok {
			slog.Warn("custom query must return exactly one row", "query", m.query, "rows", len(records))
			continue
		}
		val, ok := customValue(rec, m.valueCol)
		if !ok {
			slog.Warn("custom query has no numeric value column", "query", m.query)
			continue
		}
		ch <- prometheus.MustNewConstMetric(m.desc, m.mtype, val)
	}
}

// customValue picks the metric value from a record: the named column when set,
// otherwise a column named "value" or "count", otherwise the first numeric column.
func customValue(rec *neo4j.Record, col string) (float64, bool) {
	if col != "" {
		return jmxValue(recordValue(rec, col))
	}
	for _, preferred := range []string{"value", "count"} {
		if v, ok := jmxValue(recordValue(rec, preferred)); ok {
			return v, true
		}
	}
	for _, key := range rec.Keys {
		if v, ok := jmxValue(recordValue(rec, key)); ok {
			return v, true
		}
	}
	return 0, false
}
