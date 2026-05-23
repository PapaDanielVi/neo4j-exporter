package collector

import (
	"fmt"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"
)

// CustomQueries holds the YAML-defined custom metric queries.
type CustomQueries struct {
	Queries []CustomQuery `yaml:"custom_queries"`
}

// CustomQuery defines a single custom metric from YAML.
type CustomQuery struct {
	Query      string            `yaml:"query"`
	MetricName string            `yaml:"metric_name"`
	Type       string            `yaml:"type"`
	Help       string            `yaml:"help"`
	Labels     map[string]string `yaml:"labels"`
}

// CustomCollector holds dynamically-created Prometheus metrics from YAML.
type CustomCollector struct {
	mu      sync.RWMutex
	metrics []customMetric
}

type customMetric struct {
	desc  *prometheus.Desc
	query string
	mtype prometheus.ValueType
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
func NewCustomCollector(cq *CustomQueries) *CustomCollector {
	cc := &CustomCollector{}
	for _, q := range cq.Queries {
		var vt prometheus.ValueType
		switch q.Type {
		case "counter":
			vt = prometheus.CounterValue
		default:
			vt = prometheus.GaugeValue
		}
		cc.metrics = append(cc.metrics, customMetric{
			desc:  prometheus.NewDesc(q.MetricName, q.Help, nil, q.Labels),
			query: q.Query,
			mtype: vt,
		})
	}
	return cc
}

// Describe implements prometheus.Collector.
func (cc *CustomCollector) Describe(ch chan<- *prometheus.Desc) {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	for _, m := range cc.metrics {
		ch <- m.desc
	}
}

// Collect is a placeholder — actual query execution happens in the main collector.
func (cc *CustomCollector) Collect(ch chan<- prometheus.Metric) {
	// Custom YAML queries are executed inline by the main collector.
}
