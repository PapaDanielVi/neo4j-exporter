package collector

import (
	"context"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	memoryMBean       = "java.lang:type=Memory"
	memoryPoolMBean   = "java.lang:type=MemoryPool,name=*"
	gcMBean           = "java.lang:type=GarbageCollector,name=*"
	osMBean           = "java.lang:type=OperatingSystem"
	jmxAttrUsage      = "Usage"
	jmxFieldUsed      = "used"
	jmxFieldCommitted = "committed"
	jmxFieldMax       = "max"
)

// jmxComposite extracts a numeric sub-field from a composite JMX attribute such
// as HeapMemoryUsage. dbms.queryJmx reports composites as
// {"value": {"properties": {"used": ..., "committed": ..., ...}}}, so both the
// "value" and "properties" layers are unwrapped when present.
func jmxComposite(attrs map[string]any, attr, field string) (float64, bool) {
	raw, ok := attrs[attr]
	if !ok || raw == nil {
		return 0, false
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return 0, false
	}
	if inner, ok := m["value"].(map[string]any); ok {
		m = inner
	}
	if props, ok := m["properties"].(map[string]any); ok {
		m = props
	}
	return jmxValue(m[field])
}

// beanNameProperty extracts the name=... property from a JMX object name, e.g.
// "java.lang:type=MemoryPool,name=G1 Eden Space" yields "G1 Eden Space".
func beanNameProperty(objectName string) string {
	for part := range strings.SplitSeq(objectName, ",") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(part), "name="); ok {
			return v
		}
	}
	return objectName
}

// recordAttributes returns the name and attributes map from a name+attributes record.
func recordAttributes(rec *neo4j.Record) (string, map[string]any, bool) {
	nameVal, _ := rec.Get("name")
	name, _ := nameVal.(string)
	attrsVal, _ := rec.Get("attributes")
	attrs, ok := attrsVal.(map[string]any)
	if !ok {
		return "", nil, false
	}
	return name, attrs, true
}

// ── JVM memory (heap / non-heap) ────────────────────────────────────

func (c *Collector) collectMemory(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	rec, ok := single(c.jmxAttrs(ctx, memoryMBean))
	if !ok {
		return
	}
	attrsVal, _ := rec.Get("attributes")
	attrs, ok := attrsVal.(map[string]any)
	if !ok {
		return
	}

	heap := []struct {
		field string
		desc  *prometheus.Desc
	}{
		{jmxFieldUsed, c.jvmHeapUsed},
		{jmxFieldCommitted, c.jvmHeapCommitted},
		{jmxFieldMax, c.jvmHeapMax},
		{"init", c.jvmHeapInit},
	}
	for _, h := range heap {
		if v, ok := jmxComposite(attrs, "HeapMemoryUsage", h.field); ok {
			ch <- prometheus.MustNewConstMetric(h.desc, prometheus.GaugeValue, v, labels...)
		}
	}

	nonHeap := []struct {
		field string
		desc  *prometheus.Desc
	}{
		{jmxFieldUsed, c.jvmNonHeapUsed},
		{jmxFieldCommitted, c.jvmNonHeapCommitted},
		{jmxFieldMax, c.jvmNonHeapMax},
	}
	for _, h := range nonHeap {
		if v, ok := jmxComposite(attrs, "NonHeapMemoryUsage", h.field); ok {
			ch <- prometheus.MustNewConstMetric(h.desc, prometheus.GaugeValue, v, labels...)
		}
	}
}

// ── JVM memory pools ────────────────────────────────────────────────

func (c *Collector) collectMemoryPools(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	for _, rec := range c.jmxNamed(ctx, memoryPoolMBean) {
		name, attrs, ok := recordAttributes(rec)
		if !ok || name == "" {
			continue
		}
		poolLabels := append(append([]string{}, labels...), beanNameProperty(name))
		for field, desc := range map[string]*prometheus.Desc{
			jmxFieldUsed:      c.jvmMemoryPoolUsed,
			jmxFieldCommitted: c.jvmMemoryPoolCommit,
			jmxFieldMax:       c.jvmMemoryPoolMax,
		} {
			if v, ok := jmxComposite(attrs, jmxAttrUsage, field); ok {
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, poolLabels...)
			}
		}
	}
}

// ── JVM garbage collection ──────────────────────────────────────────

func (c *Collector) collectGC(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	for _, rec := range c.jmxNamed(ctx, gcMBean) {
		name, attrs, ok := recordAttributes(rec)
		if !ok || name == "" {
			continue
		}
		gcLabels := append(append([]string{}, labels...), beanNameProperty(name))
		if v, ok := jmxValue(attrs["CollectionCount"]); ok {
			ch <- prometheus.MustNewConstMetric(c.jvmGCCount, prometheus.CounterValue, v, gcLabels...)
		}
		if v, ok := jmxValue(attrs["CollectionTime"]); ok {
			// CollectionTime is reported in milliseconds.
			ch <- prometheus.MustNewConstMetric(c.jvmGCTime, prometheus.CounterValue, v/1000.0, gcLabels...)
		}
	}
}

// ── Operating system ────────────────────────────────────────────────

func (c *Collector) collectOS(ctx context.Context, ch chan<- prometheus.Metric, labels []string) {
	rec, ok := single(c.jmxAttrs(ctx, osMBean))
	if !ok {
		return
	}
	attrsVal, _ := rec.Get("attributes")
	attrs, ok := attrsVal.(map[string]any)
	if !ok {
		return
	}
	for attr, desc := range map[string]*prometheus.Desc{
		"ProcessCpuLoad":             c.osProcessCPULoad,
		"SystemCpuLoad":              c.osSystemCPULoad,
		"OpenFileDescriptorCount":    c.osOpenFDs,
		"MaxFileDescriptorCount":     c.osMaxFDs,
		"FreePhysicalMemorySize":     c.osFreePhysicalMem,
		"CommittedVirtualMemorySize": c.osCommittedVirtMem,
		"SystemLoadAverage":          c.osSystemLoadAvg,
		"AvailableProcessors":        c.osAvailableProcs,
	} {
		if v, ok := jmxValue(attrs[attr]); ok {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, labels...)
		}
	}
}

// jmxAttrs runs the single-bean attributes query.
func (c *Collector) jmxAttrs(ctx context.Context, mbean string) []*neo4j.Record {
	records, err := c.run.Query(ctx, systemSessionCfg(), jmxQueryAllAttrs, map[string]any{jmxMBeanParam: mbean})
	if err != nil {
		return nil
	}
	return records
}

// jmxNamed runs the wildcard name+attributes query.
func (c *Collector) jmxNamed(ctx context.Context, mbean string) []*neo4j.Record {
	records, err := c.run.Query(ctx, systemSessionCfg(), jmxQueryNameAttrs, map[string]any{jmxMBeanParam: mbean})
	if err != nil {
		return nil
	}
	return records
}
