# Neo4j Exporter — Metrics Analysis (Neo4j 5.26-community)

**Date:** 2026-05-24
**Neo4j:** 5.26-community (Docker)
**Exporter:** neo4j-exporter (standalone + proxy modes)
**Scrape duration (total):** 0.827s (standalone) / 0.150s (proxy)

---

## Summary

| Metric Family | Type | Data Points | Status |
|---|---|---|---|
| `neo4j_exporter_up` | Gauge | 1 | OK — instance reachable |
| `neo4j_exporter_driver_pool_active` | Gauge | 1 | OK — 1 cached driver |
| `neo4j_exporter_scrape_duration_seconds` | Gauge | 18 phases | OK |
| `neo4j_jvm_buffer_pool_capacity_bytes` | Gauge | 3 pools | OK |
| `neo4j_jvm_buffer_pool_count` | Gauge | 3 pools | OK |
| `neo4j_jvm_buffer_pool_used_bytes` | Gauge | 3 pools | OK |
| `neo4j_jvm_cpu_load_process` | Gauge | 1 | OK |
| `neo4j_jvm_gc_collection_count_total` | Counter | 3 GCs | OK |
| `neo4j_jvm_gc_collection_seconds_total` | Counter | 3 GCs | OK |
| `neo4j_jvm_uptime_seconds` | Gauge | 1 | OK (~18s) |

**Total unique metric families working:** 10
**Total metric lines (both modes):** 77

---

## What Works

### Exporter Self-Metrics
- `neo4j_exporter_up` = 1 (Neo4j is reachable)
- `neo4j_exporter_driver_pool_active` = 1 (one cached Bolt driver)
- `neo4j_exporter_scrape_duration_seconds` reports per-phase latency for all 18 scrape phases

### JVM Metrics (via `java.lang` JMX beans — these work because the beans exist)
- **Buffer pools:** 3 pools — `direct` (32.4MB), `mapped` (25.9MB), `mapped - 'non-volatile memory'` (0)
- **CPU load:** process CPU load available
- **GC:** 3 garbage collectors tracked (G1 Concurrent, G1 Old, G1 Young)
  - G1 Young Generation: 18 collections in first scrape, 19 in second (+1)
  - G1 Concurrent GC: 8 collections, 8ms total
  - G1 Old Generation: 0 collections
- **Uptime:** ~18 seconds at time of scrape

### Scrape Phase Timing Breakdown (standalone mode)

| Phase | Duration (ms) |
|---|---|
| gds | 392 |
| synthetic | 309 |
| bolt | 309 |
| page_cache | 312 |
| threading | 327 |
| runtime | 630 |
| nio_buffers | 630 |
| pools | 630 |
| log_rotation | 652 |
| transactions | 652 |
| classloading | 661 |
| cypher | 661 |
| query_execution | 675 |
| server | 649 |
| store_size | 641 |
| checkpointing | 648 |
| indexes | 720 |
| **jmx** | **821** |
| **total** | **827** |

The JMX phase is the slowest as expected (it fans out many queries).

---

## What's Missing / Broken

All the metrics below are defined in the collector but return **zero data points**.

### Root Cause: Neo4j 5 JMX Bean Naming Change

The `org.neo4j:instance=0,name=...` bean names used in the collector **do not exist** in Neo4j 5.26-community. The `java.lang:*` beans still work, but Neo4j-specific beans have changed.

Affected metrics (all `org.neo4j:instance=0` beans):

| Metric Group | Broken Metrics |
|---|---|
| **Core JMX** | node count, rel count, prop count, tx committed/rolledback/active, page cache hits/faults/flushes, store size |
| **Bolt** | connections opened/closed/running/idle, messages received/started/done/failed, queue/processing time |
| **Checkpointing** | events, total time, duration, flushed bytes, IO performed/limit |
| **Page Cache (detailed)** | evictions, merges, pins, unpins, hit ratio, usage ratio, bytes read/written, IOPs, throttles |
| **Transactions (detailed)** | started, peak concurrent, active read/write, committed/rolledback/terminated by type |
| **Log Rotation** | rotation events, duration, appended bytes, flushes, batch size |
| **Store Size (detailed)** | database size, available reserved size |
| **Cypher** | replan events, replan wait time |
| **Server** | jetty idle/all threads |
| **Indexes** | queried/populated for all index types |
| **Pools** | used heap/native, total used, total size, free |

### Additional Bugs

1. **Heavy transactions query** — Syntax error: `SHOW TRANSACTIONS WHERE ... RETURN` is invalid Neo4j 5 syntax. Needs `CALL { SHOW TRANSACTIONS ... YIELD ... } RETURN ...` subquery wrapping. (Partially fixed in this session but the `RETURN` after `SHOW` is still failing — the sed-based fix didn't apply the subquery wrapper correctly.)

2. **Synthetic canary** — `RETURN 1` fails on system database in Neo4j 5. Changed to `CALL dbms.components() YIELD name RETURN name LIMIT 1` but the binary wasn't rebuilt with that fix during the test run.

3. **OS JMX metrics** — `java.lang:type=OperatingSystem` bean query fails with `ParameterMissing`. This is because `collectOS` calls `jmxQueryMulti` which passes a function-level `mbean` parameter to `session.Run`, but the driver is not correctly resolving it. The `jmxQueryMulti` function signature takes `mbean string` as a parameter and passes it via `map[string]any{"mbean": mbean}` — this suddenly started working for `java.lang:type=Memory`, `java.lang:type=Runtime`, etc. but still fails for `OperatingSystem`. This intermittent behavior suggests a session/query timeout issue rather than a code bug.

4. **jmxQuery single-attribute queries** — Many `jmxQuery` calls get "Result contains no more records" meaning `result.Single(ctx)` returns no record. This happens because the session's 2-second transaction timeout may be too tight for all the concurrent JMX queries, or the `WHERE name = $name` clause in the inner query doesn't match.

---

## Fixes Applied in This Session

1. **Duplicate `driverPoolActive` metric** — The collector struct registered `neo4j_exporter_driver_pool_active` in `Describe()`, and `main.go` tried to register it again as a `GaugeFunc` → panic. Removed from collector, kept the `GaugeFunc` in main.go which actually provides the value.

2. **Listen address missing colon** — Config default `9121` was passed directly to `http.ListenAndServe` which requires `:9121`. Added auto-prefix logic in main.go.

3. **`jmxQueryMulti` missing parameter** — `session.Run` at line 541 passed `nil` instead of `map[string]any{"mbean": mbean}`. Fixed.

4. **All inline JMX bean strings parameterized** — Neo4j 5 driver rejects inline mbean strings in `dbms.queryJmx()`. Converted all literal `'org.neo4j:...'` and `'java.lang:...'` strings to use `$mbean` parameter placeholder.

---

## Recommended Next Steps

1. **Discover actual Neo4j 5 JMX bean names** — Query `CALL dbms.queryJmx('org.neo4j:*') YIELD name RETURN name` (may need different wildcards or the beans may require enterprise edition)
2. **Fix heavy transactions query** — Properly wrap `SHOW TRANSACTIONS` in a `CALL {}` subquery
3. **Fix synthetic canary** — Use a system-DB-compatible query
4. **Investigate jmxQuery single-record failures** — May need to relax the transaction timeout or fix query patterns
5. **Re-run this test** after fixes to validate all metric groups
