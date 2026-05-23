package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/PapaDanielVi/neo4j-exporter/pkg/collector"
	"github.com/PapaDanielVi/neo4j-exporter/pkg/config"
	"github.com/PapaDanielVi/neo4j-exporter/pkg/discovery"
	"github.com/PapaDanielVi/neo4j-exporter/pkg/driverpool"
	"github.com/PapaDanielVi/neo4j-exporter/pkg/luaengine"
)

var (
	// These are set at build time via -ldflags.
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing config: %v\n", err)
		os.Exit(1)
	}

	// Structured logging
	var handler slog.Handler
	if cfg.LogJSON {
		handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	} else {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	slog.Info("starting neo4j-exporter",
		"version", version, "commit", commit, "buildDate", buildDate,
		"listen", cfg.ListenAddress, "standalone_uri", cfg.Neo4jURI,
	)

	pool := driverpool.New()
	defer pool.Close()

	// Lua engine
	luaEng, err := luaengine.New(cfg.LuaScriptsDir)
	if err != nil {
		slog.Warn("lua engine init failed", "err", err)
	}
	_ = luaEng

	// Custom YAML queries
	customQueries, err := collector.LoadCustomQueries(cfg.CustomQueriesFile)
	if err != nil {
		slog.Warn("custom queries load failed", "err", err)
	}
	_ = customQueries

	// Registry for standalone mode
	reg := prometheus.NewRegistry()

	// Get the standalone driver
	standaloneDriver, err := pool.Get(context.Background(), cfg.Neo4jURI, cfg.Neo4jUser, cfg.Neo4jPassword)
	if err != nil {
		slog.Warn("standalone driver init failed (proxy mode still available)", "err", err)
	} else {
		coll := collector.New(cfg.Neo4jURI, standaloneDriver)
		reg.MustRegister(coll)
	}

	// Exporter self-metrics
	driverPoolGauge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "neo4j_exporter_driver_pool_active",
		Help: "Number of cached active database connection drivers",
	}, func() float64 {
		return float64(pool.Count())
	})
	reg.MustRegister(driverPoolGauge)

	// HTTP handlers
	mux := http.NewServeMux()

	// /metrics — standalone mode
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))

	// /scrape?target=... — proxy mode
	mux.HandleFunc("/scrape", func(w http.ResponseWriter, r *http.Request) {
		target := r.URL.Query().Get("target")
		if target == "" {
			http.Error(w, "missing 'target' query parameter", http.StatusBadRequest)
			return
		}

		scrapeReg := prometheus.NewRegistry()
		scrapeStart := time.Now()

		driver, err := pool.Get(r.Context(), target, cfg.Neo4jUser, cfg.Neo4jPassword)
		if err != nil {
			slog.Warn("proxy: failed to get driver", "target", target, "err", err)
			http.Error(w, fmt.Sprintf("driver error: %v", err), http.StatusServiceUnavailable)
			return
		}

		coll := collector.New(target, driver)
		scrapeReg.MustRegister(coll)

		promhttp.HandlerFor(scrapeReg, promhttp.HandlerOpts{
			ErrorHandling: promhttp.ContinueOnError,
		}).ServeHTTP(w, r)

		slog.Info("scrape complete", "target", target, "duration", time.Since(scrapeStart))
	})

	// /sd — service discovery
	mux.HandleFunc("/sd", func(w http.ResponseWriter, r *http.Request) {
		if cfg.SDPrimaryURI == "" {
			http.Error(w, "sd.primary-uri not configured", http.StatusServiceUnavailable)
			return
		}

		driver, err := pool.Get(r.Context(), cfg.SDPrimaryURI, cfg.Neo4jUser, cfg.Neo4jPassword)
		if err != nil {
			slog.Warn("sd: failed to get driver", "err", err)
			http.Error(w, fmt.Sprintf("driver error: %v", err), http.StatusServiceUnavailable)
			return
		}

		targets, err := discovery.Discover(r.Context(), driver, cfg.ListenAddress)
		if err != nil {
			slog.Warn("sd: discovery failed", "err", err)
			http.Error(w, fmt.Sprintf("discovery error: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(targets); err != nil {
			slog.Warn("sd: failed to encode response", "err", err)
		}
	})

	// Health endpoints
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if standaloneDriver == nil {
			http.Error(w, "no driver", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := standaloneDriver.VerifyConnectivity(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	// Start server
	addr := cfg.ListenAddress
	slog.Info("listening", "address", addr)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()

	wg.Wait()
}
