package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/PapaDanielVi/neo4j-exporter/pkg/collector"
	"github.com/PapaDanielVi/neo4j-exporter/pkg/config"
	"github.com/PapaDanielVi/neo4j-exporter/pkg/discovery"
	"github.com/PapaDanielVi/neo4j-exporter/pkg/driverpool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "Error parsing config: %v\n", err)
		os.Exit(1)
	}

	setupLogger(cfg.LogJSON)

	slog.Info("starting neo4j-exporter",
		"version", version, "commit", commit, "buildDate", buildDate,
		"listen", cfg.ListenAddress, "standalone_uri", cfg.Neo4jURI,
	)

	pool := driverpool.New()
	defer pool.Close()

	reg := prometheus.NewRegistry()
	standaloneDriver := setupStandaloneCollector(reg, pool, cfg)
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "neo4j_exporter_driver_pool_active",
		Help: "Number of cached active database connection drivers",
	}, func() float64 {
		return float64(pool.Count())
	}))

	mux := setupHandlers(reg, pool, cfg, standaloneDriver)

	serve(cfg.ListenAddress, mux)
}

func setupLogger(json bool) {
	var handler slog.Handler
	if json {
		handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	} else {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	}
	slog.SetDefault(slog.New(handler))
}

// registerCustomQueries loads the YAML custom queries and, if any are defined,
// registers a collector that executes them against the standalone driver.
func registerCustomQueries(reg *prometheus.Registry, driver neo4j.DriverWithContext, file string) {
	customQueries, err := collector.LoadCustomQueries(file)
	if err != nil {
		slog.Warn("custom queries load failed", "err", err)
		return
	}
	if len(customQueries.Queries) == 0 {
		return
	}
	reg.MustRegister(collector.NewCustomCollector(customQueries, driver))
	slog.Info("registered custom queries", "count", len(customQueries.Queries))
}

func setupStandaloneCollector(reg *prometheus.Registry, pool *driverpool.Pool, cfg *config.Config) neo4j.DriverWithContext {
	standaloneDriver, err := pool.Get(context.Background(), cfg.Neo4jURI, cfg.Neo4jUser, cfg.Neo4jPassword)
	if err != nil {
		slog.Warn("standalone driver init failed (proxy mode still available)", "err", err)
		return nil
	}
	reg.MustRegister(collector.New(cfg.Neo4jURI, standaloneDriver))
	registerCustomQueries(reg, standaloneDriver, cfg.CustomQueriesFile)
	return standaloneDriver
}

func setupHandlers(reg *prometheus.Registry, pool *driverpool.Pool, cfg *config.Config, standaloneDriver neo4j.DriverWithContext) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))

	mux.HandleFunc("/scrape", func(w http.ResponseWriter, r *http.Request) {
		handleScrape(w, r, pool, cfg)
	})

	mux.HandleFunc("/sd", func(w http.ResponseWriter, r *http.Request) {
		handleDiscovery(w, r, pool, cfg)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		handleReadiness(w, r, standaloneDriver)
	})

	return mux
}

func handleScrape(w http.ResponseWriter, r *http.Request, pool *driverpool.Pool, cfg *config.Config) {
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

	scrapeReg.MustRegister(collector.New(target, driver))

	promhttp.HandlerFor(scrapeReg, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}).ServeHTTP(w, r)

	slog.Info("scrape complete", "target", target, "duration", time.Since(scrapeStart))
}

func handleDiscovery(w http.ResponseWriter, r *http.Request, pool *driverpool.Pool, cfg *config.Config) {
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
}

func handleReadiness(w http.ResponseWriter, r *http.Request, driver neo4j.DriverWithContext) {
	if driver == nil {
		http.Error(w, "no driver", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := driver.VerifyConnectivity(ctx); err != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready"))
}

func serve(addr string, handler http.Handler) {
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	slog.Info("listening", "address", addr)

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http server error", "err", err)
		os.Exit(1)
	}
}
