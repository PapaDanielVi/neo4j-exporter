// Package config handles command-line flags and environment variables for the neo4j-exporter.
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
)

// Config holds all CLI flags and derived configuration.
type Config struct {
	// Listen address for the exporter HTTP server
	ListenAddress string
	// Neo4j connection
	Neo4jURI          string
	Neo4jUser         string
	Neo4jPassword     string
	Neo4jPasswordFile string
	// Service discovery
	SDPrimaryURI string
	// Custom metrics
	CustomQueriesFile string
	// Logging
	LogJSON bool
}

func envOrDefault(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func envOrDefaultBool(key string, defaultVal bool) bool {
	if val, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return defaultVal
}

// Parse reads flags and environment variables, returns a populated Config.
func Parse(args []string) (*Config, error) {
	cfg := &Config{}

	fs := flag.NewFlagSet("neo4j-exporter", flag.ContinueOnError)

	fs.StringVar(&cfg.ListenAddress, "web.listen-address",
		envOrDefault("NEO4J_EXPORTER_LISTEN_ADDRESS", "9121"),
		"Address to listen on for web interface.")

	fs.StringVar(&cfg.Neo4jURI, "neo4j.uri",
		envOrDefault("NEO4J_URI", "bolt://localhost:7687"),
		"Neo4j bolt URI for standalone mode (e.g. bolt://localhost:7687).")

	fs.StringVar(&cfg.Neo4jUser, "neo4j.user",
		envOrDefault("NEO4J_USER", "neo4j"),
		"Neo4j username.")

	fs.StringVar(&cfg.Neo4jPassword, "neo4j.password",
		envOrDefault("NEO4J_PASSWORD", ""),
		"Neo4j password (prefer --neo4j.password-file).")

	fs.StringVar(&cfg.Neo4jPasswordFile, "neo4j.password-file",
		envOrDefault("NEO4J_PASSWORD_FILE", ""),
		"Path to file containing the Neo4j password.")

	fs.StringVar(&cfg.SDPrimaryURI, "sd.primary-uri",
		envOrDefault("NEO4J_SD_PRIMARY_URI", ""),
		"Primary Neo4j URI for service discovery (/sd endpoint).")

	fs.StringVar(&cfg.CustomQueriesFile, "custom-queries-file",
		envOrDefault("NEO4J_EXPORTER_CUSTOM_QUERIES", "custom_queries.yaml"),
		"Path to YAML custom queries configuration.")

	fs.BoolVar(&cfg.LogJSON, "log.json",
		envOrDefaultBool("NEO4J_EXPORTER_LOG_JSON", false),
		"Output JSON logs instead of text.")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, err
		}
		return nil, fmt.Errorf("parsing flags: %w", err)
	}

	// Resolve password: prefer file over flag/env
	if cfg.Neo4jPasswordFile != "" {
		pw, err := os.ReadFile(cfg.Neo4jPasswordFile)
		if err != nil {
			return nil, fmt.Errorf("reading password file %s: %w", cfg.Neo4jPasswordFile, err)
		}
		cfg.Neo4jPassword = string(pw)
	}

	return cfg, nil
}
