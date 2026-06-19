// Package config handles command-line flags and environment variables for the neo4j-exporter.
package config

import (
	"fmt"
	"os"

	"github.com/alecthomas/kingpin/v2"
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

// Parse reads flags and environment variables, returns a populated Config.
func Parse(args []string) (*Config, error) {
	cfg := &Config{}

	app := kingpin.New("neo4j-exporter", "Prometheus exporter for Neo4j databases.")

	app.Flag("web.listen-address", "Address to listen on for web interface.").
		Default("9121").Envar("NEO4J_EXPORTER_LISTEN_ADDRESS").StringVar(&cfg.ListenAddress)

	app.Flag("neo4j.uri", "Neo4j bolt URI for standalone mode (e.g. bolt://localhost:7687).").
		Default("bolt://localhost:7687").Envar("NEO4J_URI").StringVar(&cfg.Neo4jURI)

	app.Flag("neo4j.user", "Neo4j username.").
		Default("neo4j").Envar("NEO4J_USER").StringVar(&cfg.Neo4jUser)

	app.Flag("neo4j.password", "Neo4j password (prefer --neo4j.password-file).").
		Envar("NEO4J_PASSWORD").StringVar(&cfg.Neo4jPassword)

	app.Flag("neo4j.password-file", "Path to file containing the Neo4j password.").
		Envar("NEO4J_PASSWORD_FILE").StringVar(&cfg.Neo4jPasswordFile)

	app.Flag("sd.primary-uri", "Primary Neo4j URI for service discovery (/sd endpoint).").
		Envar("NEO4J_SD_PRIMARY_URI").StringVar(&cfg.SDPrimaryURI)

	app.Flag("custom-queries-file", "Path to YAML custom queries configuration.").
		Default("custom_queries.yaml").Envar("NEO4J_EXPORTER_CUSTOM_QUERIES").StringVar(&cfg.CustomQueriesFile)

	app.Flag("log.json", "Output JSON logs instead of text.").
		Default("false").BoolVar(&cfg.LogJSON)

	_, err := app.Parse(args)
	if err != nil {
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
