// Package driverpool provides a simple in-memory cache of Neo4j driver instances keyed by URI.
package driverpool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
)

const (
	idleTimeout     = 5 * time.Minute
	cleanupInterval = 1 * time.Minute
)

// Pool manages cached Neo4j driver instances keyed by URI.
type Pool struct {
	mu      sync.Mutex
	drivers map[string]*poolEntry
	quit    chan struct{}
}

type poolEntry struct {
	driver   neo4j.DriverWithContext
	lastUsed time.Time
	user     string
	password string
}

// New creates a Pool and starts the background reaper goroutine.
func New() *Pool {
	p := &Pool{
		drivers: make(map[string]*poolEntry),
		quit:    make(chan struct{}),
	}
	go p.reaper()
	return p
}

// Get returns a cached driver for the given URI, or creates a new one.
func (p *Pool) Get(ctx context.Context, uri, user, password string) (neo4j.DriverWithContext, error) {
	p.mu.Lock()
	entry, ok := p.drivers[uri]
	if ok {
		entry.lastUsed = time.Now()
		p.mu.Unlock()
		return entry.driver, nil
	}
	p.mu.Unlock()

	// Build driver outside the lock
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, password, ""), func(c *config.Config) {
		c.MaxConnectionPoolSize = 5
		c.ConnectionAcquisitionTimeout = 5 * time.Second
	})
	if err != nil {
		return nil, fmt.Errorf("creating driver for %s: %w", uri, err)
	}

	if err := driver.VerifyConnectivity(ctx); err != nil {
		_ = driver.Close(ctx)
		return nil, fmt.Errorf("verifying connectivity to %s: %w", uri, err)
	}

	p.mu.Lock()
	// Double-check: another goroutine may have inserted while we were building
	if existing, ok := p.drivers[uri]; ok {
		p.mu.Unlock()
		_ = driver.Close(ctx)
		existing.lastUsed = time.Now()
		return existing.driver, nil
	}
	p.drivers[uri] = &poolEntry{
		driver:   driver,
		lastUsed: time.Now(),
		user:     user,
		password: password,
	}
	p.mu.Unlock()

	slog.Info("driver cached", "uri", uri)
	return driver, nil
}

// Count returns the number of cached drivers.
func (p *Pool) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.drivers)
}

// Close shuts down all cached drivers and stops the reaper.
func (p *Pool) Close() {
	close(p.quit)
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for uri, entry := range p.drivers {
		if err := entry.driver.Close(ctx); err != nil {
			slog.Warn("error closing driver", "uri", uri, "err", err)
		}
	}
	p.drivers = nil
}

// reaper periodically removes idle drivers.
func (p *Pool) reaper() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.quit:
			return
		case <-ticker.C:
			p.mu.Lock()
			now := time.Now()
			for uri, entry := range p.drivers {
				if now.Sub(entry.lastUsed) > idleTimeout {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if err := entry.driver.Close(ctx); err != nil {
						slog.Warn("error closing idle driver", "uri", uri, "err", err)
					}
					cancel()
					delete(p.drivers, uri)
					slog.Info("driver evicted from pool", "uri", uri)
				}
			}
			p.mu.Unlock()
		}
	}
}
