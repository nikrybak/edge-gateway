// Package health runs active HTTP health checks against upstreams.
package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// ErrUnhealthy wraps a failed probe caused by an unexpected status code.
var ErrUnhealthy = errors.New("unhealthy status")

// Target is something whose health can be probed and recorded.
type Target interface {
	URL() *url.URL
	SetHealthy(ok bool)
	String() string
}

// Config controls probing. Thresholds add hysteresis so a single dropped probe does
// not flap an upstream out of rotation.
type Config struct {
	Path               string
	Interval           time.Duration
	Timeout            time.Duration
	HealthyThreshold   int
	UnhealthyThreshold int
}

// Checker probes targets on a fixed interval.
type Checker struct {
	cfg    Config
	client *http.Client
	logger *slog.Logger
}

// New returns a Checker. The client should not follow redirects to other hosts and
// should have no global timeout; Config.Timeout bounds each probe.
func New(cfg Config, client *http.Client, logger *slog.Logger) *Checker {
	return &Checker{cfg: cfg, client: client, logger: logger}
}

// Run probes every target until ctx is done and returns once all probe loops exit.
func (c *Checker) Run(ctx context.Context, targets []Target) {
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Go(func() { c.loop(ctx, t) })
	}
	wg.Wait()
}

func (c *Checker) loop(ctx context.Context, t Target) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	healthy := true
	var streak int
	for {
		err := c.Probe(ctx, t)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err == nil && healthy, err != nil && !healthy:
			streak = 0
		default:
			streak++
		}
		if healthy && streak >= c.cfg.UnhealthyThreshold {
			healthy, streak = false, 0
			t.SetHealthy(false)
			c.logger.Warn("upstream marked unhealthy", "upstream", t.String(), "error", err)
		} else if !healthy && streak >= c.cfg.HealthyThreshold {
			healthy, streak = true, 0
			t.SetHealthy(true)
			c.logger.Info("upstream marked healthy", "upstream", t.String())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Probe performs one health request. Any 2xx or 3xx response counts as healthy.
func (c *Checker) Probe(ctx context.Context, t Target) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	u := *t.URL()
	u.Path = joinPath(u.Path, c.cfg.Path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("build probe: %w", err)
	}
	req.Header.Set("User-Agent", "edge-gateway-health/1")
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("probe %s: %w", t, err)
	}
	// Drain a bounded amount so the connection can be reused for the next probe.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	_ = resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("probe %s: %w %d", t, ErrUnhealthy, resp.StatusCode)
	}
	return nil
}

func joinPath(base, p string) string {
	if base == "" || base == "/" {
		return p
	}
	if base[len(base)-1] == '/' {
		base = base[:len(base)-1]
	}
	return base + p
}
