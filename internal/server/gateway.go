// Package server wires routing, rate limiting, proxying and health checking into a
// hot-reloadable gateway, and runs its HTTP listeners.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nikrybak/edge-gateway/internal/balancer"
	"github.com/nikrybak/edge-gateway/internal/config"
	"github.com/nikrybak/edge-gateway/internal/health"
	"github.com/nikrybak/edge-gateway/internal/observability"
	"github.com/nikrybak/edge-gateway/internal/proxy"
	"github.com/nikrybak/edge-gateway/internal/ratelimit"
	"github.com/nikrybak/edge-gateway/internal/router"
)

// ErrRestartRequired is returned by Reload when the new config changes settings that
// cannot be applied to running listeners.
var ErrRestartRequired = errors.New("change requires a restart")

// Options are the long-lived dependencies of a Gateway. They survive config reloads.
type Options struct {
	Logger   *slog.Logger
	Registry *observability.Registry
	// Transport carries proxied requests. Sharing one across reloads keeps upstream
	// keep-alive connections warm. Defaults to NewTransport().
	Transport http.RoundTripper
	// HealthClient performs active health checks. Defaults to a client that does not
	// follow redirects.
	HealthClient *http.Client
}

// Gateway routes requests to upstream pools according to the current config.
//
// The routing table, pools, limiters and health checkers form an immutable snapshot
// held in an atomic pointer. A reload builds a complete new snapshot off to the side
// and swaps the pointer: requests already running keep the snapshot they started with,
// new requests see the new one, and no request ever observes a half-applied config.
type Gateway struct {
	opts        Options
	httpMetrics *observability.HTTPMetrics
	attempts    *observability.CounterVec
	limited     *observability.CounterVec
	reloads     *observability.CounterVec

	current  atomic.Pointer[snapshot]
	reloadMu sync.Mutex
	draining atomic.Bool
}

type snapshot struct {
	cfg       *config.Config
	router    *router.Router[*route]
	routes    []*route
	upstreams map[string]*balancer.Upstream
	limiters  map[string]*ratelimit.HTTP
	health    []healthJob
	stop      context.CancelFunc
	stopped   chan struct{}
}

type healthJob struct {
	checker *health.Checker
	targets []health.Target
}

type route struct {
	name    string
	timeout time.Duration
	pool    *balancer.Pool
	handler http.Handler
}

// New builds a Gateway for cfg and starts its health checkers.
func New(cfg *config.Config, opts Options) (*Gateway, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Registry == nil {
		opts.Registry = observability.NewRegistry()
	}
	if opts.Transport == nil {
		opts.Transport = NewTransport()
	}
	if opts.HealthClient == nil {
		opts.HealthClient = &http.Client{
			Transport:     opts.Transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	reg := opts.Registry
	g := &Gateway{
		opts:        opts,
		httpMetrics: observability.NewHTTPMetrics(reg),
		attempts:    reg.NewCounterVec("edge_upstream_attempts_total", "Upstream attempts by outcome.", "route", "upstream", "outcome"),
		limited:     reg.NewCounterVec("edge_ratelimited_total", "Requests rejected by the rate limiter.", "route"),
		reloads:     reg.NewCounterVec("edge_config_reloads_total", "Config reload attempts by result.", "result"),
	}
	reg.NewGaugeFunc("edge_upstream_available", "1 if the upstream can receive traffic.", []string{"route", "upstream"}, g.collectUpstreams(func(u *balancer.Upstream) float64 {
		if u.Available() {
			return 1
		}
		return 0
	}))
	reg.NewGaugeFunc("edge_upstream_breaker_state", "Circuit breaker state: 0 closed, 1 open, 2 half-open.", []string{"route", "upstream"}, g.collectUpstreams(func(u *balancer.Upstream) float64 {
		return float64(u.BreakerState())
	}))

	snap, err := g.build(cfg, nil)
	if err != nil {
		return nil, err
	}
	g.current.Store(snap)
	snap.start()
	return g, nil
}

// Reload atomically replaces the running config. On error the old config stays live.
// Upstreams and rate limiters whose settings did not change are carried over, so a
// reload does not reset breaker, ejection or bucket state.
func (g *Gateway) Reload(cfg *config.Config) error {
	g.reloadMu.Lock()
	defer g.reloadMu.Unlock()

	old := g.current.Load()
	if cfg.Server.Listen != old.cfg.Server.Listen || cfg.Server.AdminListen != old.cfg.Server.AdminListen {
		g.reloads.With("rejected").Inc()
		return fmt.Errorf("listener addresses: %w", ErrRestartRequired)
	}
	snap, err := g.build(cfg, old)
	if err != nil {
		g.reloads.With("error").Inc()
		return err
	}
	g.current.Store(snap)
	old.close()
	snap.start()
	g.reloads.With("success").Inc()
	g.opts.Logger.Info("config reloaded", "routes", len(cfg.Routes))
	return nil
}

// Close stops background health checking.
func (g *Gateway) Close() {
	g.reloadMu.Lock()
	defer g.reloadMu.Unlock()
	g.current.Load().close()
}

// SetDraining makes /readyz report not ready, ahead of a graceful shutdown.
func (g *Gateway) SetDraining() { g.draining.Store(true) }

// Handler returns the public proxy handler.
func (g *Gateway) Handler() http.Handler {
	return observability.Middleware(g.opts.Logger, g.httpMetrics, http.HandlerFunc(g.serveProxy))
}

func (g *Gateway) serveProxy(w http.ResponseWriter, r *http.Request) {
	snap := g.current.Load()
	rt, _, err := snap.router.Match(r.Host, r.URL.Path)
	if err != nil {
		proxy.WriteError(w, r, http.StatusNotFound, "no route")
		return
	}
	observability.Info(r.Context()).Route = rt.name
	ctx, cancel := context.WithTimeout(r.Context(), rt.timeout)
	defer cancel()
	rt.handler.ServeHTTP(w, r.WithContext(ctx))
}

// AdminHandler serves /metrics, /healthz and /readyz.
func (g *Gateway) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", g.opts.Registry.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", g.serveReady)
	return mux
}

func (g *Gateway) serveReady(w http.ResponseWriter, _ *http.Request) {
	type routeStatus struct {
		Healthy int `json:"healthy_upstreams"`
		Total   int `json:"total_upstreams"`
	}
	body := struct {
		Ready  bool                   `json:"ready"`
		Routes map[string]routeStatus `json:"routes"`
	}{Ready: !g.draining.Load(), Routes: map[string]routeStatus{}}

	for _, rt := range g.current.Load().routes {
		body.Routes[rt.name] = routeStatus{Healthy: rt.pool.HealthyCount(), Total: len(rt.pool.Upstreams())}
	}
	w.Header().Set("Content-Type", "application/json")
	if !body.Ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(body)
}

// build constructs a snapshot for cfg, reusing state from prev where settings match.
func (g *Gateway) build(cfg *config.Config, prev *snapshot) (*snapshot, error) {
	snap := &snapshot{
		cfg:       cfg,
		upstreams: map[string]*balancer.Upstream{},
		limiters:  map[string]*ratelimit.HTTP{},
		stopped:   make(chan struct{}),
	}
	var jobs []healthJob
	b := router.NewBuilder[*route]()

	for _, rc := range cfg.Routes {
		ups := make([]*balancer.Upstream, 0, len(rc.Upstreams))
		targets := make([]health.Target, 0, len(rc.Upstreams))
		for _, raw := range rc.Upstreams {
			key := fmt.Sprintf("%s|%s|%v|%v", rc.Name, raw, rc.Passive, rc.Breaker)
			up := prev.upstream(key)
			if up == nil {
				u, err := url.Parse(raw)
				if err != nil {
					return nil, fmt.Errorf("route %s: upstream %q: %w", rc.Name, raw, err)
				}
				up = balancer.NewUpstream(u, balancer.UpstreamOptions{
					MaxFailures:      rc.Passive.MaxFailures,
					EjectFor:         rc.Passive.EjectDuration,
					BreakerThreshold: rc.Breaker.FailureThreshold,
					BreakerOpenFor:   rc.Breaker.OpenDuration,
				})
			}
			snap.upstreams[key] = up
			ups = append(ups, up)
			targets = append(targets, up)
		}
		pool := balancer.NewPool(ups)

		var handler http.Handler = proxy.New(proxy.Options{
			Route:     rc.Name,
			Balancer:  pool,
			Transport: g.opts.Transport,
			Rewrite:   rewriter(rc),
			Retry: proxy.RetryPolicy{
				Attempts:      rc.Retry.Attempts,
				BaseBackoff:   rc.Retry.BaseBackoff,
				MaxBackoff:    rc.Retry.MaxBackoff,
				PerTryTimeout: rc.Retry.PerTryTimeout,
			},
			Logger:   g.opts.Logger,
			Observer: attemptObserver{g.attempts},
		})
		if rl := rc.RateLimit; rl != nil {
			key := fmt.Sprintf("%s|%v", rc.Name, *rl)
			lim := prev.limiter(key)
			if lim == nil {
				lim = ratelimit.NewHTTP(rl.RequestsPerSecond, rl.Burst, rl.KeyHeader, nil, g.onLimited(rc.Name))
			}
			snap.limiters[key] = lim
			handler = lim.Wrap(handler)
		}

		rt := &route{name: rc.Name, timeout: rc.Timeout, pool: pool, handler: handler}
		snap.routes = append(snap.routes, rt)
		b.Add(rc.Host, rc.PathPrefix, rt)

		if hc := rc.HealthCheck; hc.Path != "" {
			jobs = append(jobs, healthJob{
				checker: health.New(health.Config{
					Path: hc.Path, Interval: hc.Interval, Timeout: hc.Timeout,
					HealthyThreshold: hc.HealthyThreshold, UnhealthyThreshold: hc.UnhealthyThreshold,
				}, g.opts.HealthClient, g.opts.Logger.With("route", rc.Name)),
				targets: targets,
			})
		}
	}
	snap.router = b.Build()
	snap.health = jobs
	return snap, nil
}

// start launches the snapshot's health checkers.
func (s *snapshot) start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.stop = cancel
	var wg sync.WaitGroup
	for _, j := range s.health {
		wg.Go(func() { j.checker.Run(ctx, j.targets) })
	}
	go func() { wg.Wait(); close(s.stopped) }()
}

func (s *snapshot) upstream(key string) *balancer.Upstream {
	if s == nil {
		return nil
	}
	return s.upstreams[key]
}

func (s *snapshot) limiter(key string) *ratelimit.HTTP {
	if s == nil {
		return nil
	}
	return s.limiters[key]
}

// close stops the snapshot's health checkers and waits for them. Reload starts the new
// checkers only afterwards, so two checkers never drive a carried-over upstream.
func (s *snapshot) close() {
	s.stop()
	<-s.stopped
}

func rewriter(rc config.Route) func(string) string {
	if !rc.StripPrefix && rc.AddPrefix == "" {
		return nil
	}
	return func(p string) string { return router.Rewrite(p, rc.PathPrefix, rc.StripPrefix, rc.AddPrefix) }
}

func (g *Gateway) onLimited(routeName string) func(*http.Request) {
	c := g.limited.With(routeName)
	return func(*http.Request) { c.Inc() }
}

func (g *Gateway) collectUpstreams(value func(*balancer.Upstream) float64) func(emit func(float64, ...string)) {
	return func(emit func(float64, ...string)) {
		snap := g.current.Load()
		if snap == nil {
			return
		}
		for _, rt := range snap.routes {
			for _, u := range rt.pool.Upstreams() {
				emit(value(u), rt.name, u.String())
			}
		}
	}
}

type attemptObserver struct{ c *observability.CounterVec }

func (o attemptObserver) ObserveAttempt(route, upstream, outcome string) {
	o.c.With(route, upstream, outcome).Inc()
}

// NewTransport returns the upstream transport: bounded dial and TLS handshake times,
// generous idle pools, and no environment proxy, since a gateway must never
// silently route through HTTP_PROXY.
func NewTransport() *http.Transport {
	return &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}
