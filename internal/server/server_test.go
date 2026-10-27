package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nikrybak/edge-gateway/internal/config"
)

// upstream is a fake backend whose behaviour tests can change while it runs.
type upstream struct {
	*httptest.Server
	name   string
	hits   atomic.Int32
	status atomic.Int32
	delay  atomic.Int64
	mu     sync.Mutex
	paths  []string
	reqIDs []string
}

func newUpstream(t *testing.T, name string) *upstream {
	t.Helper()
	u := &upstream{name: name}
	u.status.Store(http.StatusOK)
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(int(u.status.Load()))
			return
		}
		u.hits.Add(1)
		u.mu.Lock()
		u.paths = append(u.paths, r.URL.Path)
		u.reqIDs = append(u.reqIDs, r.Header.Get("X-Request-ID"))
		u.mu.Unlock()
		if d := time.Duration(u.delay.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("X-Upstream", u.name)
		w.WriteHeader(int(u.status.Load()))
		_, _ = io.WriteString(w, u.name)
	}))
	t.Cleanup(u.Close)
	return u
}

func deadURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return "http://" + addr
}

func mustConfig(t *testing.T, format string, args ...any) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(fmt.Sprintf(format, args...)))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

type harness struct {
	gw     *Gateway
	public *httptest.Server
	admin  *httptest.Server
}

func newHarness(t *testing.T, cfg *config.Config) *harness {
	t.Helper()
	gw, err := New(cfg, Options{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &harness{gw: gw, public: httptest.NewServer(gw.Handler()), admin: httptest.NewServer(gw.AdminHandler())}
	t.Cleanup(func() { h.public.Close(); h.admin.Close(); gw.Close() })
	return h
}

type result struct {
	status int
	body   string
	header http.Header
}

func (h *harness) do(t *testing.T, method, path string, hdr ...string) result {
	t.Helper()
	req, err := http.NewRequest(method, h.public.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := h.public.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return result{status: resp.StatusCode, body: string(body), header: resp.Header}
}

func TestRoutingRewriteAndRequestID(t *testing.T) {
	api := newUpstream(t, "api")
	web := newUpstream(t, "web")
	h := newHarness(t, mustConfig(t, `
server: {listen: ":0", admin_listen: ":1"}
routes:
  - {name: api, path_prefix: /api/, strip_prefix: true, add_prefix: /v1, upstreams: [%q]}
  - {name: web, path_prefix: /, upstreams: [%q]}
`, api.URL, web.URL))

	tests := []struct {
		path, wantBody, wantPath string
		up                       *upstream
	}{
		{"/api/users/7", "api", "/v1/users/7", api},
		{"/index.html", "web", "/index.html", web},
	}
	for _, tt := range tests {
		r := h.do(t, http.MethodGet, tt.path, "X-Request-ID", "req-"+tt.wantBody)
		if r.status != http.StatusOK || r.body != tt.wantBody {
			t.Fatalf("GET %s = %d %q", tt.path, r.status, r.body)
		}
		if got := r.header.Get("X-Request-ID"); got != "req-"+tt.wantBody {
			t.Errorf("response request id = %q", got)
		}
		tt.up.mu.Lock()
		gotPath, gotID := tt.up.paths[len(tt.up.paths)-1], tt.up.reqIDs[len(tt.up.reqIDs)-1]
		tt.up.mu.Unlock()
		if gotPath != tt.wantPath || gotID != "req-"+tt.wantBody {
			t.Errorf("upstream saw path %q id %q, want %q %q", gotPath, gotID, tt.wantPath, "req-"+tt.wantBody)
		}
	}
}

func TestNoRouteIs404(t *testing.T) {
	up := newUpstream(t, "a")
	h := newHarness(t, mustConfig(t, `routes: [{name: a, host: only.example.com, upstreams: [%q]}]`, up.URL))
	r := h.do(t, http.MethodGet, "/")
	if r.status != http.StatusNotFound || !strings.Contains(r.body, "request_id") {
		t.Fatalf("got %d %q, want 404 with request id", r.status, r.body)
	}
}

func TestFailoverToHealthyUpstream(t *testing.T) {
	alive := newUpstream(t, "alive")
	h := newHarness(t, mustConfig(t, `
routes:
  - name: api
    upstreams: [%q, %q]
    retry: {attempts: 2, base_backoff: 1ms, max_backoff: 2ms}
`, deadURL(t), alive.URL))

	for i := range 10 {
		if r := h.do(t, http.MethodGet, "/"); r.status != http.StatusOK || r.body != "alive" {
			t.Fatalf("request %d: %d %q", i, r.status, r.body)
		}
	}
}

func TestPassiveEjectionStopsTrafficToDeadUpstream(t *testing.T) {
	alive := newUpstream(t, "alive")
	dead := deadURL(t)
	h := newHarness(t, mustConfig(t, `
routes:
  - name: api
    upstreams: [%q, %q]
    passive: {max_failures: 2, eject_duration: 1m}
    breaker: {failure_threshold: 100}
    retry: {attempts: 2, base_backoff: 1ms, max_backoff: 2ms}
`, dead, alive.URL))

	for range 10 {
		h.do(t, http.MethodGet, "/")
	}
	metrics := h.do2(t, h.admin.URL+"/metrics")
	errLine := fmt.Sprintf(`edge_upstream_attempts_total{route="api",upstream=%q,outcome="error"} 2`, strings.TrimPrefix(dead, "http://"))
	if !strings.Contains(metrics, errLine) {
		t.Fatalf("dead upstream should get exactly 2 failed attempts before ejection; metrics:\n%s", metrics)
	}
}

func TestRetryOnlyIdempotentMethods(t *testing.T) {
	tests := []struct {
		method   string
		wantHits int32
	}{
		{http.MethodGet, 3},
		{http.MethodDelete, 3},
		{http.MethodPost, 1},
		{http.MethodPatch, 1},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			up := newUpstream(t, "flaky")
			up.status.Store(http.StatusServiceUnavailable)
			h := newHarness(t, mustConfig(t, `
routes:
  - name: api
    upstreams: [%q]
    breaker: {failure_threshold: 100}
    passive: {max_failures: 100}
    retry: {attempts: 3, base_backoff: 1ms, max_backoff: 2ms}
`, up.URL))
			r := h.do(t, tt.method, "/")
			if r.status != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want upstream's 503 passed through", r.status)
			}
			if got := up.hits.Load(); got != tt.wantHits {
				t.Fatalf("upstream hits = %d, want %d", got, tt.wantHits)
			}
		})
	}
}

func TestBreakerOpensAndRecovers(t *testing.T) {
	up := newUpstream(t, "svc")
	up.status.Store(http.StatusInternalServerError)
	h := newHarness(t, mustConfig(t, `
routes:
  - name: api
    upstreams: [%q]
    passive: {max_failures: 100}
    breaker: {failure_threshold: 3, open_duration: 150ms}
`, up.URL))

	for range 3 {
		if r := h.do(t, http.MethodGet, "/"); r.status != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500 from upstream", r.status)
		}
	}
	if r := h.do(t, http.MethodGet, "/"); r.status != http.StatusServiceUnavailable {
		t.Fatalf("with open breaker: status = %d, want 503", r.status)
	}
	if got := up.hits.Load(); got != 3 {
		t.Fatalf("open breaker let traffic through: hits = %d", got)
	}

	up.status.Store(http.StatusOK)
	time.Sleep(200 * time.Millisecond)
	for i := range 5 {
		if r := h.do(t, http.MethodGet, "/"); r.status != http.StatusOK {
			t.Fatalf("after recovery request %d: status = %d", i, r.status)
		}
	}
	if m := h.do2(t, h.admin.URL+"/metrics"); !strings.Contains(m, `edge_upstream_breaker_state{route="api",upstream=`) ||
		!strings.Contains(m, `"} 0`) {
		t.Fatalf("breaker gauge not closed:\n%s", m)
	}
}

func TestRateLimit(t *testing.T) {
	up := newUpstream(t, "svc")
	h := newHarness(t, mustConfig(t, `
routes:
  - name: api
    upstreams: [%q]
    rate_limit: {requests_per_second: 0.5, burst: 2, key_header: X-API-Key}
`, up.URL))

	tests := []struct {
		key  string
		want int
	}{
		{"alice", http.StatusOK},
		{"alice", http.StatusOK},
		{"alice", http.StatusTooManyRequests},
		{"bob", http.StatusOK},
		{"", http.StatusOK}, // falls back to the client IP bucket
	}
	for i, tt := range tests {
		var hdr []string
		if tt.key != "" {
			hdr = []string{"X-API-Key", tt.key}
		}
		r := h.do(t, http.MethodGet, "/", hdr...)
		if r.status != tt.want {
			t.Fatalf("request %d (%q): status %d, want %d", i, tt.key, r.status, tt.want)
		}
		if r.status == http.StatusTooManyRequests && r.header.Get("Retry-After") != "2" {
			t.Fatalf("Retry-After = %q, want 2", r.header.Get("Retry-After"))
		}
	}
	if got := up.hits.Load(); got != 4 {
		t.Fatalf("upstream hits = %d, want 4", got)
	}
	if m := h.do2(t, h.admin.URL+"/metrics"); !strings.Contains(m, `edge_ratelimited_total{route="api"} 1`) {
		t.Fatalf("rate limit metric missing:\n%s", m)
	}
}

func TestRouteTimeoutIs504(t *testing.T) {
	up := newUpstream(t, "slow")
	up.delay.Store(int64(time.Second))
	h := newHarness(t, mustConfig(t, `routes: [{name: api, timeout: 50ms, upstreams: [%q]}]`, up.URL))
	start := time.Now()
	if r := h.do(t, http.MethodGet, "/"); r.status != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", r.status)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("route deadline not enforced")
	}
}

func TestActiveHealthCheckRemovesAndRestoresUpstream(t *testing.T) {
	a, b := newUpstream(t, "a"), newUpstream(t, "b")
	h := newHarness(t, mustConfig(t, `
routes:
  - name: api
    upstreams: [%q, %q]
    health_check: {path: /health, interval: 50ms, timeout: 40ms, unhealthy_threshold: 2, healthy_threshold: 1}
`, a.URL, b.URL))
	healthy := func(n int) func() bool {
		return func() bool {
			return strings.Contains(h.do2(t, h.admin.URL+"/readyz"), fmt.Sprintf(`"healthy_upstreams":%d`, n))
		}
	}

	b.status.Store(http.StatusServiceUnavailable)
	eventually(t, healthy(1))
	for range 6 {
		if r := h.do(t, http.MethodGet, "/"); r.status != http.StatusOK || r.body != "a" {
			t.Fatalf("with b unhealthy: %d %q, want 200 a", r.status, r.body)
		}
	}
	b.status.Store(http.StatusOK)
	eventually(t, healthy(2))
}

func TestHotReloadUnderLoad(t *testing.T) {
	oldUp, newUp := newUpstream(t, "old"), newUpstream(t, "new")
	cfgFor := func(u string) *config.Config {
		return mustConfig(t, `routes: [{name: api, upstreams: [%q], rate_limit: {requests_per_second: 1000000, burst: 1000000}}]`, u)
	}
	h := newHarness(t, cfgFor(oldUp.URL))

	ctx, cancel := context.WithCancel(context.Background())
	var (
		wg       sync.WaitGroup
		failures atomic.Int32
		sawNew   atomic.Bool
	)
	for range 8 {
		wg.Go(func() {
			client := h.public.Client()
			for ctx.Err() == nil {
				resp, err := client.Get(h.public.URL + "/")
				if err != nil {
					failures.Add(1)
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					failures.Add(1)
				}
				if string(body) == "new" {
					sawNew.Store(true)
				}
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	for i := range 20 {
		next := oldUp.URL
		if i%2 == 0 {
			next = newUp.URL
		}
		if err := h.gw.Reload(cfgFor(next)); err != nil {
			t.Fatalf("Reload: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if n := failures.Load(); n != 0 {
		t.Fatalf("%d requests failed during reloads", n)
	}
	if !sawNew.Load() {
		t.Fatal("no request reached the new upstream")
	}
}

func TestReloadRejectsListenerChange(t *testing.T) {
	up := newUpstream(t, "a")
	h := newHarness(t, mustConfig(t, `routes: [{name: a, upstreams: [%q]}]`, up.URL))
	err := h.gw.Reload(mustConfig(t, `{server: {listen: ":7777"}, routes: [{name: a, upstreams: [%q]}]}`, up.URL))
	if err == nil || !strings.Contains(err.Error(), ErrRestartRequired.Error()) {
		t.Fatalf("want ErrRestartRequired, got %v", err)
	}
}

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	up := newUpstream(t, "slow")
	up.delay.Store(int64(300 * time.Millisecond))
	cfg := mustConfig(t, `
server: {listen: "127.0.0.1:0", admin_listen: "127.0.0.1:0", drain_delay: 100ms, shutdown_timeout: 5s}
routes: [{name: api, upstreams: [%q]}]
`, up.URL)
	gw, err := New(cfg, Options{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	public, admin, err := Listen(cfg.Server)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- Serve(ctx, gw, cfg.Server, public, admin, slog.New(slog.DiscardHandler)) }()

	inflight := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + public.Addr().String() + "/")
		if err != nil {
			inflight <- result{status: -1, body: err.Error()}
			return
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		inflight <- result{status: resp.StatusCode, body: string(body)}
	}()
	eventually(t, func() bool { return up.hits.Load() == 1 })
	cancel()

	eventually(t, func() bool {
		resp, err := http.Get("http://" + admin.Addr().String() + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusServiceUnavailable
	})
	if r := <-inflight; r.status != http.StatusOK || r.body != "slow" {
		t.Fatalf("in-flight request: %d %q, want 200 slow", r.status, r.body)
	}
	if err := <-served; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

func TestAdminEndpoints(t *testing.T) {
	up := newUpstream(t, "a")
	h := newHarness(t, mustConfig(t, `routes: [{name: a, upstreams: [%q]}]`, up.URL))
	h.do(t, http.MethodGet, "/")

	if got := h.do2(t, h.admin.URL+"/healthz"); got != "ok\n" {
		t.Fatalf("/healthz = %q", got)
	}
	if got := h.do2(t, h.admin.URL+"/readyz"); !strings.Contains(got, `"ready":true`) {
		t.Fatalf("/readyz = %q", got)
	}
	m := h.do2(t, h.admin.URL+"/metrics")
	for _, want := range []string{
		`edge_requests_total{route="a",method="GET",code="200"} 1`,
		`edge_request_duration_seconds_count{route="a"} 1`,
		`edge_upstream_available{route="a",upstream=`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func (h *harness) do2(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
