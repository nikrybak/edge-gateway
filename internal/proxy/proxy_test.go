package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nikrybak/edge-gateway/internal/balancer"
)

func TestRetryable(t *testing.T) {
	withGetBody := httptest.NewRequest(http.MethodPut, "/", strings.NewReader("x"))
	withGetBody.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("x")), nil }
	streamingPut := httptest.NewRequest(http.MethodPut, "/", strings.NewReader("x"))
	streamingPut.GetBody = nil

	tests := []struct {
		name string
		req  *http.Request
		want bool
	}{
		{"GET", httptest.NewRequest(http.MethodGet, "/", nil), true},
		{"HEAD", httptest.NewRequest(http.MethodHead, "/", nil), true},
		{"DELETE", httptest.NewRequest(http.MethodDelete, "/", nil), true},
		{"POST", httptest.NewRequest(http.MethodPost, "/", nil), false},
		{"PATCH", httptest.NewRequest(http.MethodPatch, "/", nil), false},
		{"PUT with replayable body", withGetBody, true},
		{"PUT with streaming body", streamingPut, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Retryable(tt.req); got != tt.want {
				t.Fatalf("Retryable = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	base, maxDelay := 100*time.Millisecond, time.Second
	tests := []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 200 * time.Millisecond},
		{3, 200 * time.Millisecond, 400 * time.Millisecond},
		{5, 500 * time.Millisecond, time.Second},  // capped
		{80, 500 * time.Millisecond, time.Second}, // shift overflow stays capped
	}
	for _, tt := range tests {
		lo := Backoff(tt.attempt, base, maxDelay, func(int64) int64 { return 0 })
		hi := Backoff(tt.attempt, base, maxDelay, func(n int64) int64 { return n - 1 })
		if lo != tt.min || hi != tt.max {
			t.Errorf("attempt %d: range [%s, %s], want [%s, %s]", tt.attempt, lo, hi, tt.min, tt.max)
		}
	}
}

func TestJoinURLPath(t *testing.T) {
	tests := []struct{ target, in, want string }{
		{"http://u", "/a/b", "/a/b"},
		{"http://u/", "/a", "/a"},
		{"http://u/base", "/a", "/base/a"},
		{"http://u/base/", "/a", "/base/a"},
	}
	for _, tt := range tests {
		target, _ := url.Parse(tt.target)
		in, _ := url.Parse(tt.in)
		if got, _ := joinURLPath(target, in); got != tt.want {
			t.Errorf("join(%q, %q) = %q, want %q", tt.target, tt.in, got, tt.want)
		}
	}
}

type countingObserver struct{ outcomes map[string]int }

func (c *countingObserver) ObserveAttempt(_, _, outcome string) { c.outcomes[outcome]++ }

func TestProxyRetriesOnAnotherUpstream(t *testing.T) {
	var badHits, goodHits atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		badHits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		_, _ = io.WriteString(w, r.URL.Path)
	}))
	defer good.Close()

	obs := &countingObserver{outcomes: map[string]int{}}
	p := New(Options{
		Route:     "test",
		Balancer:  pool(t, bad.URL, good.URL+"/base"),
		Transport: http.DefaultTransport,
		Rewrite:   func(p string) string { return strings.TrimPrefix(p, "/api") },
		Retry:     RetryPolicy{Attempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
		Observer:  obs,
	})
	front := httptest.NewServer(p)
	defer front.Close()

	resp, err := http.Get(front.URL + "/api/items")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "/base/items" {
		t.Fatalf("got %d %q, want 200 /base/items", resp.StatusCode, body)
	}
	if badHits.Load() != 1 || goodHits.Load() != 1 {
		t.Fatalf("hits bad=%d good=%d, want 1 and 1", badHits.Load(), goodHits.Load())
	}
	if obs.outcomes["status_5xx"] != 1 || obs.outcomes["ok"] != 1 {
		t.Fatalf("outcomes = %v", obs.outcomes)
	}
}

func TestProxyPerTryTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer slow.Close()

	p := New(Options{
		Balancer:  pool(t, slow.URL),
		Transport: http.DefaultTransport,
		Retry:     RetryPolicy{Attempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond, PerTryTimeout: 20 * time.Millisecond},
	})
	rec := httptest.NewRecorder()
	start := time.Now()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("took %s, per-try timeout not applied", elapsed)
	}
}

func pool(t *testing.T, raws ...string) *balancer.Pool {
	t.Helper()
	ups := make([]*balancer.Upstream, len(raws))
	for i, raw := range raws {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		ups[i] = balancer.NewUpstream(u, balancer.UpstreamOptions{BreakerThreshold: 5, BreakerOpenFor: time.Second})
	}
	return balancer.NewPool(ups)
}
