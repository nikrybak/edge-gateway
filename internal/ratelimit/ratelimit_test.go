package ratelimit

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clock struct{ ns atomic.Int64 }

func (c *clock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *clock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

func TestLimiterBurstAndRefill(t *testing.T) {
	c := &clock{}
	c.Advance(time.Hour)
	l := New(2, 3, c.Now) // 2 tokens/s, burst 3

	steps := []struct {
		advance   time.Duration
		wantOK    bool
		wantRetry time.Duration
	}{
		{0, true, 0},
		{0, true, 0},
		{0, true, 0},
		{0, false, 500 * time.Millisecond},
		{250 * time.Millisecond, false, 250 * time.Millisecond},
		{250 * time.Millisecond, true, 0},
		{0, false, 500 * time.Millisecond},
		{10 * time.Second, true, 0}, // capped at burst, not 20 tokens
		{0, true, 0},
		{0, true, 0},
		{0, false, 500 * time.Millisecond},
	}
	for i, s := range steps {
		c.Advance(s.advance)
		ok, retry := l.Allow("client")
		if ok != s.wantOK || retry != s.wantRetry {
			t.Fatalf("step %d: got (%t, %s), want (%t, %s)", i, ok, retry, s.wantOK, s.wantRetry)
		}
	}
}

func TestLimiterKeysAreIndependent(t *testing.T) {
	l := New(1, 1, (&clock{}).Now)
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("first request for a rejected")
	}
	if ok, _ := l.Allow("a"); ok {
		t.Fatal("second request for a allowed")
	}
	if ok, _ := l.Allow("b"); !ok {
		t.Fatal("request for b rejected")
	}
}

func TestLimiterEvictsFullBuckets(t *testing.T) {
	c := &clock{}
	c.Advance(time.Hour)
	l := New(10, 10, c.Now) // refills fully in 1s
	for i := range 1000 {
		l.Allow(strconv.Itoa(i))
	}
	if got := l.Len(); got != 1000 {
		t.Fatalf("Len = %d, want 1000", got)
	}
	c.Advance(2 * time.Second)
	for i := range 1000 {
		l.Allow("new-" + strconv.Itoa(i))
	}
	if got := l.Len(); got > 1100 {
		t.Fatalf("Len = %d after sweep, stale buckets were not evicted", got)
	}
}

func TestLimiterConcurrentExactBudget(t *testing.T) {
	l := New(1e-9, 1000, nil) // effectively no refill during the test
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 200 {
				if ok, _ := l.Allow("shared"); ok {
					allowed.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if got := allowed.Load(); got != 1000 {
		t.Fatalf("allowed %d requests, want exactly the burst of 1000", got)
	}
}

func TestHTTPMiddleware(t *testing.T) {
	c := &clock{}
	var limited atomic.Int32
	h := NewHTTP(1, 2, "X-Api-Key", c.Now, func(*http.Request) { limited.Add(1) }).
		Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))

	do := func(ip, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = ip + ":5000"
		if key != "" {
			r.Header.Set("X-Api-Key", key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	tests := []struct {
		ip, key    string
		wantStatus int
	}{
		{"10.0.0.1", "", http.StatusNoContent},
		{"10.0.0.1", "", http.StatusNoContent},
		{"10.0.0.1", "", http.StatusTooManyRequests},
		{"10.0.0.1", "k1", http.StatusNoContent}, // key bucket is separate from the IP bucket
		{"10.0.0.2", "k1", http.StatusNoContent}, // same key from another IP shares the bucket
		{"10.0.0.3", "k1", http.StatusTooManyRequests},
		{"10.0.0.9", "10.0.0.4", http.StatusNoContent}, // key namespace cannot touch IP buckets
		{"10.0.0.4", "", http.StatusNoContent},
	}
	for i, tt := range tests {
		w := do(tt.ip, tt.key)
		if w.Code != tt.wantStatus {
			t.Fatalf("request %d (%s, %q): status %d, want %d", i, tt.ip, tt.key, w.Code, tt.wantStatus)
		}
		if w.Code == http.StatusTooManyRequests && w.Header().Get("Retry-After") != "1" {
			t.Errorf("request %d: Retry-After = %q, want 1", i, w.Header().Get("Retry-After"))
		}
	}
	if got := limited.Load(); got != 2 {
		t.Errorf("onLimited called %d times, want 2", got)
	}
}

func BenchmarkLimiterAllow(b *testing.B) {
	for _, keys := range []int{1, 1024, 65536} {
		names := make([]string, keys)
		for i := range names {
			names[i] = fmt.Sprintf("client-%d", i)
		}
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			l := New(1e9, 1<<30, nil)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					l.Allow(names[i%keys])
					i++
				}
			})
		})
	}
}
