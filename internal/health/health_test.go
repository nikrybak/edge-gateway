package health

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

type target struct {
	u       *url.URL
	healthy atomic.Bool
	changes atomic.Int32
}

func (t *target) URL() *url.URL  { return t.u }
func (t *target) String() string { return t.u.Host }
func (t *target) Healthy() bool  { return t.healthy.Load() }
func (t *target) SetHealthy(ok bool) {
	t.healthy.Store(ok)
	t.changes.Add(1)
}

func newTarget(t *testing.T, raw string) *target {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	tg := &target{u: u}
	tg.healthy.Store(true)
	return tg
}

func testChecker() *Checker {
	return New(Config{
		Path: "/health", Interval: 5 * time.Millisecond, Timeout: 50 * time.Millisecond,
		HealthyThreshold: 2, UnhealthyThreshold: 2,
	}, &http.Client{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestProbe(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		wantErr error
	}{
		{"ok", http.StatusOK, nil},
		{"redirect counts as healthy", http.StatusFound, nil},
		{"server error", http.StatusServiceUnavailable, ErrUnhealthy},
		{"not found", http.StatusNotFound, ErrUnhealthy},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/base/health" {
					t.Errorf("probe path = %q", r.URL.Path)
				}
				w.WriteHeader(tt.status)
			}))
			defer srv.Close()
			err := testChecker().Probe(context.Background(), newTarget(t, srv.URL+"/base/"))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Probe err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRunMarksDownAndUp(t *testing.T) {
	var failing atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	tg := newTarget(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { testChecker().Run(ctx, []Target{tg}); close(done) }()
	defer func() { cancel(); <-done }()

	failing.Store(true)
	eventually(t, func() bool { return !tg.healthy.Load() })
	failing.Store(false)
	eventually(t, func() bool { return tg.healthy.Load() })
	if got := tg.changes.Load(); got != 2 {
		t.Errorf("SetHealthy called %d times, want 2 (no flapping)", got)
	}
}

func TestRunStopsOnUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := srv.URL
	srv.Close()

	tg := newTarget(t, addr)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { testChecker().Run(ctx, []Target{tg}); close(done) }()
	eventually(t, func() bool { return !tg.healthy.Load() })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
