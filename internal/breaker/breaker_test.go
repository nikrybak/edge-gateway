package breaker

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestBreaker() (*Breaker, *fakeClock) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	return New(3, 10*time.Second, clk.Now), clk
}

func mustAllow(t *testing.T, b *Breaker) Permit {
	t.Helper()
	p, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow: %v (state %s)", err, b.State())
	}
	return p
}

func TestBreakerTransitions(t *testing.T) {
	tests := []struct {
		name  string
		steps func(t *testing.T, b *Breaker, clk *fakeClock)
		want  State
	}{
		{"stays closed below threshold", func(t *testing.T, b *Breaker, _ *fakeClock) {
			mustAllow(t, b).Failure()
			mustAllow(t, b).Failure()
		}, Closed},
		{"success resets failure streak", func(t *testing.T, b *Breaker, _ *fakeClock) {
			mustAllow(t, b).Failure()
			mustAllow(t, b).Failure()
			mustAllow(t, b).Success()
			mustAllow(t, b).Failure()
		}, Closed},
		{"opens at threshold", func(t *testing.T, b *Breaker, _ *fakeClock) {
			for range 3 {
				mustAllow(t, b).Failure()
			}
		}, Open},
		{"half-open after open duration", func(t *testing.T, b *Breaker, clk *fakeClock) {
			for range 3 {
				mustAllow(t, b).Failure()
			}
			clk.Advance(10 * time.Second)
		}, HalfOpen},
		{"probe success closes", func(t *testing.T, b *Breaker, clk *fakeClock) {
			for range 3 {
				mustAllow(t, b).Failure()
			}
			clk.Advance(10 * time.Second)
			mustAllow(t, b).Success()
		}, Closed},
		{"probe failure reopens", func(t *testing.T, b *Breaker, clk *fakeClock) {
			for range 3 {
				mustAllow(t, b).Failure()
			}
			clk.Advance(10 * time.Second)
			mustAllow(t, b).Failure()
		}, Open},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, clk := newTestBreaker()
			tt.steps(t, b, clk)
			if got := b.State(); got != tt.want {
				t.Fatalf("state = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestBreakerRejectsWhileOpen(t *testing.T) {
	b, clk := newTestBreaker()
	for range 3 {
		mustAllow(t, b).Failure()
	}
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("want ErrOpen, got %v", err)
	}
	clk.Advance(10 * time.Second)
	probe := mustAllow(t, b)
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("second request during probe: want ErrOpen, got %v", err)
	}
	probe.Cancel()
	mustAllow(t, b).Success()
	if b.State() != Closed {
		t.Fatalf("state = %s, want closed", b.State())
	}
}

func TestBreakerIgnoresStaleResults(t *testing.T) {
	b, clk := newTestBreaker()
	slow := mustAllow(t, b) // admitted while closed, finishes much later
	for range 3 {
		mustAllow(t, b).Failure()
	}
	clk.Advance(10 * time.Second)
	probe := mustAllow(t, b)
	slow.Success()
	if b.State() != HalfOpen {
		t.Fatalf("stale success changed state to %s", b.State())
	}
	probe.Failure()
	if b.State() != Open {
		t.Fatalf("state = %s, want open", b.State())
	}
}

func TestBreakerConcurrentUse(t *testing.T) {
	b := New(5, time.Millisecond, nil)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 1000 {
				p, err := b.Allow()
				if err != nil {
					continue
				}
				if (i+j)%3 == 0 {
					p.Failure()
				} else {
					p.Success()
				}
			}
		})
	}
	wg.Wait()
}
