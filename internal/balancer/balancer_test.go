package balancer

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clock struct{ ns atomic.Int64 }

func (c *clock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *clock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

func newPool(t *testing.T, n int, opts UpstreamOptions) *Pool {
	t.Helper()
	ups := make([]*Upstream, n)
	for i := range ups {
		u, err := url.Parse(fmt.Sprintf("http://10.0.0.%d:80", i+1))
		if err != nil {
			t.Fatal(err)
		}
		ups[i] = NewUpstream(u, opts)
	}
	return NewPool(ups)
}

func defaultOpts(c *clock) UpstreamOptions {
	return UpstreamOptions{MaxFailures: 2, EjectFor: time.Minute, BreakerThreshold: 100, BreakerOpenFor: time.Second, Clock: c.Now}
}

func TestPoolRoundRobin(t *testing.T) {
	p := newPool(t, 3, defaultOpts(&clock{}))
	counts := map[string]int{}
	for range 300 {
		u, err := p.Next(nil)
		if err != nil {
			t.Fatal(err)
		}
		counts[u.String()]++
	}
	for host, c := range counts {
		if c != 100 {
			t.Errorf("%s got %d picks, want 100", host, c)
		}
	}
}

func TestPoolSkips(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(p *Pool, c *clock) []*Upstream // returns tried
		want    []string
	}{
		{"unhealthy", func(p *Pool, _ *clock) []*Upstream {
			p.upstreams[0].SetHealthy(false)
			return nil
		}, []string{"10.0.0.2:80", "10.0.0.3:80"}},
		{"tried", func(p *Pool, _ *clock) []*Upstream {
			return []*Upstream{p.upstreams[1]}
		}, []string{"10.0.0.1:80", "10.0.0.3:80"}},
		{"passively ejected", func(p *Pool, _ *clock) []*Upstream {
			for range 2 {
				mustAcquire(t, p.upstreams[2]).Failure()
			}
			return nil
		}, []string{"10.0.0.1:80", "10.0.0.2:80"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &clock{}
			p := newPool(t, 3, defaultOpts(c))
			tried := tt.prepare(p, c)
			seen := map[string]bool{}
			for range 10 {
				u, err := p.Next(tried)
				if err != nil {
					t.Fatal(err)
				}
				seen[u.String()] = true
			}
			if len(seen) != len(tt.want) {
				t.Fatalf("picked %v, want %v", seen, tt.want)
			}
			for _, w := range tt.want {
				if !seen[w] {
					t.Errorf("%s never picked", w)
				}
			}
		})
	}
}

func TestEjectionExpires(t *testing.T) {
	c := &clock{}
	p := newPool(t, 1, defaultOpts(c))
	u := p.upstreams[0]
	mustAcquire(t, u).Failure()
	mustAcquire(t, u).Failure()
	if _, err := p.Next(nil); !errors.Is(err, ErrNoHealthyUpstream) {
		t.Fatalf("want ErrNoHealthyUpstream, got %v", err)
	}
	c.Advance(time.Minute)
	if _, err := p.Next(nil); err != nil {
		t.Fatalf("after ejection: %v", err)
	}
}

func TestSuccessResetsPassiveStreak(t *testing.T) {
	c := &clock{}
	u := newPool(t, 1, defaultOpts(c)).upstreams[0]
	mustAcquire(t, u).Failure()
	mustAcquire(t, u).Success()
	mustAcquire(t, u).Failure()
	if !u.Available() {
		t.Fatal("upstream ejected although failures were not consecutive")
	}
}

func TestOpenBreakerMakesUnavailable(t *testing.T) {
	c := &clock{}
	opts := defaultOpts(c)
	opts.MaxFailures = 0
	opts.BreakerThreshold = 1
	u := newPool(t, 1, opts).upstreams[0]
	mustAcquire(t, u).Failure()
	if u.Available() {
		t.Fatal("upstream available with open breaker")
	}
	c.Advance(time.Second)
	if !u.Available() {
		t.Fatal("upstream unavailable once breaker is half-open")
	}
}

func TestPoolConcurrentNext(t *testing.T) {
	p := newPool(t, 4, defaultOpts(&clock{}))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				u, err := p.Next(nil)
				if err != nil {
					t.Error(err)
					return
				}
				perm, err := u.Acquire()
				if err == nil {
					perm.Success()
				}
			}
		})
	}
	wg.Wait()
}

func mustAcquire(t *testing.T, u *Upstream) Permit {
	t.Helper()
	p, err := u.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	return p
}
