// Package balancer tracks upstream availability and picks upstreams round-robin.
package balancer

import (
	"errors"
	"net/url"
	"slices"
	"sync/atomic"
	"time"

	"github.com/nikrybak/edge-gateway/internal/breaker"
)

// ErrNoHealthyUpstream is returned when every upstream is unhealthy, ejected, open or
// already tried for the current request.
var ErrNoHealthyUpstream = errors.New("no healthy upstream")

// UpstreamOptions configures passive ejection and the circuit breaker of an upstream.
type UpstreamOptions struct {
	// MaxFailures consecutive failed requests eject the upstream for EjectFor.
	// Zero disables passive ejection.
	MaxFailures int
	EjectFor    time.Duration

	BreakerThreshold int
	BreakerOpenFor   time.Duration

	// Clock defaults to time.Now.
	Clock func() time.Time
}

// Upstream is one backend address with its health, ejection and breaker state. All
// methods are safe for concurrent use.
//
// Three independent signals gate traffic, each answering a different question:
// active health ("does the health endpoint answer?"), passive ejection ("has real
// traffic been failing?", recovered by time) and the breaker ("should we stop
// hammering it?", recovered by a single live probe).
type Upstream struct {
	url     *url.URL
	breaker *breaker.Breaker
	opts    UpstreamOptions

	healthy      atomic.Bool
	ejectedUntil atomic.Int64 // unix nanoseconds
	failures     atomic.Int32
}

// NewUpstream returns an upstream that starts healthy, so a fresh gateway serves
// traffic before the first health check completes; the breaker covers the gap.
func NewUpstream(u *url.URL, opts UpstreamOptions) *Upstream {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	up := &Upstream{
		url:     u,
		breaker: breaker.New(opts.BreakerThreshold, opts.BreakerOpenFor, opts.Clock),
		opts:    opts,
	}
	up.healthy.Store(true)
	return up
}

// URL returns the upstream base URL. Callers must not modify it.
func (u *Upstream) URL() *url.URL { return u.url }

// String returns the upstream address for logs and metric labels.
func (u *Upstream) String() string { return u.url.Host }

// SetHealthy records the result of active health checking.
func (u *Upstream) SetHealthy(ok bool) { u.healthy.Store(ok) }

// Healthy reports the last active health check result.
func (u *Upstream) Healthy() bool { return u.healthy.Load() }

// BreakerState reports the circuit breaker state.
func (u *Upstream) BreakerState() breaker.State { return u.breaker.State() }

// Available reports whether the upstream may receive a new request.
func (u *Upstream) Available() bool {
	if !u.healthy.Load() {
		return false
	}
	if u.opts.Clock().UnixNano() < u.ejectedUntil.Load() {
		return false
	}
	return u.breaker.State() != breaker.Open
}

// Acquire asks the breaker to admit one request.
func (u *Upstream) Acquire() (Permit, error) {
	p, err := u.breaker.Allow()
	if err != nil {
		return Permit{}, err
	}
	return Permit{u: u, bp: p}, nil
}

// Permit tracks one request to an upstream. Exactly one method must be called.
type Permit struct {
	u  *Upstream
	bp breaker.Permit
}

// Success records a successful request.
func (p Permit) Success() {
	p.u.failures.Store(0)
	p.bp.Success()
}

// Failure records a failed request and ejects the upstream after MaxFailures in a row.
func (p Permit) Failure() {
	p.bp.Failure()
	if p.u.opts.MaxFailures <= 0 {
		return
	}
	if n := p.u.failures.Add(1); int(n) >= p.u.opts.MaxFailures {
		p.u.failures.Store(0)
		p.u.ejectedUntil.Store(p.u.opts.Clock().Add(p.u.opts.EjectFor).UnixNano())
	}
}

// Cancel releases the permit without blaming the upstream, e.g. when the client left.
func (p Permit) Cancel() { p.bp.Cancel() }

// Pool is a fixed set of upstreams balanced round-robin.
type Pool struct {
	upstreams []*Upstream
	next      atomic.Uint64
}

// NewPool returns a pool over upstreams.
func NewPool(upstreams []*Upstream) *Pool {
	return &Pool{upstreams: upstreams}
}

// Upstreams returns the pool members. The slice must not be modified.
func (p *Pool) Upstreams() []*Upstream { return p.upstreams }

// Next returns the next available upstream that is not in tried. Skipping tried
// upstreams makes a retry land on a different backend whenever one exists.
func (p *Pool) Next(tried []*Upstream) (*Upstream, error) {
	n := uint64(len(p.upstreams))
	if n == 0 {
		return nil, ErrNoHealthyUpstream
	}
	start := p.next.Add(1) - 1
	for i := range n {
		u := p.upstreams[(start+i)%n]
		if u.Available() && !slices.Contains(tried, u) {
			return u, nil
		}
	}
	return nil, ErrNoHealthyUpstream
}

// HealthyCount returns how many upstreams are currently available.
func (p *Pool) HealthyCount() int {
	c := 0
	for _, u := range p.upstreams {
		if u.Available() {
			c++
		}
	}
	return c
}
