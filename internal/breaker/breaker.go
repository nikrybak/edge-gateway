// Package breaker implements a consecutive-failure circuit breaker with a single
// half-open probe.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen is returned by Allow while the breaker rejects traffic.
var ErrOpen = errors.New("circuit breaker is open")

// State is the breaker state.
type State int

// Breaker states.
const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Clock returns the current time. Tests inject a fake one.
type Clock func() time.Time

// Breaker trips to Open after Threshold consecutive failures. After OpenFor it lets
// exactly one probe request through (HalfOpen); the probe's outcome closes the breaker
// or re-opens it for another OpenFor.
//
// A mutex is used instead of atomics because state transitions touch several fields
// together and the critical sections are a handful of instructions; the breaker is
// never the bottleneck next to a network round trip.
type Breaker struct {
	threshold int
	openFor   time.Duration
	now       Clock

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	probing  bool
	// generation increments on every state change so that results of requests
	// admitted in an earlier state cannot flip the current one.
	generation uint64
}

// New returns a closed breaker. A nil clock means time.Now.
func New(threshold int, openFor time.Duration, clock Clock) *Breaker {
	if clock == nil {
		clock = time.Now
	}
	return &Breaker{threshold: max(1, threshold), openFor: openFor, now: clock}
}

// Permit is a token for one admitted request. Exactly one of its methods must be
// called when the request finishes.
type Permit struct {
	b          *Breaker
	generation uint64
	probe      bool
}

// Allow admits a request or returns ErrOpen.
func (b *Breaker) Allow() (Permit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.currentLocked() {
	case Open:
		return Permit{}, ErrOpen
	case HalfOpen:
		if b.probing {
			return Permit{}, ErrOpen
		}
		b.probing = true
		return Permit{b: b, generation: b.generation, probe: true}, nil
	default:
		return Permit{b: b, generation: b.generation}, nil
	}
}

// Success records a successful request.
func (p Permit) Success() { p.done(true) }

// Failure records a failed request.
func (p Permit) Failure() { p.done(false) }

// Cancel releases the permit without counting it, for requests aborted by the client.
func (p Permit) Cancel() {
	if p.b == nil || !p.probe {
		return
	}
	p.b.mu.Lock()
	if p.b.generation == p.generation {
		p.b.probing = false
	}
	p.b.mu.Unlock()
}

func (p Permit) done(ok bool) {
	b := p.b
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if p.generation != b.generation {
		return
	}
	switch {
	case ok && p.probe:
		b.transitionLocked(Closed)
	case ok:
		b.failures = 0
	case p.probe:
		b.transitionLocked(Open)
	default:
		b.failures++
		if b.failures >= b.threshold {
			b.transitionLocked(Open)
		}
	}
}

// State reports the current state, promoting Open to HalfOpen once OpenFor elapsed.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.currentLocked()
}

func (b *Breaker) currentLocked() State {
	if b.state == Open && b.now().Sub(b.openedAt) >= b.openFor {
		b.transitionLocked(HalfOpen)
	}
	return b.state
}

func (b *Breaker) transitionLocked(to State) {
	b.state = to
	b.failures = 0
	b.probing = false
	b.generation++
	if to == Open {
		b.openedAt = b.now()
	}
}
