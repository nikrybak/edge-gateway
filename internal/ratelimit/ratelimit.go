// Package ratelimit implements a sharded, per-key token bucket limiter.
package ratelimit

import (
	"math"
	"sync"
	"time"
)

const shardCount = 64 // power of two so the shard index is a mask

// Limiter is a token bucket per key. Each key may burst up to Burst requests and is
// refilled at Rate tokens per second. It is safe for concurrent use.
//
// Keys are spread over 64 independently locked shards so that unrelated clients rarely
// contend. Buckets are created on first use and evicted lazily: a bucket that has
// refilled to full capacity is indistinguishable from a new one, so dropping it loses
// no information and memory stays proportional to recently active clients.
type Limiter struct {
	ratePerNs float64
	burst     float64
	fullAfter int64 // nanoseconds to refill an empty bucket
	now       func() time.Time
	shards    [shardCount]shard
}

type shard struct {
	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep int64
	// Padding keeps neighbouring shard mutexes on separate cache lines.
	_ [40]byte
}

type bucket struct {
	tokens float64
	last   int64 // unix nanoseconds of the last refill
}

// New returns a limiter. clock may be nil, meaning time.Now.
func New(ratePerSecond float64, burst int, clock func() time.Time) *Limiter {
	if clock == nil {
		clock = time.Now
	}
	l := &Limiter{
		ratePerNs: ratePerSecond / float64(time.Second),
		burst:     float64(max(1, burst)),
		now:       clock,
	}
	l.fullAfter = int64(l.burst / l.ratePerNs)
	for i := range l.shards {
		l.shards[i].buckets = make(map[string]*bucket)
	}
	return l
}

// Allow consumes one token for key. When the bucket is empty it returns false and the
// time until the next token is available.
func (l *Limiter) Allow(key string) (ok bool, retryAfter time.Duration) {
	now := l.now().UnixNano()
	s := &l.shards[fnv32(key)&(shardCount-1)]

	s.mu.Lock()
	b, found := s.buckets[key]
	if !found {
		if now-s.lastSweep >= l.fullAfter {
			l.sweepLocked(s, now)
		}
		b = &bucket{tokens: l.burst, last: now}
		s.buckets[key] = b
	} else if elapsed := now - b.last; elapsed > 0 {
		b.tokens = math.Min(l.burst, b.tokens+float64(elapsed)*l.ratePerNs)
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		s.mu.Unlock()
		return true, 0
	}
	missing := 1 - b.tokens
	s.mu.Unlock()
	return false, time.Duration(math.Ceil(missing / l.ratePerNs))
}

func (l *Limiter) sweepLocked(s *shard, now int64) {
	s.lastSweep = now
	for k, b := range s.buckets {
		if now-b.last >= l.fullAfter {
			delete(s.buckets, k)
		}
	}
}

// Len returns the number of tracked buckets. It is intended for tests and metrics.
func (l *Limiter) Len() int {
	n := 0
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		n += len(s.buckets)
		s.mu.Unlock()
	}
	return n
}

// fnv32 is FNV-1a inlined over the string to avoid the []byte conversion and the
// interface allocation of hash/fnv.
func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
