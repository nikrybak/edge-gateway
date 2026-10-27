package ratelimit

import (
	"net"
	"net/http"
	"strconv"
	"time"
)

// HTTP rate limits requests by an API key header, falling back to the client IP.
//
// API keys and IPs live in separate limiters on purpose: with a shared keyspace a
// client could send another client's IP as its API key and drain that client's bucket.
type HTTP struct {
	header    string
	byKey     *Limiter
	byIP      *Limiter
	onLimited func(*http.Request)
}

// NewHTTP returns a limiter for HTTP requests. header may be empty to limit by IP only.
// onLimited, if not nil, is called for every rejected request.
func NewHTTP(ratePerSecond float64, burst int, header string, clock func() time.Time, onLimited func(*http.Request)) *HTTP {
	h := &HTTP{header: header, byIP: New(ratePerSecond, burst, clock), onLimited: onLimited}
	if header != "" {
		h.byKey = New(ratePerSecond, burst, clock)
	}
	return h
}

// Wrap returns next guarded by the limiter. Rejected requests get 429 with a
// Retry-After header rounded up to whole seconds, as RFC 9110 requires.
func (h *HTTP) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok, wait := h.allow(r)
		if ok {
			next.ServeHTTP(w, r)
			return
		}
		if h.onLimited != nil {
			h.onLimited(r)
		}
		secs := max(1, int((wait+time.Second-1)/time.Second))
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}` + "\n"))
	})
}

func (h *HTTP) allow(r *http.Request) (bool, time.Duration) {
	if h.byKey != nil {
		if key := r.Header.Get(h.header); key != "" {
			return h.byKey.Allow(key)
		}
	}
	return h.byIP.Allow(ClientIP(r))
}

// ClientIP returns the host part of r.RemoteAddr. X-Forwarded-For is deliberately
// ignored: it is client-controlled unless a trusted proxy in front of the gateway
// overwrites it, and trusting it by default would let anyone pick their own bucket.
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
