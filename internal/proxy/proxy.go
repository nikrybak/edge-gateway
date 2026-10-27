// Package proxy forwards requests to an upstream pool with retries and per-upstream
// circuit breaking.
package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/nikrybak/edge-gateway/internal/balancer"
	"github.com/nikrybak/edge-gateway/internal/breaker"
	"github.com/nikrybak/edge-gateway/internal/observability"
)

// ErrUpstreamStatus marks an attempt that returned a retryable 5xx status.
var ErrUpstreamStatus = errors.New("upstream returned retryable status")

// StatusClientClosedRequest is recorded when the client disconnects before the
// upstream answers. The code is borrowed from nginx; it never reaches the client.
const StatusClientClosedRequest = 499

// Balancer picks the upstream for each attempt, avoiding the ones already tried.
type Balancer interface {
	Next(tried []*balancer.Upstream) (*balancer.Upstream, error)
}

// Observer receives one call per upstream attempt. Outcome is one of "ok", "error",
// "status_5xx", "breaker_open" or "canceled".
type Observer interface {
	ObserveAttempt(route, upstream, outcome string)
}

// RetryPolicy controls retries. Attempts includes the first try.
type RetryPolicy struct {
	Attempts      int
	BaseBackoff   time.Duration
	MaxBackoff    time.Duration
	PerTryTimeout time.Duration
}

// Options configures a Proxy. Balancer and Transport are required.
type Options struct {
	Route     string
	Balancer  Balancer
	Transport http.RoundTripper
	// Rewrite maps the incoming path to the upstream path. Nil keeps it unchanged.
	Rewrite  func(path string) string
	Retry    RetryPolicy
	Logger   *slog.Logger
	Observer Observer
	// Jitter returns a uniform value in [0, n). Nil uses math/rand/v2.
	Jitter func(n int64) int64
}

// Proxy is an http.Handler forwarding to a pool of upstreams.
//
// It is built on httputil.ReverseProxy for header hygiene, streaming, trailers and
// protocol upgrades, and plugs the retry loop in as the RoundTripper: ReverseProxy
// only writes to the client after the transport returns, so retries stay invisible to
// the client until a final response is chosen.
type Proxy struct {
	rp *httputil.ReverseProxy
}

// New returns a Proxy.
func New(opts Options) *Proxy {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Observer == nil {
		opts.Observer = nopObserver{}
	}
	if opts.Jitter == nil {
		opts.Jitter = rand.Int64N
	}
	if opts.Retry.Attempts < 1 {
		opts.Retry.Attempts = 1
	}
	rewrite := opts.Rewrite
	if rewrite == nil {
		rewrite = func(p string) string { return p }
	}
	rt := &retryTransport{opts: opts}
	return &Proxy{rp: &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetXForwarded()
			pr.Out.URL.Path = rewrite(pr.In.URL.Path)
			if pr.In.URL.RawPath != "" {
				pr.Out.URL.RawPath = rewrite(pr.In.URL.RawPath)
			}
			// Let the upstream see its own host name; the original one travels in
			// X-Forwarded-Host.
			pr.Out.Host = ""
		},
		Transport:    rt,
		ErrorHandler: errorHandler(opts.Logger),
		ErrorLog:     slog.NewLogLogger(opts.Logger.Handler(), slog.LevelWarn),
	}}
}

// ServeHTTP implements http.Handler.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.rp.ServeHTTP(w, r) }

type retryTransport struct {
	opts Options
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	info := observability.Info(ctx)
	attempts := 1
	if Retryable(req) {
		attempts = t.opts.Retry.Attempts
	}

	tried := make([]*balancer.Upstream, 0, attempts)
	var lastErr error
	for attempt := range attempts {
		if attempt > 0 {
			if err := sleep(ctx, Backoff(attempt, t.opts.Retry.BaseBackoff, t.opts.Retry.MaxBackoff, t.opts.Jitter)); err != nil {
				return nil, errors.Join(lastErr, err)
			}
		}
		up, err := t.next(tried)
		if err != nil {
			return nil, errors.Join(lastErr, err)
		}
		tried = append(tried, up)
		info.Upstream, info.Attempts = up.String(), attempt+1

		permit, err := up.Acquire()
		if err != nil {
			t.opts.Observer.ObserveAttempt(t.opts.Route, up.String(), "breaker_open")
			lastErr = fmt.Errorf("upstream %s: %w", up, err)
			continue
		}

		resp, err := t.try(ctx, req, up)
		switch {
		case err != nil && errors.Is(ctx.Err(), context.Canceled):
			permit.Cancel()
			t.opts.Observer.ObserveAttempt(t.opts.Route, up.String(), "canceled")
			return nil, err
		case err != nil:
			permit.Failure()
			t.opts.Observer.ObserveAttempt(t.opts.Route, up.String(), "error")
			lastErr = fmt.Errorf("upstream %s: %w", up, err)
			if ctx.Err() != nil {
				return nil, lastErr
			}
			t.logRetry(ctx, req, up, attempt, attempts, lastErr)
			continue
		case resp.StatusCode >= 500:
			permit.Failure()
			t.opts.Observer.ObserveAttempt(t.opts.Route, up.String(), "status_5xx")
			if attempt == attempts-1 || !retryableStatus(resp.StatusCode) {
				return resp, nil
			}
			drain(resp.Body)
			lastErr = fmt.Errorf("upstream %s: %w %d", up, ErrUpstreamStatus, resp.StatusCode)
			t.logRetry(ctx, req, up, attempt, attempts, lastErr)
			continue
		default:
			permit.Success()
			t.opts.Observer.ObserveAttempt(t.opts.Route, up.String(), "ok")
			return resp, nil
		}
	}
	return nil, lastErr
}

// next prefers an upstream not yet tried and falls back to any available one, so a
// pool with a single healthy member can still use its retry budget.
func (t *retryTransport) next(tried []*balancer.Upstream) (*balancer.Upstream, error) {
	up, err := t.opts.Balancer.Next(tried)
	if err != nil && len(tried) > 0 {
		up, err = t.opts.Balancer.Next(nil)
	}
	return up, err
}

func (t *retryTransport) try(ctx context.Context, req *http.Request, up *balancer.Upstream) (*http.Response, error) {
	cancel := context.CancelFunc(func() {})
	// Upgraded connections outlive the round trip, so a per-try deadline would kill them.
	if d := t.opts.Retry.PerTryTimeout; d > 0 && req.Header.Get("Upgrade") == "" {
		ctx, cancel = context.WithTimeout(ctx, d)
	}
	out := req.Clone(ctx)
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			cancel()
			return nil, fmt.Errorf("replay body: %w", err)
		}
		out.Body = body
	}
	target := up.URL()
	out.URL.Scheme = target.Scheme
	out.URL.Host = target.Host
	out.URL.Path, out.URL.RawPath = joinURLPath(target, req.URL)

	resp, err := t.opts.Transport.RoundTrip(out)
	if err != nil {
		cancel()
		return nil, err
	}
	// The attempt context must outlive RoundTrip because the body is streamed after it
	// returns; it is released when ReverseProxy closes the body.
	resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

func (t *retryTransport) logRetry(ctx context.Context, req *http.Request, up *balancer.Upstream, attempt, attempts int, err error) {
	t.opts.Logger.LogAttrs(ctx, slog.LevelWarn, "upstream attempt failed",
		slog.String("request_id", observability.RequestID(ctx)),
		slog.String("route", t.opts.Route),
		slog.String("method", req.Method),
		slog.String("upstream", up.String()),
		slog.Int("attempt", attempt+1),
		slog.Int("max_attempts", attempts),
		slog.String("error", err.Error()),
	)
}

// Retryable reports whether req may be sent more than once: the method must be
// idempotent per RFC 9110 and the body, if any, must be replayable. A retried POST
// could charge a card twice; a retried GET at worst wastes upstream work.
func Retryable(req *http.Request) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodPut, http.MethodDelete:
	default:
		return false
	}
	return req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
}

func retryableStatus(code int) bool {
	return code == http.StatusBadGateway || code == http.StatusServiceUnavailable || code == http.StatusGatewayTimeout
}

// Backoff returns the delay before retry number attempt (1-based) using exponential
// growth capped at max with "equal jitter": half the delay is fixed and half random.
// Unlike full jitter this keeps a guaranteed minimum spacing between attempts while
// still de-synchronizing clients that failed at the same moment.
func Backoff(attempt int, base, maxDelay time.Duration, jitter func(int64) int64) time.Duration {
	d := maxDelay
	if shift := attempt - 1; shift < 32 {
		if exp := base << shift; exp > 0 && exp < maxDelay {
			d = exp
		}
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(jitter(int64(half)+1))
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func drain(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 64<<10))
	_ = body.Close()
}

func joinURLPath(target, in *url.URL) (path, rawPath string) {
	if target.Path == "" || target.Path == "/" {
		return in.Path, in.RawPath
	}
	base := strings.TrimSuffix(target.Path, "/")
	path = base + in.Path
	if in.RawPath != "" {
		rawPath = strings.TrimSuffix(target.EscapedPath(), "/") + in.RawPath
	}
	return path, rawPath
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func errorHandler(logger *slog.Logger) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		status, msg := classify(r.Context(), err)
		ctx := r.Context()
		if status != StatusClientClosedRequest {
			logger.LogAttrs(ctx, slog.LevelWarn, "proxy error",
				slog.String("request_id", observability.RequestID(ctx)),
				slog.Int("status", status),
				slog.String("error", err.Error()))
		}
		WriteError(w, r, status, msg)
	}
}

func classify(ctx context.Context, err error) (int, string) {
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return StatusClientClosedRequest, "client closed request"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "upstream timed out"
	case errors.Is(err, breaker.ErrOpen), errors.Is(err, balancer.ErrNoHealthyUpstream):
		return http.StatusServiceUnavailable, "no upstream available"
	default:
		return http.StatusBadGateway, "upstream unavailable"
	}
}

// WriteError writes a JSON error body carrying the request ID.
func WriteError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error     string `json:"error"`
		RequestID string `json:"request_id,omitempty"`
	}{msg, observability.RequestID(r.Context())})
}

type nopObserver struct{}

func (nopObserver) ObserveAttempt(string, string, string) {}
