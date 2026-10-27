package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// RequestIDHeader is the header used to accept and propagate request IDs.
const RequestIDHeader = "X-Request-ID"

const maxRequestIDLen = 128

type ctxKey int

const (
	requestIDKey ctxKey = iota
	infoKey
)

// NewLogger returns a JSON slog logger writing to w.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// RequestID returns the request ID stored in ctx, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// RequestInfo is filled in by inner handlers so the access log and metrics, which run
// outside them, can report the route and upstream that served the request. It is only
// touched from the request goroutine.
type RequestInfo struct {
	Route    string
	Upstream string
	Attempts int
}

// Info returns the RequestInfo for ctx. It returns a throwaway value outside of the
// Middleware so callers never need a nil check.
func Info(ctx context.Context) *RequestInfo {
	if ri, ok := ctx.Value(infoKey).(*RequestInfo); ok {
		return ri
	}
	return &RequestInfo{}
}

// HTTPMetrics are the request-level metric families recorded by Middleware.
type HTTPMetrics struct {
	Requests *CounterVec   // route, method, code
	Duration *HistogramVec // route
}

// NewHTTPMetrics registers the request metric families on reg.
func NewHTTPMetrics(reg *Registry) *HTTPMetrics {
	return &HTTPMetrics{
		Requests: reg.NewCounterVec("edge_requests_total", "Requests handled, by route, method and status code.", "route", "method", "code"),
		Duration: reg.NewHistogramVec("edge_request_duration_seconds", "End-to-end request latency.", DefaultLatencyBuckets, "route"),
	}
}

// Middleware assigns or propagates X-Request-ID, then records an access log line and
// request metrics once the inner handler returns.
func Middleware(logger *slog.Logger, m *HTTPMetrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get(RequestIDHeader)
		if !validRequestID(id) {
			id = newRequestID()
			r.Header.Set(RequestIDHeader, id)
		}
		w.Header().Set(RequestIDHeader, id)

		info := &RequestInfo{}
		ctx := context.WithValue(context.WithValue(r.Context(), requestIDKey, id), infoKey, info)
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		elapsed := time.Since(start)
		route := info.Route
		if route == "" {
			route = "none"
		}
		m.Requests.With(route, r.Method, strconv.Itoa(rec.status)).Inc()
		m.Duration.With(route).Observe(elapsed.Seconds())

		level := slog.LevelInfo
		if rec.status >= 500 {
			level = slog.LevelWarn
		}
		logger.LogAttrs(ctx, level, "request",
			slog.String("request_id", id),
			slog.String("method", r.Method),
			slog.String("host", r.Host),
			slog.String("path", r.URL.Path),
			slog.String("route", route),
			slog.String("upstream", info.Upstream),
			slog.Int("attempts", info.Attempts),
			slog.Int("status", rec.status),
			slog.Int64("bytes", rec.bytes),
			slog.Duration("duration", elapsed),
			slog.String("remote", r.RemoteAddr),
		)
	})
}

// validRequestID accepts client IDs only if they are short printable ASCII, so a
// hostile value cannot inject log lines or bloat every upstream request.
func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		if c := id[i]; c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}

func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error since Go 1.24
	return hex.EncodeToString(b[:])
}

// recorder captures the status code and body size. Unwrap lets http.ResponseController
// reach the underlying writer for Flush, which streaming responses rely on.
type recorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (r *recorder) WriteHeader(code int) {
	if !r.wroteHeader && code >= 200 {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(p []byte) (int, error) {
	r.wroteHeader = true
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
