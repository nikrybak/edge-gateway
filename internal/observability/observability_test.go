package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestRegistryExposition(t *testing.T) {
	reg := NewRegistry()
	c := reg.NewCounterVec("test_requests_total", "Requests.", "route", "code")
	h := reg.NewHistogramVec("test_latency_seconds", "Latency.", []float64{0.1, 0.5}, "route")
	reg.NewGaugeFunc("test_up", "Up.", []string{"upstream"}, func(emit func(float64, ...string)) {
		emit(1, "a:80")
		emit(0, `b"\`)
	})

	c.With("api", "200").Inc()
	c.With("api", "200").Inc()
	c.With("api", "503").Inc()
	h.With("api").Observe(0.05)
	h.With("api").Observe(0.1)
	h.With("api").Observe(3)

	var buf bytes.Buffer
	if _, err := reg.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	want := `# HELP test_latency_seconds Latency.
# TYPE test_latency_seconds histogram
test_latency_seconds_bucket{route="api",le="0.1"} 2
test_latency_seconds_bucket{route="api",le="0.5"} 2
test_latency_seconds_bucket{route="api",le="+Inf"} 3
test_latency_seconds_sum{route="api"} 3.15
test_latency_seconds_count{route="api"} 3
# HELP test_requests_total Requests.
# TYPE test_requests_total counter
test_requests_total{route="api",code="200"} 2
test_requests_total{route="api",code="503"} 1
# HELP test_up Up.
# TYPE test_up gauge
test_up{upstream="a:80"} 1
test_up{upstream="b\"\\"} 0
`
	if got := buf.String(); got != want {
		t.Fatalf("exposition mismatch\n--- got\n%s--- want\n%s", got, want)
	}
}

func TestLabelArityMismatchDoesNotPanic(t *testing.T) {
	c := NewRegistry().NewCounterVec("x_total", "X.", "a", "b")
	c.With("only-one").Inc()
	if got := c.With("only-one", "").Value(); got != 1 {
		t.Fatalf("padded series value = %d, want 1", got)
	}
}

func TestCounterConcurrentInc(t *testing.T) {
	c := NewRegistry().NewCounterVec("x_total", "X.", "k")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				c.With("v").Inc()
			}
		})
	}
	wg.Wait()
	if got := c.With("v").Value(); got != 8000 {
		t.Fatalf("count = %d, want 8000", got)
	}
}

func TestMiddlewareRequestID(t *testing.T) {
	tests := []struct {
		name     string
		incoming string
		keep     bool
	}{
		{"generated when missing", "", false},
		{"propagated when valid", "abc-123", true},
		{"replaced when it contains spaces", "abc 123", false},
		{"replaced when too long", strings.Repeat("a", 200), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			var seenHeader, seenCtx string
			h := Middleware(slog.New(slog.NewJSONHandler(&logs, nil)), NewHTTPMetrics(NewRegistry()),
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					seenHeader = r.Header.Get(RequestIDHeader)
					seenCtx = RequestID(r.Context())
					Info(r.Context()).Route = "api"
					w.WriteHeader(http.StatusTeapot)
				}))
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			if tt.incoming != "" {
				req.Header.Set(RequestIDHeader, tt.incoming)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			got := rec.Header().Get(RequestIDHeader)
			if got == "" || got != seenHeader || got != seenCtx {
				t.Fatalf("ids differ: response %q, upstream header %q, ctx %q", got, seenHeader, seenCtx)
			}
			if (got == tt.incoming) != tt.keep {
				t.Fatalf("id %q, incoming %q, keep=%t", got, tt.incoming, tt.keep)
			}

			var line map[string]any
			if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
				t.Fatalf("log is not JSON: %v: %s", err, logs.String())
			}
			if line["request_id"] != got || line["route"] != "api" || line["status"] != float64(http.StatusTeapot) {
				t.Errorf("unexpected log line: %v", line)
			}
		})
	}
}
