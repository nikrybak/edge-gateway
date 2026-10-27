// Package observability provides request IDs, structured access logs and a minimal
// Prometheus text-format metrics registry.
package observability

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultLatencyBuckets are histogram upper bounds in seconds tuned for a gateway:
// dense below 100ms where most proxied requests land, sparse up to 10s timeouts.
var DefaultLatencyBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Registry holds metric families and renders them in the Prometheus text exposition
// format. It is a deliberately small subset of client_golang: counters, histograms and
// callback gauges with fixed label names, which is all the gateway needs.
type Registry struct {
	mu       sync.Mutex
	families []family
}

type family interface {
	name() string
	write(w *bufio.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) register(f family) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.families {
		if existing.name() == f.name() {
			// Duplicate registration is a wiring bug caught at startup, never per request.
			panic("observability: metric " + f.name() + " registered twice")
		}
	}
	r.families = append(r.families, f)
}

// WriteTo renders all metrics.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	fams := slices.Clone(r.families)
	r.mu.Unlock()
	slices.SortFunc(fams, func(a, b family) int { return strings.Compare(a.name(), b.name()) })

	cw := &countingWriter{w: w}
	bw := bufio.NewWriter(cw)
	for _, f := range fams {
		f.write(bw)
	}
	err := bw.Flush()
	return cw.n, err
}

// Handler serves the registry at /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = r.WriteTo(w)
	})
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// series is the label-keyed storage shared by vector metrics.
type series[T any] struct {
	labels []string
	m      sync.Map // joined label values -> *seriesEntry[T]
	newFn  func() *T
}

type seriesEntry[T any] struct {
	values []string
	metric *T
}

func (s *series[T]) with(values []string) *T {
	if len(values) != len(s.labels) {
		// A mismatch is a wiring bug, but metrics must never take a request down, so
		// the values are padded or truncated instead of panicking.
		fixed := make([]string, len(s.labels))
		copy(fixed, values)
		values = fixed
	}
	key := strings.Join(values, "\xff")
	if e, ok := s.m.Load(key); ok {
		return e.(*seriesEntry[T]).metric
	}
	e, _ := s.m.LoadOrStore(key, &seriesEntry[T]{values: slices.Clone(values), metric: s.newFn()})
	return e.(*seriesEntry[T]).metric
}

func (s *series[T]) sorted() []*seriesEntry[T] {
	var out []*seriesEntry[T]
	s.m.Range(func(_, v any) bool {
		out = append(out, v.(*seriesEntry[T]))
		return true
	})
	slices.SortFunc(out, func(a, b *seriesEntry[T]) int { return slices.Compare(a.values, b.values) })
	return out
}

// Counter is a monotonically increasing integer.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Value returns the current count.
func (c *Counter) Value() uint64 { return c.v.Load() }

// CounterVec is a counter family partitioned by labels.
type CounterVec struct {
	fname, help string
	s           series[Counter]
}

// NewCounterVec registers a counter family.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{fname: name, help: help, s: series[Counter]{labels: labels, newFn: func() *Counter { return &Counter{} }}}
	r.register(c)
	return c
}

// With returns the counter for the given label values, creating it on first use.
func (c *CounterVec) With(values ...string) *Counter { return c.s.with(values) }

func (c *CounterVec) name() string { return c.fname }

func (c *CounterVec) write(w *bufio.Writer) {
	writeHeader(w, c.fname, c.help, "counter")
	for _, e := range c.s.sorted() {
		writeSample(w, c.fname, c.s.labels, e.values, "", "", float64(e.metric.Value()))
	}
}

// Histogram counts observations into cumulative buckets.
type Histogram struct {
	bounds  []float64
	counts  []atomic.Uint64 // per bucket, non-cumulative; the last is +Inf
	count   atomic.Uint64
	sumBits atomic.Uint64
}

// Observe records v.
func (h *Histogram) Observe(v float64) {
	i, _ := slices.BinarySearch(h.bounds, v)
	h.counts[i].Add(1)
	h.count.Add(1)
	for {
		old := h.sumBits.Load()
		if h.sumBits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

// HistogramVec is a histogram family partitioned by labels.
type HistogramVec struct {
	fname, help string
	s           series[Histogram]
}

// NewHistogramVec registers a histogram family with the given sorted bucket bounds.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	bounds := slices.Clone(buckets)
	slices.Sort(bounds)
	h := &HistogramVec{fname: name, help: help, s: series[Histogram]{labels: labels, newFn: func() *Histogram {
		return &Histogram{bounds: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
	}}}
	r.register(h)
	return h
}

// With returns the histogram for the given label values.
func (h *HistogramVec) With(values ...string) *Histogram { return h.s.with(values) }

func (h *HistogramVec) name() string { return h.fname }

func (h *HistogramVec) write(w *bufio.Writer) {
	writeHeader(w, h.fname, h.help, "histogram")
	for _, e := range h.s.sorted() {
		m := e.metric
		var cum uint64
		for i, b := range m.bounds {
			cum += m.counts[i].Load()
			writeSample(w, h.fname+"_bucket", h.s.labels, e.values, "le", formatFloat(b), float64(cum))
		}
		cum += m.counts[len(m.bounds)].Load()
		writeSample(w, h.fname+"_bucket", h.s.labels, e.values, "le", "+Inf", float64(cum))
		writeSample(w, h.fname+"_sum", h.s.labels, e.values, "", "", math.Float64frombits(m.sumBits.Load()))
		writeSample(w, h.fname+"_count", h.s.labels, e.values, "", "", float64(cum))
	}
}

// GaugeFunc is a gauge family whose samples are produced on scrape. It suits values
// that already live elsewhere, such as upstream health, and change with config reloads.
type GaugeFunc struct {
	fname, help string
	labels      []string
	collect     func(emit func(value float64, labelValues ...string))
}

// NewGaugeFunc registers a callback gauge.
func (r *Registry) NewGaugeFunc(name, help string, labels []string, collect func(emit func(value float64, labelValues ...string))) {
	r.register(&GaugeFunc{fname: name, help: help, labels: labels, collect: collect})
}

func (g *GaugeFunc) name() string { return g.fname }

func (g *GaugeFunc) write(w *bufio.Writer) {
	writeHeader(w, g.fname, g.help, "gauge")
	g.collect(func(v float64, values ...string) {
		if len(values) == len(g.labels) {
			writeSample(w, g.fname, g.labels, values, "", "", v)
		}
	})
}

func writeHeader(w *bufio.Writer, name, help, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func writeSample(w *bufio.Writer, name string, labels, values []string, extraName, extraValue string, v float64) {
	w.WriteString(name)
	if len(labels) > 0 || extraName != "" {
		w.WriteByte('{')
		sep := ""
		for i, l := range labels {
			w.WriteString(sep + l + `="` + escapeLabel(values[i]) + `"`)
			sep = ","
		}
		if extraName != "" {
			w.WriteString(sep + extraName + `="` + extraValue + `"`)
		}
		w.WriteByte('}')
	}
	w.WriteByte(' ')
	w.WriteString(formatFloat(v))
	w.WriteByte('\n')
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escapeLabel(s string) string { return labelEscaper.Replace(s) }

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}
