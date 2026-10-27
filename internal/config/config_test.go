package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const validYAML = `
server:
  listen: ":8080"
  admin_listen: ":9090"
routes:
  - name: api
    host: API.example.com
    path_prefix: /api/
    strip_prefix: true
    upstreams: ["http://10.0.0.1:8000", "https://10.0.0.2"]
    retry: {attempts: 3}
    rate_limit: {requests_per_second: 5, key_header: x-api-key}
`

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte(validYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	r := cfg.Routes[0]
	checks := []struct {
		name      string
		got, want any
	}{
		{"host lowercased", r.Host, "api.example.com"},
		{"route timeout", r.Timeout, 10 * time.Second},
		{"retry attempts kept", r.Retry.Attempts, 3},
		{"base backoff", r.Retry.BaseBackoff, 25 * time.Millisecond},
		{"burst defaults to rps", r.RateLimit.Burst, 5},
		{"header canonicalized", r.RateLimit.KeyHeader, "X-Api-Key"},
		{"breaker threshold", r.Breaker.FailureThreshold, 5},
		{"shutdown timeout", cfg.Server.ShutdownTimeout, 15 * time.Second},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		wantField string
	}{
		{"empty file", ``, "config"},
		{"no routes", `server: {listen: ":1"}`, "routes"},
		{"missing name", `routes: [{upstreams: ["http://a"]}]`, "routes[0].name"},
		{"bad scheme", `routes: [{name: a, upstreams: ["ftp://a"]}]`, "routes[0].upstreams[0]"},
		{"missing host", `routes: [{name: a, upstreams: ["http://"]}]`, "routes[0].upstreams[0]"},
		{"no upstreams", `routes: [{name: a}]`, "routes[0].upstreams"},
		{"relative prefix", `routes: [{name: a, path_prefix: api, upstreams: ["http://a"]}]`, "routes[0].path_prefix"},
		{"bad wildcard", `routes: [{name: a, host: "a.*.com", upstreams: ["http://a"]}]`, "routes[0].host"},
		{"too many attempts", `routes: [{name: a, upstreams: ["http://a"], retry: {attempts: 50}}]`, "routes[0].retry.attempts"},
		{"per try above timeout", `routes: [{name: a, timeout: 1s, upstreams: ["http://a"], retry: {per_try_timeout: 2s}}]`, "routes[0].retry.per_try_timeout"},
		{"health timeout above interval", `routes: [{name: a, upstreams: ["http://a"], health_check: {path: /h, interval: 1s, timeout: 2s}}]`, "routes[0].health_check.timeout"},
		{"zero rate", `routes: [{name: a, upstreams: ["http://a"], rate_limit: {burst: 1}}]`, "routes[0].rate_limit.requests_per_second"},
		{"same listeners", `{server: {listen: ":1", admin_listen: ":1"}, routes: [{name: a, upstreams: ["http://a"]}]}`, "server.admin_listen"},
		{
			"duplicate name",
			`routes: [{name: a, path_prefix: /x, upstreams: ["http://a"]}, {name: a, path_prefix: /y, upstreams: ["http://a"]}]`,
			"routes[1].name",
		},
		{
			"duplicate matcher",
			`routes: [{name: a, upstreams: ["http://a"]}, {name: b, upstreams: ["http://b"]}]`,
			"routes[1]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("want ErrInvalidConfig, got %v", err)
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("want *ValidationError, got %T: %v", err, err)
			}
			if !strings.Contains(err.Error(), tt.wantField+":") {
				t.Errorf("error %q does not mention field %q", err, tt.wantField)
			}
		})
	}
}

func TestParseRejectsUnknownField(t *testing.T) {
	_, err := Parse([]byte(`routes: [{name: a, upstream: ["http://a"]}]`))
	if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "upstream") {
		t.Fatalf("want unknown field error, got %v", err)
	}
}

func TestParseReportsAllErrors(t *testing.T) {
	_, err := Parse([]byte(`routes: [{upstreams: ["ftp://a"], retry: {attempts: 0, base_backoff: -1s}}]`))
	for _, field := range []string{"routes[0].name", "routes[0].upstreams[0]", "routes[0].retry"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("error %q is missing %s", err, field)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("want os.ErrNotExist, got %v", err)
	}
}

func TestWatcherReloadsOnChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gw.yaml")
	writeFile(t, path, validYAML)

	var (
		mu      sync.Mutex
		configs []*Config
		errs    []error
	)
	w := NewWatcher(path, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx,
			func(c *Config) { mu.Lock(); configs = append(configs, c); mu.Unlock() },
			func(err error) { mu.Lock(); errs = append(errs, err); mu.Unlock() })
	}()
	t.Cleanup(func() { cancel(); <-done })

	writeFile(t, path, "routes: [")
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(errs) > 0 })

	writeFile(t, path, strings.Replace(validYAML, "attempts: 3", "attempts: 2", 1))
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(configs) > 0 })

	mu.Lock()
	defer mu.Unlock()
	if got := configs[len(configs)-1].Routes[0].Retry.Attempts; got != 2 {
		t.Fatalf("reloaded attempts = %d, want 2", got)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
