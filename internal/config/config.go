// Package config loads, defaults and validates the gateway configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ErrInvalidConfig is wrapped by every validation failure returned from Parse and Load.
var ErrInvalidConfig = errors.New("invalid config")

// ValidationError describes a single invalid field. Field is a dotted path such as
// "routes[1].retry.attempts".
type ValidationError struct {
	Field string
	Msg   string
}

func (e *ValidationError) Error() string { return e.Field + ": " + e.Msg }

// Unwrap lets callers match any validation failure with errors.Is(err, ErrInvalidConfig).
func (e *ValidationError) Unwrap() error { return ErrInvalidConfig }

// Config is the root of the gateway configuration file.
type Config struct {
	Server Server  `yaml:"server"`
	Routes []Route `yaml:"routes"`
}

// Server holds listener and lifecycle settings. Listener addresses are read once at
// startup; a hot reload that changes them is rejected.
type Server struct {
	Listen            string        `yaml:"listen"`
	AdminListen       string        `yaml:"admin_listen"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
	// DrainDelay keeps serving after /readyz turns unready so that load balancers
	// stop sending new connections before the listener closes.
	DrainDelay time.Duration `yaml:"drain_delay"`
}

// Route maps a host and path prefix to a pool of upstreams.
type Route struct {
	Name        string        `yaml:"name"`
	Host        string        `yaml:"host"`
	PathPrefix  string        `yaml:"path_prefix"`
	StripPrefix bool          `yaml:"strip_prefix"`
	AddPrefix   string        `yaml:"add_prefix"`
	Timeout     time.Duration `yaml:"timeout"`
	Upstreams   []string      `yaml:"upstreams"`
	HealthCheck HealthCheck   `yaml:"health_check"`
	Passive     Passive       `yaml:"passive"`
	Breaker     Breaker       `yaml:"breaker"`
	Retry       Retry         `yaml:"retry"`
	RateLimit   *RateLimit    `yaml:"rate_limit"`
}

// HealthCheck configures active probing. An empty Path disables it.
type HealthCheck struct {
	Path               string        `yaml:"path"`
	Interval           time.Duration `yaml:"interval"`
	Timeout            time.Duration `yaml:"timeout"`
	HealthyThreshold   int           `yaml:"healthy_threshold"`
	UnhealthyThreshold int           `yaml:"unhealthy_threshold"`
}

// Passive configures ejection of an upstream after consecutive failed requests.
type Passive struct {
	MaxFailures   int           `yaml:"max_failures"`
	EjectDuration time.Duration `yaml:"eject_duration"`
}

// Breaker configures the per-upstream circuit breaker.
type Breaker struct {
	FailureThreshold int           `yaml:"failure_threshold"`
	OpenDuration     time.Duration `yaml:"open_duration"`
}

// Retry configures retries of idempotent requests. Attempts includes the first try.
type Retry struct {
	Attempts      int           `yaml:"attempts"`
	BaseBackoff   time.Duration `yaml:"base_backoff"`
	MaxBackoff    time.Duration `yaml:"max_backoff"`
	PerTryTimeout time.Duration `yaml:"per_try_timeout"`
}

// RateLimit configures a token bucket per client. Clients are identified by KeyHeader
// when present on the request and by remote IP otherwise.
type RateLimit struct {
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
	KeyHeader         string  `yaml:"key_header"`
}

// Load reads and parses the file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// Parse decodes YAML, applies defaults and validates the result. Unknown keys are
// rejected so that typos fail loudly instead of silently using defaults.
func Parse(data []byte) (*Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, &ValidationError{Field: "config", Msg: "file is empty"}
		}
		return nil, fmt.Errorf("%w: decode yaml: %w", ErrInvalidConfig, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	s := &c.Server
	setDefault(&s.Listen, ":8080")
	setDefault(&s.AdminListen, ":9090")
	setDefault(&s.ReadHeaderTimeout, 5*time.Second)
	setDefault(&s.IdleTimeout, 90*time.Second)
	setDefault(&s.ShutdownTimeout, 15*time.Second)

	for i := range c.Routes {
		r := &c.Routes[i]
		setDefault(&r.PathPrefix, "/")
		setDefault(&r.Timeout, 10*time.Second)
		r.Host = strings.ToLower(r.Host)

		hc := &r.HealthCheck
		setDefault(&hc.Interval, 5*time.Second)
		setDefault(&hc.Timeout, time.Second)
		setDefault(&hc.HealthyThreshold, 2)
		setDefault(&hc.UnhealthyThreshold, 3)

		setDefault(&r.Passive.MaxFailures, 5)
		setDefault(&r.Passive.EjectDuration, 30*time.Second)

		setDefault(&r.Breaker.FailureThreshold, 5)
		setDefault(&r.Breaker.OpenDuration, 10*time.Second)

		setDefault(&r.Retry.Attempts, 1)
		setDefault(&r.Retry.BaseBackoff, 25*time.Millisecond)
		setDefault(&r.Retry.MaxBackoff, time.Second)

		if rl := r.RateLimit; rl != nil {
			if rl.Burst == 0 {
				rl.Burst = max(1, int(rl.RequestsPerSecond))
			}
			if rl.KeyHeader != "" {
				rl.KeyHeader = textproto.CanonicalMIMEHeaderKey(rl.KeyHeader)
			}
		}
	}
}

func setDefault[T comparable](field *T, def T) {
	var zero T
	if *field == zero {
		*field = def
	}
}

// Validate reports every problem at once, joined with errors.Join, so an operator can
// fix a broken file in one pass.
func (c *Config) Validate() error {
	var errs []error
	fail := func(field, format string, args ...any) {
		errs = append(errs, &ValidationError{Field: field, Msg: fmt.Sprintf(format, args...)})
	}

	// Port 0 asks the OS for a free port, so two such listeners never collide.
	if c.Server.Listen == c.Server.AdminListen && !strings.HasSuffix(c.Server.Listen, ":0") {
		fail("server.admin_listen", "must differ from server.listen (%q)", c.Server.Listen)
	}
	if c.Server.ShutdownTimeout < 0 || c.Server.DrainDelay < 0 {
		fail("server", "timeouts must not be negative")
	}
	if len(c.Routes) == 0 {
		fail("routes", "at least one route is required")
	}

	names := make(map[string]int, len(c.Routes))
	matchers := make(map[string]string, len(c.Routes))
	for i := range c.Routes {
		r := &c.Routes[i]
		p := fmt.Sprintf("routes[%d]", i)

		switch prev, dup := names[r.Name]; {
		case r.Name == "":
			fail(p+".name", "is required")
		case dup:
			fail(p+".name", "%q already used by routes[%d]", r.Name, prev)
		default:
			names[r.Name] = i
		}

		if !strings.HasPrefix(r.PathPrefix, "/") {
			fail(p+".path_prefix", "must start with '/', got %q", r.PathPrefix)
		}
		if r.AddPrefix != "" && !strings.HasPrefix(r.AddPrefix, "/") {
			fail(p+".add_prefix", "must start with '/', got %q", r.AddPrefix)
		}
		if strings.Contains(r.Host, "*") && (!strings.HasPrefix(r.Host, "*.") || strings.Count(r.Host, "*") > 1) {
			fail(p+".host", "wildcard must be a single leading label like *.example.com, got %q", r.Host)
		}
		key := r.Host + "|" + r.PathPrefix
		if other, dup := matchers[key]; dup {
			fail(p, "host %q and path_prefix %q already routed by %q", r.Host, r.PathPrefix, other)
		} else {
			matchers[key] = r.Name
		}
		if r.Timeout < 0 {
			fail(p+".timeout", "must not be negative")
		}

		if len(r.Upstreams) == 0 {
			fail(p+".upstreams", "at least one upstream is required")
		}
		for j, raw := range r.Upstreams {
			if err := validateUpstream(raw); err != nil {
				fail(fmt.Sprintf("%s.upstreams[%d]", p, j), "%v", err)
			}
		}

		hc := r.HealthCheck
		if hc.Path != "" && !strings.HasPrefix(hc.Path, "/") {
			fail(p+".health_check.path", "must start with '/', got %q", hc.Path)
		}
		if hc.Path != "" && hc.Timeout >= hc.Interval {
			fail(p+".health_check.timeout", "must be shorter than interval (%s)", hc.Interval)
		}
		if hc.HealthyThreshold < 1 || hc.UnhealthyThreshold < 1 {
			fail(p+".health_check", "thresholds must be at least 1")
		}
		if r.Passive.MaxFailures < 0 {
			fail(p+".passive.max_failures", "must not be negative")
		}
		if r.Breaker.FailureThreshold < 1 {
			fail(p+".breaker.failure_threshold", "must be at least 1")
		}
		if r.Breaker.OpenDuration <= 0 {
			fail(p+".breaker.open_duration", "must be positive")
		}

		rt := r.Retry
		if rt.Attempts < 1 || rt.Attempts > 10 {
			fail(p+".retry.attempts", "must be between 1 and 10, got %d", rt.Attempts)
		}
		if rt.BaseBackoff <= 0 || rt.MaxBackoff < rt.BaseBackoff {
			fail(p+".retry", "need 0 < base_backoff <= max_backoff")
		}
		if rt.PerTryTimeout < 0 || (rt.PerTryTimeout > 0 && r.Timeout > 0 && rt.PerTryTimeout > r.Timeout) {
			fail(p+".retry.per_try_timeout", "must be between 0 and the route timeout (%s)", r.Timeout)
		}

		if rl := r.RateLimit; rl != nil {
			if rl.RequestsPerSecond <= 0 {
				fail(p+".rate_limit.requests_per_second", "must be positive")
			}
			if rl.Burst < 1 {
				fail(p+".rate_limit.burst", "must be at least 1")
			}
		}
	}
	return errors.Join(errs...)
}

func validateUpstream(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https, got %q", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("missing host in %q", raw)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("query and fragment are not allowed in %q", raw)
	}
	return nil
}
