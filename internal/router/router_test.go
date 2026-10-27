package router

import (
	"errors"
	"fmt"
	"testing"
)

func testRouter() *Router[string] {
	b := NewBuilder[string]()
	b.Add("", "/", "catch-all")
	b.Add("", "/api", "api")
	b.Add("", "/api/v2/", "api-v2")
	b.Add("api.example.com", "/", "api-host")
	b.Add("api.example.com", "/admin", "api-host-admin")
	b.Add("*.example.com", "/", "wildcard")
	b.Add("*.eu.example.com", "/", "wildcard-eu")
	return b.Build()
}

func TestMatch(t *testing.T) {
	r := testRouter()
	tests := []struct {
		host, path string
		want       string
		wantPrefix string
	}{
		{"other.org", "/", "catch-all", "/"},
		{"other.org", "/api", "api", "/api"},
		{"other.org", "/api/users", "api", "/api"},
		{"other.org", "/apiary", "catch-all", "/"},
		{"other.org", "/api/v2/users", "api-v2", "/api/v2/"},
		{"other.org", "/api/v2", "api", "/api"},
		{"api.example.com", "/admin/x", "api-host-admin", "/admin"},
		{"API.Example.com:8443", "/", "api-host", "/"},
		{"www.example.com", "/api/users", "wildcard", "/"},
		{"x.eu.example.com", "/", "wildcard-eu", "/"},
		{"example.com", "/api", "api", "/api"},
		{"[::1]:8080", "/", "catch-all", "/"},
	}
	for _, tt := range tests {
		t.Run(tt.host+tt.path, func(t *testing.T) {
			got, prefix, err := r.Match(tt.host, tt.path)
			if err != nil {
				t.Fatalf("Match: %v", err)
			}
			if got != tt.want || prefix != tt.wantPrefix {
				t.Fatalf("got (%q, %q), want (%q, %q)", got, prefix, tt.want, tt.wantPrefix)
			}
		})
	}
}

func TestMatchNoRoute(t *testing.T) {
	b := NewBuilder[int]()
	b.Add("only.example.com", "/v1/", 1)
	r := b.Build()
	for _, c := range [][2]string{{"only.example.com", "/v2"}, {"other.example.com", "/v1/x"}} {
		if _, _, err := r.Match(c[0], c[1]); !errors.Is(err, ErrNoRoute) {
			t.Errorf("Match(%q, %q) err = %v, want ErrNoRoute", c[0], c[1], err)
		}
	}
}

func TestRewrite(t *testing.T) {
	tests := []struct {
		path, matched string
		strip         bool
		add, want     string
	}{
		{"/api/users", "/api/", true, "", "/users"},
		{"/api/users", "/api", true, "", "/users"},
		{"/api", "/api", true, "", "/"},
		{"/api/", "/api/", true, "", "/"},
		{"/api/users", "/api", false, "", "/api/users"},
		{"/api/users", "/api", true, "/v2", "/v2/users"},
		{"/users", "/", true, "/v2/", "/v2/users"},
		{"/users", "/", false, "/internal", "/internal/users"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("%s|%s|%t|%s", tt.path, tt.matched, tt.strip, tt.add)
		if got := Rewrite(tt.path, tt.matched, tt.strip, tt.add); got != tt.want {
			t.Errorf("%s: got %q, want %q", name, got, tt.want)
		}
	}
}

func BenchmarkMatch(b *testing.B) {
	bld := NewBuilder[int]()
	for i := range 50 {
		bld.Add(fmt.Sprintf("svc%d.example.com", i), "/", i)
		bld.Add("", fmt.Sprintf("/svc%d/v1/", i), i)
	}
	bld.Add("*.example.com", "/", -1)
	r := bld.Build()

	cases := []struct{ name, host, path string }{
		{"exact-host", "svc42.example.com", "/orders/1"},
		{"wildcard-host", "unknown.example.com", "/orders/1"},
		{"path-prefix", "gateway.local:8080", "/svc25/v1/orders"},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := r.Match(c.host, c.path); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
