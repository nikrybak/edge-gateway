// Package router matches requests to routes by host and path prefix.
package router

import (
	"errors"
	"net"
	"sort"
	"strings"
)

// ErrNoRoute is returned by Match when no route accepts the request.
var ErrNoRoute = errors.New("no route matched")

// Router is an immutable host and path-prefix matcher. Build one with a Builder; the
// result is safe for concurrent use without locking and is replaced wholesale on
// config reload.
//
// Matching precedence: an exact host beats a wildcard host ("*.example.com"), which
// beats a route without a host. Within the chosen host group the longest path prefix
// wins. The more specific host wins even when a less specific one has a longer
// prefix, because host is what operators use to separate tenants.
type Router[T any] struct {
	exact    map[string][]entry[T]
	wildcard []wildcardGroup[T]
	any      []entry[T]
}

type entry[T any] struct {
	prefix string
	value  T
}

type wildcardGroup[T any] struct {
	suffix  string // ".example.com"
	entries []entry[T]
}

// Builder collects routes for a Router.
type Builder[T any] struct {
	exact    map[string][]entry[T]
	wildcard map[string][]entry[T]
	any      []entry[T]
}

// NewBuilder returns an empty Builder.
func NewBuilder[T any]() *Builder[T] {
	return &Builder[T]{exact: map[string][]entry[T]{}, wildcard: map[string][]entry[T]{}}
}

// Add registers value for host and path prefix. An empty host matches any host,
// "*.example.com" matches any subdomain of example.com (but not example.com itself).
// A prefix without a trailing slash matches on segment boundaries: "/api" matches
// "/api" and "/api/users" but not "/apiary".
func (b *Builder[T]) Add(host, prefix string, value T) {
	host = strings.ToLower(host)
	e := entry[T]{prefix: prefix, value: value}
	switch {
	case host == "":
		b.any = append(b.any, e)
	case strings.HasPrefix(host, "*."):
		b.wildcard[host[1:]] = append(b.wildcard[host[1:]], e)
	default:
		b.exact[host] = append(b.exact[host], e)
	}
}

// Build returns the immutable Router.
func (b *Builder[T]) Build() *Router[T] {
	r := &Router[T]{exact: make(map[string][]entry[T], len(b.exact)), any: sortEntries(b.any)}
	for h, es := range b.exact {
		r.exact[h] = sortEntries(es)
	}
	for suffix, es := range b.wildcard {
		r.wildcard = append(r.wildcard, wildcardGroup[T]{suffix: suffix, entries: sortEntries(es)})
	}
	// Longer suffixes are more specific: *.eu.example.com before *.example.com.
	sort.Slice(r.wildcard, func(i, j int) bool { return len(r.wildcard[i].suffix) > len(r.wildcard[j].suffix) })
	return r
}

func sortEntries[T any](es []entry[T]) []entry[T] {
	out := append([]entry[T](nil), es...)
	sort.SliceStable(out, func(i, j int) bool { return len(out[i].prefix) > len(out[j].prefix) })
	return out
}

// Match returns the value of the best route for host and path, together with the
// matched prefix that a caller may strip. host may include a port.
func (r *Router[T]) Match(host, path string) (value T, prefix string, err error) {
	host = normalizeHost(host)
	if es, ok := r.exact[host]; ok {
		if e, ok := matchPath(es, path); ok {
			return e.value, e.prefix, nil
		}
	}
	for _, g := range r.wildcard {
		if len(host) > len(g.suffix) && strings.HasSuffix(host, g.suffix) {
			if e, ok := matchPath(g.entries, path); ok {
				return e.value, e.prefix, nil
			}
		}
	}
	if e, ok := matchPath(r.any, path); ok {
		return e.value, e.prefix, nil
	}
	var zero T
	return zero, "", ErrNoRoute
}

func matchPath[T any](es []entry[T], path string) (entry[T], bool) {
	for _, e := range es {
		if hasPathPrefix(path, e.prefix) {
			return e, true
		}
	}
	return entry[T]{}, false
}

func hasPathPrefix(path, prefix string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if len(path) == len(prefix) || strings.HasSuffix(prefix, "/") {
		return true
	}
	return path[len(prefix)] == '/'
}

// normalizeHost strips the port and lowercases without allocating in the common
// case where the host is already lowercase.
func normalizeHost(host string) string {
	if i := strings.LastIndexByte(host, ':'); i >= 0 && !strings.HasSuffix(host, "]") {
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
	}
	for i := 0; i < len(host); i++ {
		if c := host[i]; 'A' <= c && c <= 'Z' {
			return strings.ToLower(host)
		}
	}
	return host
}

// Rewrite applies prefix stripping and prefix addition to path. It is exported so the
// proxy and tests share one definition of the rewrite semantics.
func Rewrite(path, matched string, strip bool, add string) string {
	if strip {
		path = strings.TrimPrefix(path, strings.TrimSuffix(matched, "/"))
		if path == "" || path[0] != '/' {
			path = "/" + path
		}
	}
	if add != "" {
		path = strings.TrimSuffix(add, "/") + path
	}
	return path
}
