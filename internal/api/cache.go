package api

import (
	"net/http"
	"sync"
	"time"
)

// responseCache is a small TTL cache for high-frequency GET endpoints whose
// data refresh rate is bounded by the underlying bd/gt CLI cost (1-3s per
// call, mostly bd -> Dolt round-trips with no connection pool). The web UI
// polls every 5s, so a 1500ms TTL absorbs polling bursts while keeping
// observed staleness imperceptible.
//
// This is a SYMPTOM-LAYER fix for hq-f95. The root cause is the lack of
// connection pooling in bd -> Dolt. Cleaner long-term: fix the pool in
// the bd binary (upstream gastownhall/beads). Cache stays as belt-and-
// suspenders.
//
// Doctrine:
//   - Cache is invalidated by time only, never by mutation. The viewer is
//     READ-ONLY per the gvid contract; all writes happen via bd/gt CLIs
//     outside this process. Therefore mutation-invalidation is impossible
//     by design, and a pure TTL cache is correct.
//   - 503 / error responses MUST NOT be cached. Health degradation should
//     not stick.
//   - Each cache key is the path + raw query. Different ?filter= params
//     get separate slots.
//
// Origin: hq-f95 in beads, verb 'build gvid perf cache' 2026-05-13.

type cachedResponse struct {
	expiresAt time.Time
	status    int
	body      []byte
	headers   http.Header
}

type responseCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]cachedResponse
}

func newResponseCache(ttl time.Duration) *responseCache {
	return &responseCache{
		ttl:     ttl,
		entries: make(map[string]cachedResponse),
	}
}

func (c *responseCache) key(r *http.Request) string {
	return r.URL.Path + "?" + r.URL.RawQuery
}

func (c *responseCache) get(r *http.Request) (cachedResponse, bool) {
	if c == nil {
		return cachedResponse{}, false
	}
	c.mu.RLock()
	entry, ok := c.entries[c.key(r)]
	c.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		return cachedResponse{}, false
	}
	return entry, true
}

func (c *responseCache) set(r *http.Request, status int, headers http.Header, body []byte) {
	if c == nil || status < 200 || status >= 300 {
		return
	}
	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	hdrCopy := make(http.Header, len(headers))
	for k, v := range headers {
		hdrCopy[k] = append([]string(nil), v...)
	}
	c.mu.Lock()
	c.entries[c.key(r)] = cachedResponse{
		expiresAt: time.Now().Add(c.ttl),
		status:    status,
		body:      bodyCopy,
		headers:   hdrCopy,
	}
	c.mu.Unlock()
}

// captureWriter wraps http.ResponseWriter to record status, headers, and body
// so the cache layer can store the served response after the handler runs.
type captureWriter struct {
	http.ResponseWriter
	status int
	body   []byte
	wrote  bool
}

func (cw *captureWriter) WriteHeader(code int) {
	cw.status = code
	cw.wrote = true
	cw.ResponseWriter.WriteHeader(code)
}

func (cw *captureWriter) Write(b []byte) (int, error) {
	if !cw.wrote {
		cw.status = http.StatusOK
		cw.wrote = true
	}
	cw.body = append(cw.body, b...)
	return cw.ResponseWriter.Write(b)
}

// withCache wraps an http.HandlerFunc with cache lookup + store. Cached
// payloads serve in microseconds; misses fall through to the underlying
// handler and populate the cache for the next caller within the TTL window.
func (s *Server) withCache(cache *responseCache, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if entry, ok := cache.get(r); ok {
			for k, v := range entry.headers {
				w.Header()[k] = v
			}
			w.Header().Set("X-Cache", "HIT")
			w.WriteHeader(entry.status)
			_, _ = w.Write(entry.body)
			return
		}
		cw := &captureWriter{ResponseWriter: w}
		w.Header().Set("X-Cache", "MISS")
		h(cw, r)
		if cw.status == 0 {
			cw.status = http.StatusOK
		}
		cache.set(r, cw.status, w.Header(), cw.body)
	}
}
