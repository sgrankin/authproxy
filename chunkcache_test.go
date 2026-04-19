package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestXetChunkKey(t *testing.T) {
	cases := []struct {
		host, path, q string
		wantOK        bool
	}{
		{"transfer.xethub.hf.co", "/xorbs/default/abc", "X-Xet-Signed-Range=bytes%3D0-100&Signature=xyz", true},
		{"cas-bridge.xethub.hf.co", "/xet-bridge-us/file/chunk", "X-Xet-Signed-Range=bytes%3D0-50", true},
		// Non-xorb path: not cacheable.
		{"cas-server.xethub.hf.co", "/v1/reconstructions/hash", "", false},
		// Missing signed-range: not cacheable.
		{"transfer.xethub.hf.co", "/xorbs/default/abc", "X-Other=1", false},
	}
	for _, c := range cases {
		_, ok := xetChunkKey(c.host, c.path, c.q)
		if ok != c.wantOK {
			t.Errorf("xetChunkKey(%q, %q, %q) ok=%v, want %v", c.host, c.path, c.q, ok, c.wantOK)
		}
	}

	// Same inputs produce same key; different signed-ranges produce different keys.
	k1, _ := xetChunkKey("transfer.xethub.hf.co", "/xorbs/default/abc", "X-Xet-Signed-Range=bytes%3D0-100&Signature=s1")
	k2, _ := xetChunkKey("transfer.xethub.hf.co", "/xorbs/default/abc", "X-Xet-Signed-Range=bytes%3D0-100&Signature=s2")
	k3, _ := xetChunkKey("transfer.xethub.hf.co", "/xorbs/default/abc", "X-Xet-Signed-Range=bytes%3D0-50&Signature=s1")
	if k1 != k2 {
		t.Error("keys should match when only signature query param differs")
	}
	if k1 == k3 {
		t.Error("keys should differ when signed-range differs")
	}
}

// Chunk cache hit should serve the cached bytes without hitting upstream.
func TestChunkCache_HitSkipsUpstream(t *testing.T) {
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Range", "bytes 0-4/5")
		w.WriteHeader(http.StatusPartialContent)
		io.WriteString(w, "hello")
	}))
	defer upstream.Close()

	cc, err := newChunkCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	fetch := func() (*http.Response, error) {
		req, _ := http.NewRequest("GET", upstream.URL, nil)
		return http.DefaultClient.Do(req)
	}

	// First call: miss, fetches upstream, caches.
	rec1 := httptest.NewRecorder()
	cc.serve(rec1, httptest.NewRequest("GET", "/x", nil), "testkey-abcdef", fetch)
	if rec1.Code != http.StatusPartialContent {
		t.Fatalf("first: code %d", rec1.Code)
	}
	if rec1.Body.String() != "hello" {
		t.Fatalf("first: body %q", rec1.Body.String())
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("first: expected 1 upstream hit, got %d", got)
	}

	// Second call: cache hit, no upstream.
	rec2 := httptest.NewRecorder()
	cc.serve(rec2, httptest.NewRequest("GET", "/x", nil), "testkey-abcdef", fetch)
	if rec2.Code != http.StatusPartialContent {
		t.Fatalf("second: code %d", rec2.Code)
	}
	if rec2.Body.String() != "hello" {
		t.Fatalf("second: body %q", rec2.Body.String())
	}
	if got := rec2.Header().Get("Content-Range"); got != "bytes 0-4/5" {
		t.Errorf("second: Content-Range %q", got)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("second: expected cache hit, got %d upstream hits", got)
	}
}

// Concurrent callers for the same key should dedup to a single upstream fetch.
func TestChunkCache_SingleflightDedup(t *testing.T) {
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		time.Sleep(30 * time.Millisecond) // hold the lock
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "dedupe")
	}))
	defer upstream.Close()

	cc, err := newChunkCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fetch := func() (*http.Response, error) {
		req, _ := http.NewRequest("GET", upstream.URL, nil)
		return http.DefaultClient.Do(req)
	}

	done := make(chan string, 3)
	for range 3 {
		go func() {
			rec := httptest.NewRecorder()
			cc.serve(rec, httptest.NewRequest("GET", "/x", nil), "sf-key", fetch)
			done <- rec.Body.String()
		}()
	}
	for range 3 {
		if s := <-done; s != "dedupe" {
			t.Errorf("body=%q", s)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("expected 1 upstream fetch (concurrent dedup), got %d", got)
	}
}

// Non-200/206 responses are not cached: the next call re-fetches.
func TestChunkCache_NonSuccessNotCached(t *testing.T) {
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "nope")
	}))
	defer upstream.Close()

	cc, err := newChunkCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fetch := func() (*http.Response, error) {
		return http.DefaultClient.Do(httptest.NewRequest("GET", upstream.URL, nil).WithContext(httptest.NewRequest("GET", "/", nil).Context()))
	}
	_ = fetch
	simpleFetch := func() (*http.Response, error) {
		req, _ := http.NewRequest("GET", upstream.URL, nil)
		return http.DefaultClient.Do(req)
	}

	rec1 := httptest.NewRecorder()
	cc.serve(rec1, httptest.NewRequest("GET", "/x", nil), "err-key", simpleFetch)
	if rec1.Code != 404 || !strings.Contains(rec1.Body.String(), "nope") {
		t.Fatalf("first: code %d body %q", rec1.Code, rec1.Body.String())
	}

	rec2 := httptest.NewRecorder()
	cc.serve(rec2, httptest.NewRequest("GET", "/x", nil), "err-key", simpleFetch)
	if rec2.Code != 404 {
		t.Fatalf("second: code %d", rec2.Code)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("expected 2 upstream hits (no caching on error), got %d", got)
	}
}
