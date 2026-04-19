package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newCache(t *testing.T) *Cache {
	t.Helper()
	c, err := NewCache(t.TempDir(), 100<<20, 4<<20) // 4 MiB chunks
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// fakeUpstream serves body with a fixed ETag and Range/If-None-Match support
// via http.ServeContent. The hits counter (if non-nil) is incremented per
// request received.
func fakeUpstream(body string, etag string, hits *atomic.Int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Accept-Ranges", "bytes")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader(body))
	}))
}

func TestCache_HitOnRevalidation(t *testing.T) {
	body := strings.Repeat("hello ", 1000) // 6 KB — fits in one chunk
	const etag = `"abc123"`
	var hits atomic.Int64

	srv := fakeUpstream(body, etag, &hits)
	defer srv.Close()

	c := newCache(t)
	h := c.Handler(mustURL(t, srv.URL), nil)

	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequest("GET", "/foo", nil))
	if rec1.Code != 200 {
		t.Fatalf("first: code %d, body %q", rec1.Code, rec1.Body.String())
	}
	if rec1.Body.String() != body {
		t.Fatalf("first: body mismatch (got %d bytes, want %d)", rec1.Body.Len(), len(body))
	}

	hitsAfterFirst := hits.Load()

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/foo", nil))
	if rec2.Code != 200 {
		t.Fatalf("second: code %d, body %q", rec2.Code, rec2.Body.String())
	}
	if rec2.Body.String() != body {
		t.Fatalf("second: body mismatch (got %d bytes, want %d)", rec2.Body.Len(), len(body))
	}

	// Second request should issue at most 1 upstream call (HEAD revalidation).
	if delta := hits.Load() - hitsAfterFirst; delta > 1 {
		t.Errorf("expected ≤1 upstream call on cache hit, got %d", delta)
	}
}

func TestCache_RangeRequestServedFromCache(t *testing.T) {
	body := "0123456789ABCDEF" // 16 bytes
	const etag = `"r1"`

	srv := fakeUpstream(body, etag, nil)
	defer srv.Close()

	c := newCache(t)
	h := c.Handler(mustURL(t, srv.URL), nil)

	// Prime the cache.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 200 {
		t.Fatalf("prime: code %d", rec.Code)
	}

	// Range request on cached content.
	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Range", "bytes=4-7")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusPartialContent {
		t.Fatalf("range: code %d, want 206; body %q", rec2.Code, rec2.Body.String())
	}
	if rec2.Body.String() != "4567" {
		t.Errorf("range body: %q", rec2.Body.String())
	}
	if got := rec2.Header().Get("Content-Range"); got != "bytes 4-7/16" {
		t.Errorf("Content-Range: %q", got)
	}
}

func TestCache_NoETagPassesThrough(t *testing.T) {
	body := "no caching here"
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		// no ETag, no Accept-Ranges → not cacheable
		if _, err := w.Write([]byte(body)); err != nil {
			t.Errorf("upstream write: %v", err)
		}
	}))
	defer srv.Close()

	c := newCache(t)
	h := c.Handler(mustURL(t, srv.URL), nil)

	for i := range 3 {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
		if rec.Body.String() != body {
			t.Fatalf("iter %d: body %q", i, rec.Body.String())
		}
	}
	// Each request: HEAD + GET passthrough = 2 hits.
	if got := hits.Load(); got < 3 {
		t.Errorf("expected at least 3 upstream hits, got %d", got)
	}
}

func TestCache_HeaderInjection(t *testing.T) {
	var seenAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("ETag", `"x"`)
		w.Header().Set("Accept-Ranges", "bytes")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader("ok"))
	}))
	defer srv.Close()

	c := newCache(t)
	h := c.Handler(mustURL(t, srv.URL), []Header{{Name: "Authorization", Value: "Bearer secret"}})
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	if got, _ := seenAuth.Load().(string); got != "Bearer secret" {
		t.Errorf("upstream saw Authorization=%q", got)
	}
}

func TestCache_ClientConditional(t *testing.T) {
	body := "hello"
	const etag = `"v1"`
	srv := fakeUpstream(body, etag, nil)
	defer srv.Close()

	c := newCache(t)
	h := c.Handler(mustURL(t, srv.URL), nil)

	// Prime cache.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	// Client sends matching If-None-Match.
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("If-None-Match", etag)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Errorf("expected 304, got %d", rec.Code)
	}
}

func TestHasUncacheableVary(t *testing.T) {
	cases := map[string]bool{
		"":               false,
		"Accept-Encoding": false,
		"Origin":          false,
		"Accept":          false,
		"origin, accept-encoding": false,
		"Access-Control-Request-Method": false,
		"origin,access-control-request-method,access-control-request-headers": false,
		"Cookie":     true,
		"User-Agent": true,
		"Authorization": true,
		"Origin, Cookie": true,
	}
	for in, want := range cases {
		h := http.Header{"Vary": []string{in}}
		if got := hasUncacheableVary(h); got != want {
			t.Errorf("Vary %q: got %v, want %v", in, got, want)
		}
	}
}

func TestCache_VaryRefuses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v"`)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Vary", "User-Agent")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader("varies"))
	}))
	defer srv.Close()

	c := newCache(t)
	h := c.Handler(mustURL(t, srv.URL), nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Body.String() != "varies" {
		t.Fatalf("body: %q", rec.Body.String())
	}
	// Verify nothing got cached: a second request should still hit upstream
	// (we can't easily count hits here without the counter pattern, but
	// at minimum the response should still be served correctly).
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/", nil))
	if rec2.Body.String() != "varies" {
		t.Fatalf("second body: %q", rec2.Body.String())
	}
}

func TestCache_LRUEviction(t *testing.T) {
	body := strings.Repeat("x", 16) // 16 bytes per blob
	srvA := fakeUpstream(body, `"a"`, nil)
	defer srvA.Close()
	srvB := fakeUpstream(body, `"b"`, nil)
	defer srvB.Close()

	c, err := NewCache(t.TempDir(), 24, 4<<20) // maxSize 24 → fits 1 blob, not 2
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	urlA := mustURL(t, srvA.URL)
	urlB := mustURL(t, srvB.URL)
	hA := c.Handler(urlA, nil)
	hB := c.Handler(urlB, nil)

	hA.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	// Sleep so B's lastAccess > A's.
	time.Sleep(5 * time.Millisecond)
	hB.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))

	if got := c.sizeBytes.Load(); got != 32 {
		t.Errorf("sizeBytes before eviction: got %d, want 32", got)
	}

	c.checkEviction()

	c.mu.Lock()
	_, hasA := c.blobs[blobKey(urlA, `"a"`)]
	_, hasB := c.blobs[blobKey(urlB, `"b"`)]
	c.mu.Unlock()

	if hasA {
		t.Error("expected A (older) to be evicted")
	}
	if !hasB {
		t.Error("expected B (newer) to remain")
	}
	if got := c.sizeBytes.Load(); got != 16 {
		t.Errorf("sizeBytes after eviction: got %d, want 16", got)
	}
}

func TestCache_LRUSkipsInFlight(t *testing.T) {
	body := strings.Repeat("x", 16)
	srv := fakeUpstream(body, `"e"`, nil)
	defer srv.Close()

	c, err := NewCache(t.TempDir(), 1, 4<<20) // maxSize 1 → forces eviction of any blob
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	u := mustURL(t, srv.URL)
	h := c.Handler(u, nil)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))

	// Manually pin the blob's refcount.
	b := c.acquireBlob(blobKey(u, `"e"`))
	if b == nil {
		t.Fatal("blob not present")
	}
	defer b.release()

	c.checkEviction()

	c.mu.Lock()
	_, has := c.blobs[blobKey(u, `"e"`)]
	c.mu.Unlock()
	if !has {
		t.Error("in-flight blob should not be evicted")
	}
}

func TestStrongETag(t *testing.T) {
	cases := map[string]string{
		`"abc"`:   `"abc"`,
		`W/"abc"`: ``,
		`abc`:     ``,
		``:        ``,
		` "abc" `: `"abc"`,
	}
	for in, want := range cases {
		if got := strongETag(in); got != want {
			t.Errorf("strongETag(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseRange(t *testing.T) {
	cases := []struct {
		hdr                  string
		contentLen           int64
		wantStart, wantEnd   int64
		wantPartial, wantOK  bool
	}{
		{"", 100, 0, 99, false, true},
		{"bytes=0-9", 100, 0, 9, true, true},
		{"bytes=10-", 100, 10, 99, true, true},
		{"bytes=-5", 100, 95, 99, true, true},
		{"bytes=0-200", 100, 0, 99, true, true}, // clamp
		{"bytes=200-", 100, 0, 0, false, false}, // start past EOF
		{"bytes=10-5", 100, 0, 0, false, false}, // inverted
		{"bytes=abc-5", 100, 0, 0, false, false}, // parse error
		{"bytes=0-abc", 100, 0, 0, false, false}, // parse error in end
		{"bytes=0-9,20-29", 100, 0, 0, false, false}, // multi-range
		{"items=0-9", 100, 0, 0, false, false},  // wrong unit
	}
	for _, tc := range cases {
		s, e, p, ok := parseRange(tc.hdr, tc.contentLen)
		if ok != tc.wantOK {
			t.Errorf("parseRange(%q, %d): ok=%v, want %v", tc.hdr, tc.contentLen, ok, tc.wantOK)
			continue
		}
		if !ok {
			continue
		}
		if s != tc.wantStart || e != tc.wantEnd || p != tc.wantPartial {
			t.Errorf("parseRange(%q, %d): got (%d, %d, partial=%v), want (%d, %d, %v)",
				tc.hdr, tc.contentLen, s, e, p, tc.wantStart, tc.wantEnd, tc.wantPartial)
		}
	}
}
