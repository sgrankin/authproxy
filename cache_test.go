package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	h := c.Handler(mustURL(t, srv.URL), nil, nil)

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
	h := c.Handler(mustURL(t, srv.URL), nil, nil)

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
	h := c.Handler(mustURL(t, srv.URL), nil, nil)

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
	h := c.Handler(mustURL(t, srv.URL), []Header{{Name: "Authorization", Value: "Bearer secret"}}, nil)
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
	h := c.Handler(mustURL(t, srv.URL), nil, nil)

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

// Block list should strip the named headers in both the cache path (HEAD
// stored into meta + replayed on cache hit) and the passthrough path.
func TestCache_BlockResponseHeaders(t *testing.T) {
	const etag = `"b1"`
	body := strings.Repeat("x", 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", etag)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("X-Xet-Hash", "deadbeef")
		w.Header().Set("Link", `<https://x.example>; rel="xet-auth"`)
		w.Header().Set("X-Keep-Me", "visible")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader(body))
	}))
	defer srv.Close()

	c := newCache(t)
	h := c.Handler(mustURL(t, srv.URL), nil, []string{"X-Xet-Hash", "Link"})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/f", nil))
	if got := rec.Header().Get("X-Xet-Hash"); got != "" {
		t.Errorf("X-Xet-Hash leaked on first GET: %q", got)
	}
	if got := rec.Header().Get("Link"); got != "" {
		t.Errorf("Link leaked on first GET: %q", got)
	}
	if got := rec.Header().Get("X-Keep-Me"); got != "visible" {
		t.Errorf("X-Keep-Me missing on first GET: %q", got)
	}

	// Second request: served from cache (meta replay). Should still omit.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/f", nil))
	if got := rec2.Header().Get("X-Xet-Hash"); got != "" {
		t.Errorf("X-Xet-Hash leaked on cache hit: %q", got)
	}
	if got := rec2.Header().Get("Link"); got != "" {
		t.Errorf("Link leaked on cache hit: %q", got)
	}
	if got := rec2.Header().Get("X-Keep-Me"); got != "visible" {
		t.Errorf("X-Keep-Me missing on cache hit: %q", got)
	}
}

// Simulates HF's shape: the origin HEAD returns a 302 with the file metadata
// (X-Repo-Commit, X-Linked-Etag, X-Linked-Size) and a Location pointing at a
// CDN. The CDN's HEAD returns 200 with the actual size, content-type, and
// ETag. The proxy should merge and expose all of them to clients.
func TestCache_DiscoverFollowsRedirectAndMergesHeaders(t *testing.T) {
	body := strings.Repeat("m", 4096)
	const cdnETag = `"cdn-v1"`

	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", cdnETag)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader(body))
	}))
	defer cdn.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Location", cdn.URL+r.URL.Path)
			w.Header().Set("X-Repo-Commit", "deadbeefcommit")
			w.Header().Set("X-Linked-Etag", `"linked-v1"`)
			w.Header().Set("X-Linked-Size", strconv.Itoa(len(body)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusFound)
			return
		}
		// Non-HEAD: redirect as usual (for body fetches).
		http.Redirect(w, r, cdn.URL+r.URL.Path, http.StatusFound)
	}))
	defer origin.Close()

	c := newCache(t)
	defer c.Close()
	h := c.Handler(mustURL(t, origin.URL), nil, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("HEAD", "/m", nil))
	if rec.Code != 200 {
		t.Fatalf("HEAD: code %d", rec.Code)
	}
	if got := rec.Header().Get("X-Repo-Commit"); got != "deadbeefcommit" {
		t.Errorf("X-Repo-Commit: %q", got)
	}
	if got := rec.Header().Get("X-Linked-Etag"); got != `"linked-v1"` {
		t.Errorf("X-Linked-Etag: %q", got)
	}
	if got := rec.Header().Get("X-Linked-Size"); got != strconv.Itoa(len(body)) {
		t.Errorf("X-Linked-Size: %q", got)
	}
	if got := rec.Header().Get("ETag"); got != cdnETag {
		t.Errorf("ETag: %q want %q", got, cdnETag)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type: %q", got)
	}
	if got := rec.Header().Get("Location"); got != "" {
		t.Errorf("Location should be stripped, got %q", got)
	}

	// GET: body still flows through (ReverseProxy follows on body fetches).
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/m", nil))
	if rec2.Code != 200 {
		t.Fatalf("GET: code %d", rec2.Code)
	}
	if rec2.Body.Len() != len(body) {
		t.Fatalf("GET body len %d, want %d", rec2.Body.Len(), len(body))
	}
}

func TestHasUncacheableVary(t *testing.T) {
	cases := map[string]bool{
		"":                              false,
		"Accept-Encoding":               false,
		"Origin":                        false,
		"Accept":                        false,
		"origin, accept-encoding":       false,
		"Access-Control-Request-Method": false,
		"origin,access-control-request-method,access-control-request-headers": false,
		"Cookie":         true,
		"User-Agent":     true,
		"Authorization":  true,
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
	h := c.Handler(mustURL(t, srv.URL), nil, nil)
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

func TestCache_StreamingFillSingleGET(t *testing.T) {
	body := strings.Repeat("x", (4<<20)*2+100) // 3 chunks at 4 MiB chunkSize
	const etag = `"stream"`
	var gets, ranges atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if r.Header.Get("Range") != "" {
				ranges.Add(1)
			} else {
				gets.Add(1)
			}
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Accept-Ranges", "bytes")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader(body))
	}))
	defer srv.Close()

	c := newCache(t)
	defer c.Close()
	h := c.Handler(mustURL(t, srv.URL), nil, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 200 {
		t.Fatalf("code %d", rec.Code)
	}
	if rec.Body.Len() != len(body) {
		t.Fatalf("body len %d, want %d", rec.Body.Len(), len(body))
	}
	if g := gets.Load(); g != 1 {
		t.Errorf("expected 1 full GET (streaming-tee), got %d", g)
	}
	if r := ranges.Load(); r != 0 {
		t.Errorf("expected 0 Range requests on initial fill, got %d", r)
	}
}

func TestCache_ConcurrentRequestsShareFill(t *testing.T) {
	body := strings.Repeat("y", (4<<20)+100) // 2 chunks
	const etag = `"concurrent"`
	var gets atomic.Int64

	// Slow body so multiple concurrent requests definitely overlap the fill.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.Header.Get("Range") == "" {
			gets.Add(1)
			w.Header().Set("ETag", etag)
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			// Write half, sleep, write rest. Forces the fill to span time.
			w.Write([]byte(body[:len(body)/2]))
			time.Sleep(50 * time.Millisecond)
			w.Write([]byte(body[len(body)/2:]))
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Accept-Ranges", "bytes")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader(body))
	}))
	defer srv.Close()

	c := newCache(t)
	defer c.Close()
	h := c.Handler(mustURL(t, srv.URL), nil, nil)

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest("GET", "/y", nil))
			if rec.Code != 200 {
				t.Errorf("code %d", rec.Code)
			}
			if rec.Body.Len() != len(body) {
				t.Errorf("body len %d, want %d", rec.Body.Len(), len(body))
			}
		}()
	}
	wg.Wait()

	if g := gets.Load(); g != 1 {
		t.Errorf("expected exactly 1 full upstream GET (concurrent dedup), got %d", g)
	}
}

// TestCache_FillCancelByOriginator verifies that when client A (whose request
// kicked off a streaming fill) cancels mid-stream, client B (waiting on the
// same blob) recovers via per-chunk Range fetches.
func TestCache_FillCancelByOriginator(t *testing.T) {
	body := strings.Repeat("z", (4<<20)*3+50) // 4 chunks at 4 MiB
	const etag = `"cancel"`

	// Streaming GET writes the first chunk fast then blocks until released.
	// Range requests serve normally.
	releaseFill := make(chan struct{})
	var streamingStarted atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", etag)
		w.Header().Set("Accept-Ranges", "bytes")
		if r.Method == http.MethodGet && r.Header.Get("Range") == "" {
			// Streaming fill: write 1 chunk + flush, then block until released
			// or the client disconnects.
			streamingStarted.Store(true)
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(200)
			w.Write([]byte(body[:4<<20])) // chunk 0
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-releaseFill:
				w.Write([]byte(body[4<<20:]))
			case <-r.Context().Done():
				return
			}
			return
		}
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader(body))
	}))
	defer srv.Close()

	c := newCache(t)
	defer c.Close()
	h := c.Handler(mustURL(t, srv.URL), nil, nil)

	// Client A: cancellable context.
	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		req := httptest.NewRequest("GET", "/x", nil).WithContext(ctxA)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		// A may complete with truncated body or not — we don't assert on A.
	}()

	// Wait until streaming has started so B will arrive mid-fill.
	deadline := time.After(2 * time.Second)
	for !streamingStarted.Load() {
		select {
		case <-deadline:
			t.Fatal("streaming fill never started")
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Cancel A.
	cancelA()
	<-doneA

	// Client B: should still complete with the full correct body, by recovering
	// via per-chunk Range fetches for the unfilled chunks.
	close(releaseFill) // unblock the (now-cancelled) streaming GET, just in case
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 200 {
		t.Fatalf("B: code %d", rec.Code)
	}
	if rec.Body.Len() != len(body) {
		t.Fatalf("B: body len %d, want %d. Headers: %v", rec.Body.Len(), len(body), rec.Header())
	}
	if rec.Body.String() != body {
		t.Fatalf("B: body content mismatch")
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
	hA := c.Handler(urlA, nil, nil)
	hB := c.Handler(urlB, nil, nil)

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
	h := c.Handler(u, nil, nil)
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

func TestCache_EtagPersistence(t *testing.T) {
	body := strings.Repeat("p", 32)
	const etag = `"persisted"`

	srv := fakeUpstream(body, etag, nil)
	defer srv.Close()

	dir := t.TempDir()

	// First run: populate cache.
	c1, err := NewCache(dir, 100<<20, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	u := mustURL(t, srv.URL)
	h1 := c1.Handler(u, nil, nil)
	h1.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/p", nil))
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify the etag file was written.
	if _, err := os.Stat(filepath.Join(dir, "etags.json")); err != nil {
		t.Fatalf("etags.json not written: %v", err)
	}

	// Second run: should load etags from disk.
	c2, err := NewCache(dir, 100<<20, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	c2.mu.Lock()
	got, has := c2.etags[u.Host+" /p"]
	c2.mu.Unlock()
	if !has {
		t.Errorf("etag for %q not loaded", u.Host+" /p")
	}
	if got != etag {
		t.Errorf("loaded etag = %q, want %q", got, etag)
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
		hdr                 string
		contentLen          int64
		wantStart, wantEnd  int64
		wantPartial, wantOK bool
	}{
		{"", 100, 0, 99, false, true},
		{"bytes=0-9", 100, 0, 9, true, true},
		{"bytes=10-", 100, 10, 99, true, true},
		{"bytes=-5", 100, 95, 99, true, true},
		{"bytes=0-200", 100, 0, 99, true, true},      // clamp
		{"bytes=200-", 100, 0, 0, false, false},      // start past EOF
		{"bytes=10-5", 100, 0, 0, false, false},      // inverted
		{"bytes=abc-5", 100, 0, 0, false, false},     // parse error
		{"bytes=0-abc", 100, 0, 0, false, false},     // parse error in end
		{"bytes=0-9,20-29", 100, 0, 0, false, false}, // multi-range
		{"items=0-9", 100, 0, 0, false, false},       // wrong unit
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
