package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Cache is a sliced/chunked HTTP cache. Each cached response is stored as a
// directory of fixed-size chunk files plus a meta sidecar. Chunks are fetched
// on demand via Range requests against the upstream, and concurrent requests
// for the same chunk are deduplicated via per-chunk singleflight.
type Cache struct {
	dir        string
	chunkSize  int64
	maxSize    int64
	client     *http.Client // follows redirects (for body GETs)
	headClient *http.Client // returns the first response (for discover HEADs)

	mu    sync.Mutex
	blobs map[string]*blob  // blobKey → blob
	etags map[string]string // requestKey → last-known strong ETag

	sizeBytes atomic.Int64 // sum of bytes across all complete chunks of all blobs

	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

const (
	evictionInterval  = time.Minute
	etagFlushInterval = 30 * time.Second
	etagsFile         = "etags.json"
)

// NewCache opens (or creates) a cache rooted at dir.
func NewCache(dir string, maxSize, chunkSize int64) (*Cache, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be positive")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("cache mkdir: %w", err)
	}
	transport := &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
	}
	c := &Cache{
		dir:       dir,
		chunkSize: chunkSize,
		maxSize:   maxSize,
		blobs:     map[string]*blob{},
		etags:     map[string]string{},
		closeCh:   make(chan struct{}),
		client:    &http.Client{Transport: transport},
		headClient: &http.Client{
			Transport: transport,
			// discover() needs to see each upstream response as-is — following
			// 3xx would swallow HF's HEAD-time headers (X-Repo-Commit,
			// X-Linked-Etag, …) that live on the redirect, not the final CDN
			// 200. discover handles redirects manually.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	if err := c.load(); err != nil {
		return nil, err
	}
	c.loadEtags()
	if maxSize > 0 {
		c.wg.Add(1)
		go c.evictLoop()
	}
	c.wg.Add(1)
	go c.etagFlushLoop()
	return c, nil
}

// Close stops background goroutines and flushes the etag map. Safe to call
// multiple times. Callers should shut down any HTTP servers using this Cache
// first; in-flight requests that mutate c.etags after Close are silently
// dropped (no panic, but their work is lost).
func (c *Cache) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeCh)
	})
	c.wg.Wait()
	c.flushEtags() // final flush after background loop has stopped
	return nil
}

func (c *Cache) evictLoop() {
	defer c.wg.Done()
	t := time.NewTicker(evictionInterval)
	defer t.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-t.C:
			c.checkEviction()
		}
	}
}

func (c *Cache) etagFlushLoop() {
	defer c.wg.Done()
	t := time.NewTicker(etagFlushInterval)
	defer t.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-t.C:
			c.flushEtags()
		}
	}
}

// loadEtags reads the persisted URL→ETag map from disk into c.etags. Best
// effort: on parse error, the map is left empty and we'll repopulate as
// requests arrive.
func (c *Cache) loadEtags() {
	f, err := os.Open(filepath.Join(c.dir, etagsFile))
	if err != nil {
		return // missing file is normal on first run
	}
	defer f.Close()
	loaded := map[string]string{}
	if err := json.NewDecoder(f).Decode(&loaded); err != nil {
		log.Printf("cache: decode %s: %v", etagsFile, err)
		return
	}
	// Drop entries whose blob isn't present (might have been evicted between runs).
	c.mu.Lock()
	defer c.mu.Unlock()
	for rk, etag := range loaded {
		host, _, ok := strings.Cut(rk, " ")
		if !ok {
			continue
		}
		if _, exists := c.blobs[blobKeyFromHost(host, etag)]; exists {
			c.etags[rk] = etag
		}
	}
}

// flushEtags writes the current URL→ETag map to disk, omitting entries whose
// blob is no longer present. The snapshot/write is intentionally lossy:
// entries added between the c.mu.Unlock and the disk write are not in the
// persisted file but remain in memory until the next flush. Acceptable since
// missing an etag entry just costs one full fetch on cold start.
func (c *Cache) flushEtags() {
	c.mu.Lock()
	snap := make(map[string]string, len(c.etags))
	for rk, etag := range c.etags {
		host, _, ok := strings.Cut(rk, " ")
		if !ok {
			continue
		}
		if _, exists := c.blobs[blobKeyFromHost(host, etag)]; exists {
			snap[rk] = etag
		}
	}
	c.mu.Unlock()
	path := filepath.Join(c.dir, etagsFile)
	if err := writeJSONAtomic(path, snap); err != nil {
		log.Printf("cache: flush %s: %v", etagsFile, err)
	}
}

// checkEviction removes least-recently-accessed blobs until total size is
// under maxSize. Blobs with active reads/fills (inFlight > 0) are skipped.
func (c *Cache) checkEviction() {
	if c.sizeBytes.Load() <= c.maxSize {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	type cand struct {
		b  *blob
		ts int64
	}
	cands := make([]cand, 0, len(c.blobs))
	for _, b := range c.blobs {
		if b.inFlight.Load() == 0 {
			cands = append(cands, cand{b, b.lastAccess.Load()})
		}
	}
	// Sort by lastAccess ascending (oldest first). Small N expected; bubble-style sort is fine.
	for i := 1; i < len(cands); i++ {
		for j := i; j > 0 && cands[j-1].ts > cands[j].ts; j-- {
			cands[j-1], cands[j] = cands[j], cands[j-1]
		}
	}
	for _, cd := range cands {
		if c.sizeBytes.Load() <= c.maxSize {
			break
		}
		b := cd.b
		// Re-check refcount under cache lock — eviction holds c.mu, and
		// acquireBlob bumps refcount under c.mu, so this is race-free.
		if b.inFlight.Load() > 0 {
			continue
		}
		delete(c.blobs, b.key)
		freed := b.bytes.Load()
		if err := os.RemoveAll(b.dir); err != nil {
			log.Printf("cache: evict %s: %v", b.key, err)
			continue
		}
		c.sizeBytes.Add(-freed)
		log.Printf("cache: evicted %s (%d bytes)", b.key, freed)
	}
}

func (c *Cache) load() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		blobDir := filepath.Join(c.dir, e.Name())
		b, err := loadBlob(blobDir, c)
		if err != nil {
			// Failed-to-load blob dirs are unrecoverable (corrupt meta,
			// missing files, etc.) and don't contribute to sizeBytes — so
			// LRU eviction can't reclaim them. Reap on startup.
			log.Printf("cache: removing unloadable blob dir %s: %v", e.Name(), err)
			if rerr := os.RemoveAll(blobDir); rerr != nil {
				log.Printf("cache: remove %s: %v", e.Name(), rerr)
			}
			continue
		}
		c.blobs[e.Name()] = b
	}
	return nil
}

// ModifyResponse is an optional hook invoked on each upstream response
// (HEAD or passthrough GET) before the headers are cached or the body is
// streamed. Adapters use it to rewrite protocol-specific headers (e.g. xet
// endpoints) or reconstruction bodies.
type ModifyResponse func(*http.Response) error

func (c *Cache) Handler(upstream *url.URL, headers []Header, blockResponseHeaders []string, modify ModifyResponse) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.serve(w, r, upstream, headers, blockResponseHeaders, modify)
	})
}

// requestKey identifies a particular client-facing URL on a particular service.
// Service identity must be in the key so two services sharing a Cache instance
// can't poison each other's URL→ETag map.
func requestKeyFor(upstream *url.URL, r *http.Request) string {
	return upstream.Host + " " + r.URL.RequestURI()
}

// blobKey hashes upstream identity + ETag so two services that happen to have
// matching ETag values (e.g. CDN-style sequential or shared mirror artifacts)
// don't collide on the body cache.
func blobKey(upstream *url.URL, etag string) string {
	return blobKeyFromHost(upstream.Host, etag)
}

func blobKeyFromHost(host, etag string) string {
	h := sha256.New()
	h.Write([]byte(host))
	h.Write([]byte{0})
	h.Write([]byte(etag))
	return hex.EncodeToString(h.Sum(nil))
}

func (c *Cache) serve(w http.ResponseWriter, r *http.Request, upstream *url.URL, headers []Header, block []string, modify ModifyResponse) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		c.passthrough(w, r, upstream, headers, block, modify)
		return
	}

	rk := requestKeyFor(upstream, r)
	c.mu.Lock()
	knownETag := c.etags[rk]
	c.mu.Unlock()

	// Discover the current upstream state (HEAD with conditional). We follow
	// redirects so the response we get is the final one (e.g. HF → CDN).
	etag, meta, supportsRange, err := c.discover(r.Context(), upstream, r, headers, block, knownETag)
	if err != nil {
		http.Error(w, "upstream: "+err.Error(), http.StatusBadGateway)
		return
	}

	// 304: cached entry, if still present, is authoritative.
	if etag == knownETag && etag != "" {
		if b := c.acquireBlob(blobKey(upstream, etag)); b != nil {
			defer b.release()
			c.serveFromBlob(w, r, upstream, headers, b)
			return
		}
		// Entry was evicted between fetches — fall through to re-fetch.
		c.mu.Lock()
		delete(c.etags, rk)
		c.mu.Unlock()
		etag = ""
	}

	if etag == "" || !supportsRange || meta.ContentLength <= 0 {
		// Not cacheable — no strong ETag, or upstream won't honor Range,
		// or we can't predetermine size. Pass through.
		c.passthrough(w, r, upstream, headers, block, modify)
		return
	}
	if hasUncacheableVary(meta.Header) {
		c.passthrough(w, r, upstream, headers, block, modify)
		return
	}

	c.mu.Lock()
	c.etags[rk] = etag
	c.mu.Unlock()

	b, created := c.getOrCreateBlob(upstream, etag, meta)
	if b == nil {
		c.passthrough(w, r, upstream, headers, block, modify)
		return
	}
	defer b.release()
	if created {
		// Kick off streaming-tee fill in a goroutine, sharing this request's
		// context. If the client cancels mid-fill, remaining chunks are
		// released back to empty so subsequent requests can recover them via
		// per-chunk Range fetches.
		fetchFull := c.fetchFullFn(upstream, r, headers, b.meta.UpstreamETag)
		b.startStreamingFill(r.Context(), fetchFull)
	}
	c.serveFromBlob(w, r, upstream, headers, b)
}

// fetchFullFn returns a closure that issues a full-body GET of upstream for
// the current request. If upstreamETag is non-empty it's enforced via
// If-Match (guards against the upstream swapping content mid-fetch). Empty
// means the upstream didn't expose an etag usable at the body URL (e.g. a
// redirect target's CDN that HF's X-Linked-Etag doesn't match) — skip the
// precondition.
func (c *Cache) fetchFullFn(upstream *url.URL, r *http.Request, headers []Header, upstreamETag string) func(ctx context.Context) (io.ReadCloser, error) {
	return func(ctx context.Context) (io.ReadCloser, error) {
		out, err := buildOutbound(ctx, http.MethodGet, r, upstream, headers)
		if err != nil {
			return nil, err
		}
		if upstreamETag != "" {
			out.Header.Set("If-Match", upstreamETag)
		}
		resp, err := c.client.Do(out)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("upstream GET: %d", resp.StatusCode)
		}
		return resp.Body, nil
	}
}

// discover issues a HEAD against the upstream without auto-following
// redirects. If knownETag is non-empty it's sent as If-None-Match; on 304 the
// function returns etag = knownETag and meta from the original cached entry.
//
// On a 3xx response, discover issues a second HEAD against the Location
// (following further redirects) to get the final resource's ETag / size /
// content-type. Headers from the initial response are merged so HF-specific
// metadata (X-Repo-Commit, X-Linked-Etag, X-Linked-Size, X-Xet-*) that lives
// on the redirect survives for the client.
func (c *Cache) discover(ctx context.Context, upstream *url.URL, r *http.Request, headers []Header, block []string, knownETag string) (etag string, meta blobMeta, supportsRange bool, err error) {
	out, err := buildOutbound(ctx, http.MethodHead, r, upstream, headers)
	if err != nil {
		return "", blobMeta{}, false, err
	}
	if knownETag != "" {
		out.Header.Set("If-None-Match", knownETag)
	}
	resp, err := c.headClient.Do(out)
	if err != nil {
		return "", blobMeta{}, false, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // tiny

	switch {
	case resp.StatusCode == http.StatusNotModified:
		return knownETag, blobMeta{}, true, nil
	case resp.StatusCode == http.StatusOK:
		respETag := strongETag(resp.Header.Get("ETag"))
		etag = preferLinkedETag(resp.Header, respETag)
		supportsRange = strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes")
		meta = blobMeta{
			ETag:          etag,
			UpstreamETag:  respETag,
			StatusCode:    http.StatusOK,
			ContentType:   resp.Header.Get("Content-Type"),
			ContentLength: preferLinkedSize(resp.Header, resp.ContentLength),
			Header:        filterResponseHeaders(resp.Header, block),
			ChunkSize:     c.chunkSize,
		}
		return etag, meta, supportsRange, nil
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		loc := resp.Header.Get("Location")
		if loc == "" {
			return "", blobMeta{}, false, fmt.Errorf("upstream HEAD %d without Location", resp.StatusCode)
		}
		absLoc, err := out.URL.Parse(loc)
		if err != nil {
			return "", blobMeta{}, false, fmt.Errorf("parse Location %q: %w", loc, err)
		}
		follow, err := http.NewRequestWithContext(ctx, http.MethodHead, absLoc.String(), nil)
		if err != nil {
			return "", blobMeta{}, false, err
		}
		followResp, err := c.client.Do(follow)
		if err != nil {
			return "", blobMeta{}, false, err
		}
		defer followResp.Body.Close()
		io.Copy(io.Discard, followResp.Body)
		if followResp.StatusCode != http.StatusOK {
			return "", blobMeta{}, false, fmt.Errorf("upstream HEAD after redirect: %d", followResp.StatusCode)
		}
		// Merge: start from origin's redirect headers (HF metadata), overlay
		// final response headers (CDN size/type/etag). Strip Location — we
		// always serve 200 from cache, Location is not meaningful downstream.
		merged := http.Header{}
		for k, vs := range resp.Header {
			merged[k] = append([]string{}, vs...)
		}
		for k, vs := range followResp.Header {
			merged[k] = append([]string{}, vs...)
		}
		merged.Del("Location")
		respETag := strongETag(followResp.Header.Get("ETag"))
		// Linked headers are on the origin's 3xx (resp), not the CDN's 200.
		etag = preferLinkedETag(resp.Header, respETag)
		supportsRange = strings.EqualFold(followResp.Header.Get("Accept-Ranges"), "bytes") ||
			strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes")
		meta = blobMeta{
			ETag:          etag,
			UpstreamETag:  respETag,
			StatusCode:    http.StatusOK,
			ContentType:   followResp.Header.Get("Content-Type"),
			ContentLength: preferLinkedSize(resp.Header, followResp.ContentLength),
			Header:        filterResponseHeaders(merged, block),
			ChunkSize:     c.chunkSize,
		}
		return etag, meta, supportsRange, nil
	default:
		return "", blobMeta{}, false, fmt.Errorf("upstream HEAD returned %d", resp.StatusCode)
	}
}

// acquireBlob looks up a blob by key and bumps its in-flight refcount before
// returning. Returns nil if not present. Caller must call b.release() when done
// to allow eviction. Atomic with eviction (which also takes c.mu).
func (c *Cache) acquireBlob(key string) *blob {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.blobs[key]
	if !ok {
		return nil
	}
	b.acquire()
	return b
}

// getOrCreateBlob returns an existing blob (with refcount incremented) or
// creates a new one. The created return is true only on the path that
// allocates a new blob — the caller can use it to decide whether to kick off
// a streaming-tee fill. Caller must release.
func (c *Cache) getOrCreateBlob(upstream *url.URL, etag string, meta blobMeta) (b *blob, created bool) {
	key := blobKey(upstream, etag)
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.blobs[key]; ok {
		existing.acquire()
		return existing, false
	}
	b = &blob{
		key:       key,
		dir:       filepath.Join(c.dir, key),
		meta:      meta,
		chunkSize: meta.ChunkSize,
		cache:     c,
	}
	if err := b.init(); err != nil {
		log.Printf("cache: blob init %s: %v", key, err)
		return nil, false
	}
	c.blobs[key] = b
	b.acquire()
	return b, true
}

func (c *Cache) serveFromBlob(w http.ResponseWriter, r *http.Request, upstream *url.URL, headers []Header, b *blob) {
	b.touch()

	// Honor client conditional: if their If-None-Match matches our ETag, 304.
	if im := r.Header.Get("If-None-Match"); im != "" && im == b.meta.ETag {
		w.Header().Set("ETag", b.meta.ETag)
		w.WriteHeader(http.StatusNotModified)
		return
	}

	contentLen := b.meta.ContentLength
	start, end, partial, ok := parseRange(r.Header.Get("Range"), contentLen)
	if !ok {
		http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	// Pre-fetch the first chunk before committing response headers so fetch
	// failures on the very first chunk surface as a clean 502 rather than a
	// truncated 200/206 with mismatched Content-Length. Mid-stream failures
	// on later chunks still result in a truncated body — the Go http.Client
	// detects this as ErrUnexpectedEOF via the Content-Length mismatch — but
	// at least the request gets *some* coherent feedback.
	//
	// Skip for HEAD: metadata was already validated by discover(), and we
	// won't write a body anyway, so there's no upside to paying a potentially
	// 4 MiB+ Range fetch just to answer metadata.
	chunkSize := b.chunkSize
	firstIdx := int(start / chunkSize)
	if r.Method != http.MethodHead {
		if err := b.ensureChunk(r.Context(), firstIdx, c.fetchChunkFn(upstream, r, headers, b, firstIdx)); err != nil {
			log.Printf("cache: ensure first chunk %d: %v", firstIdx, err)
			http.Error(w, "upstream: "+err.Error(), http.StatusBadGateway)
			return
		}
	}

	for k, vs := range b.meta.Header {
		w.Header()[k] = append([]string{}, vs...)
	}
	if b.meta.ContentType != "" {
		w.Header().Set("Content-Type", b.meta.ContentType)
	}
	w.Header().Set("ETag", b.meta.ETag)

	length := end - start + 1
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	if partial {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, contentLen))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(b.meta.StatusCode)
	}
	if r.Method == http.MethodHead {
		return
	}

	// Walk chunks covering [start, end] and stream their bytes.
	for off := start; off <= end; {
		idx := int(off / chunkSize)
		chunkStart := int64(idx) * chunkSize
		chunkEnd := chunkStart + chunkSize - 1
		if chunkEnd >= contentLen {
			chunkEnd = contentLen - 1
		}
		sliceFrom := off - chunkStart
		sliceTo := chunkEnd - chunkStart
		if end < chunkEnd {
			sliceTo = end - chunkStart
		}
		if idx != firstIdx {
			if err := b.ensureChunk(r.Context(), idx, c.fetchChunkFn(upstream, r, headers, b, idx)); err != nil {
				log.Printf("cache: ensure chunk %d: %v", idx, err)
				return
			}
		}
		if err := b.copyChunk(w, idx, sliceFrom, sliceTo); err != nil {
			log.Printf("cache: copy chunk %d: %v", idx, err)
			return
		}
		off = chunkEnd + 1
	}
}

// fetchChunkFn builds the per-chunk fetcher for a specific request. The closure
// captures upstream + headers + the original client request (for path/query).
func (c *Cache) fetchChunkFn(upstream *url.URL, r *http.Request, headers []Header, b *blob, idx int) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		chunkStart := int64(idx) * b.chunkSize
		chunkEnd := chunkStart + b.chunkSize - 1
		if chunkEnd >= b.meta.ContentLength {
			chunkEnd = b.meta.ContentLength - 1
		}

		out, err := buildOutbound(ctx, http.MethodGet, r, upstream, headers)
		if err != nil {
			return err
		}
		out.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunkStart, chunkEnd))
		// Validate against the upstream's own ETag (the body URL's CDN etag,
		// not our canonical X-Linked-Etag which the CDN doesn't know). Skip
		// the precondition if the upstream didn't give us one.
		if b.meta.UpstreamETag != "" {
			out.Header.Set("If-Match", b.meta.UpstreamETag)
		}

		resp, err := c.client.Do(out)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusPartialContent:
			// good
		case http.StatusOK:
			// Server ignored Range. Only OK if this is the only chunk.
			if chunkStart != 0 {
				return fmt.Errorf("upstream returned 200 to ranged request for chunk %d", idx)
			}
		case http.StatusPreconditionFailed:
			return fmt.Errorf("upstream ETag changed (412)")
		default:
			return fmt.Errorf("upstream Range fetch: %d", resp.StatusCode)
		}
		return b.writeChunk(idx, resp.Body)
	}
}

func (c *Cache) passthrough(w http.ResponseWriter, r *http.Request, upstream *url.URL, headers []Header, block []string, modify ModifyResponse) {
	out, err := buildOutbound(r.Context(), r.Method, r, upstream, headers)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	resp, err := c.client.Do(out)
	if err != nil {
		http.Error(w, "upstream: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if modify != nil {
		if err := modify(resp); err != nil {
			http.Error(w, "upstream modify: "+err.Error(), http.StatusBadGateway)
			return
		}
	}
	streamResponse(w, resp, block)
}

func streamResponse(w http.ResponseWriter, resp *http.Response, block []string) {
	for k, vs := range filterResponseHeaders(resp.Header, block) {
		w.Header()[k] = append([]string{}, vs...)
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// buildOutbound constructs a request to upstream by combining upstream's URL
// stub with the client request's path/query, and applying header injection.
// Note: X-Forwarded-* headers are intentionally NOT added — this proxy's job
// is to make calls look like they originate from the proxy host, with our
// injected credentials.
func buildOutbound(ctx context.Context, method string, r *http.Request, upstream *url.URL, headers []Header) (*http.Request, error) {
	out := *upstream
	out.Path = joinPaths(upstream.Path, r.URL.Path)
	out.RawQuery = r.URL.RawQuery
	req, err := http.NewRequestWithContext(ctx, method, out.String(), nil)
	if err != nil {
		return nil, err
	}
	connHdr := r.Header.Get("Connection")
	for k, vs := range r.Header {
		if isHopByHop(k, connHdr) {
			continue
		}
		req.Header[k] = append([]string{}, vs...)
	}
	req.Host = upstream.Host
	for _, h := range headers {
		req.Header.Set(h.Name, h.Value)
	}
	return req, nil
}

var hopByHopFixed = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// isHopByHop returns true for canonical hop-by-hop headers and any header
// listed in a Connection: header (RFC 7230 §6.1).
func isHopByHop(h, connectionValue string) bool {
	canon := http.CanonicalHeaderKey(h)
	if hopByHopFixed[canon] {
		return true
	}
	if connectionValue == "" {
		return false
	}
	for name := range strings.SplitSeq(connectionValue, ",") {
		if strings.EqualFold(strings.TrimSpace(name), h) {
			return true
		}
	}
	return false
}

func joinPaths(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	case strings.HasSuffix(a, "/") && strings.HasPrefix(b, "/"):
		return a + b[1:]
	case !strings.HasSuffix(a, "/") && !strings.HasPrefix(b, "/"):
		return a + "/" + b
	default:
		return a + b
	}
}

// preferLinkedETag returns the strong X-Linked-Etag value if present (that's
// the upstream-declared content hash for the file, e.g. HF's LFS digest),
// otherwise the given fallback. The linked etag is stable across CDN boundary
// crossings where the response ETag is not.
func preferLinkedETag(h http.Header, fallback string) string {
	if linked := strongETag(h.Get("X-Linked-Etag")); linked != "" {
		return linked
	}
	return fallback
}

// preferLinkedSize returns X-Linked-Size if parseable and positive, otherwise
// the given fallback. Needed for redirects where the origin's Content-Length
// describes a tiny redirect body instead of the real resource size.
func preferLinkedSize(h http.Header, fallback int64) int64 {
	if v := h.Get("X-Linked-Size"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func strongETag(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.HasPrefix(raw, "W/") {
		return ""
	}
	if !strings.HasPrefix(raw, `"`) || !strings.HasSuffix(raw, `"`) {
		return ""
	}
	return raw
}

// filterResponseHeaders returns a copy of h with hop-by-hop headers and any
// user-configured blocked headers removed.
func filterResponseHeaders(h http.Header, block []string) http.Header {
	conn := h.Get("Connection")
	var blockSet map[string]bool
	if len(block) > 0 {
		blockSet = make(map[string]bool, len(block))
		for _, name := range block {
			blockSet[http.CanonicalHeaderKey(name)] = true
		}
	}
	out := http.Header{}
	for k, vs := range h {
		if isHopByHop(k, conn) {
			continue
		}
		if blockSet[http.CanonicalHeaderKey(k)] {
			continue
		}
		out[k] = append([]string{}, vs...)
	}
	return out
}

// hasUncacheableVary returns true if the response declares a Vary header that
// would make a single cache entry per URL incorrect. Identity is fine; anything
// else is rejected for v1.
func hasUncacheableVary(h http.Header) bool {
	v := h.Get("Vary")
	if v == "" {
		return false
	}
	for name := range strings.SplitSeq(v, ",") {
		canon := strings.ToLower(strings.TrimSpace(name))
		switch canon {
		case "", "accept-encoding", "origin", "accept":
			// accept-encoding: we don't negotiate gzip; outbound is identity.
			// origin/accept: single-origin proxy → constant.
			continue
		}
		if strings.HasPrefix(canon, "access-control-") {
			// CORS preflight headers; meaningful only for OPTIONS.
			continue
		}
		// Vary on client-specific things (Cookie, Authorization, User-Agent, etc.)
		// genuinely creates cache-poisoning risk; refuse.
		return true
	}
	return false
}

// parseRange parses a single-range "Range: bytes=A-B" header against contentLen.
// Returns start, end (inclusive), partial=true if the request was a range,
// ok=false on malformed input. An empty Range header returns the full body.
func parseRange(rangeHdr string, contentLen int64) (start, end int64, partial, ok bool) {
	if rangeHdr == "" {
		return 0, contentLen - 1, false, true
	}
	if !strings.HasPrefix(rangeHdr, "bytes=") || strings.Contains(rangeHdr, ",") {
		return 0, 0, false, false
	}
	spec := strings.TrimPrefix(rangeHdr, "bytes=")
	startS, endS, ok2 := strings.Cut(spec, "-")
	if !ok2 {
		return 0, 0, false, false
	}
	if startS == "" {
		// Suffix: bytes=-N
		n, err := strconv.ParseInt(endS, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false, false
		}
		start = max(contentLen-n, 0)
		return start, contentLen - 1, true, true
	}
	s, err := strconv.ParseInt(startS, 10, 64)
	if err != nil || s < 0 {
		return 0, 0, false, false
	}
	start = s
	if endS == "" {
		end = contentLen - 1
	} else {
		e, err := strconv.ParseInt(endS, 10, 64)
		if err != nil {
			return 0, 0, false, false
		}
		end = e
		if end >= contentLen {
			end = contentLen - 1
		}
	}
	if start > end || start >= contentLen {
		return 0, 0, false, false
	}
	return start, end, true, true
}

// --- blob ---

type blobMeta struct {
	// ETag is the canonical file identity — prefer X-Linked-Etag (the content
	// hash HF exposes on HEAD) over the final response's ETag. Used as the
	// cache key and served to clients.
	ETag string `json:"etag"`
	// UpstreamETag is the ETag on the final-resource HEAD (the CDN ETag for
	// HF). Needed for If-Match on chunked Range GETs against the CDN, which
	// doesn't know about X-Linked-Etag. Empty if no redirect or the final
	// upstream didn't send one.
	UpstreamETag  string      `json:"upstream_etag,omitempty"`
	StatusCode    int         `json:"status"`
	ContentType   string      `json:"content_type,omitempty"`
	ContentLength int64       `json:"content_length"`
	Header        http.Header `json:"header,omitempty"`
	ChunkSize     int64       `json:"chunk_size"`
}

type blob struct {
	key       string
	dir       string
	meta      blobMeta
	chunkSize int64
	cache     *Cache // back-ref for size accounting

	mu         sync.Mutex
	chunks     []chunkSlot
	lastAccess atomic.Int64
	inFlight   atomic.Int32 // active reads/fills; eviction skips while >0
	bytes      atomic.Int64 // sum of complete chunk file sizes
}

func (b *blob) acquire() { b.inFlight.Add(1) }
func (b *blob) release() { b.inFlight.Add(-1) }

// chunkCompleted records that a chunk became complete. Must be called exactly
// once per chunk transitioning from empty to complete.
func (b *blob) chunkCompleted(idx int) {
	size := b.expectedChunkSize(idx)
	b.bytes.Add(size)
	if b.cache != nil {
		b.cache.sizeBytes.Add(size)
	}
}

type chunkState uint8

const (
	chunkEmpty chunkState = iota
	chunkInflight
	chunkComplete
)

type chunkSlot struct {
	state   chunkState
	err     error
	waiters chan struct{} // closed when state moves out of inflight
}

func (b *blob) init() error {
	if err := os.MkdirAll(filepath.Join(b.dir, "chunks"), 0o700); err != nil {
		return err
	}
	if err := writeJSONAtomic(filepath.Join(b.dir, "meta.json"), b.meta); err != nil {
		return err
	}
	n := chunkCount(b.meta.ContentLength, b.chunkSize)
	b.chunks = make([]chunkSlot, n)
	// Mark already-present chunks complete (e.g. partial fill from a prior run).
	for i := range b.chunks {
		if statOK(b.chunkPath(i), b.expectedChunkSize(i)) {
			b.chunks[i].state = chunkComplete
			b.chunkCompleted(i)
		}
	}
	return nil
}

func loadBlob(dir string, cache *Cache) (*blob, error) {
	var meta blobMeta
	f, err := os.Open(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, err
	}
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		f.Close()
		return nil, err
	}
	f.Close()
	b := &blob{
		key:       filepath.Base(dir),
		dir:       dir,
		meta:      meta,
		chunkSize: meta.ChunkSize,
		cache:     cache,
	}
	n := chunkCount(meta.ContentLength, meta.ChunkSize)
	b.chunks = make([]chunkSlot, n)
	for i := range b.chunks {
		if statOK(b.chunkPath(i), b.expectedChunkSize(i)) {
			b.chunks[i].state = chunkComplete
			b.chunkCompleted(i)
		}
	}
	return b, nil
}

func (b *blob) touch() {
	b.lastAccess.Store(timeNowUnixNano())
}

func (b *blob) chunkPath(idx int) string {
	return filepath.Join(b.dir, "chunks", strconv.Itoa(idx))
}

func (b *blob) expectedChunkSize(idx int) int64 {
	chunkStart := int64(idx) * b.chunkSize
	chunkEnd := min(chunkStart+b.chunkSize, b.meta.ContentLength)
	return chunkEnd - chunkStart
}

func (b *blob) ensureChunk(ctx context.Context, idx int, fetch func(ctx context.Context) error) error {
	for {
		b.mu.Lock()
		s := &b.chunks[idx]
		switch s.state {
		case chunkComplete:
			b.mu.Unlock()
			return nil
		case chunkInflight:
			ch := s.waiters
			b.mu.Unlock()
			select {
			case <-ch:
				// Re-loop: holder may have completed, errored, or released
				// without writing (cancelled streaming-tee fill). Re-check.
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		default: // chunkEmpty
			// Previous holder errored fatally — propagate rather than thrash.
			if s.err != nil {
				err := s.err
				b.mu.Unlock()
				return err
			}
			// Race: a previous holder released the chunk (closing waiters)
			// in the same scheduling moment our own ctx was cancelled. The
			// select above may have picked the waiter case; we'd otherwise
			// proceed to claim+fetch with a cancelled ctx. Check ctx here
			// so we propagate the cancellation rather than leaking work.
			if err := ctx.Err(); err != nil {
				b.mu.Unlock()
				return err
			}
			s.state = chunkInflight
			s.waiters = make(chan struct{})
			b.mu.Unlock()

			err := fetch(ctx)

			b.mu.Lock()
			if err == nil {
				s.err = nil
				s.state = chunkComplete
				b.chunkCompleted(idx)
			} else {
				s.err = err
				s.state = chunkEmpty
				os.Remove(b.chunkPath(idx))
			}
			close(s.waiters)
			s.waiters = nil
			b.mu.Unlock()
			return err
		}
	}
}

// startStreamingFill claims all currently-empty chunks and spawns a goroutine
// to fill them sequentially from a single upstream GET. Bound to ctx — if ctx
// is cancelled mid-fill, remaining chunks are released back to empty so
// subsequent requests can recover them via per-chunk Range fetches.
//
// Returns immediately. No-op if no empty chunks (e.g. partial-recovery blob).
// Caller must hold a refcount on b (so it doesn't get evicted underneath the
// fill goroutine); the goroutine acquires its own refcount before returning
// to the caller.
func (b *blob) startStreamingFill(ctx context.Context, fetchFull func(ctx context.Context) (io.ReadCloser, error)) {
	b.mu.Lock()
	var claimed []int
	for i := range b.chunks {
		if b.chunks[i].state == chunkEmpty {
			b.chunks[i].state = chunkInflight
			b.chunks[i].waiters = make(chan struct{})
			claimed = append(claimed, i)
		}
	}
	b.mu.Unlock()
	if len(claimed) == 0 {
		return
	}
	b.acquire() // hold across the goroutine's lifetime
	go func() {
		defer b.release()
		b.runStreamingFill(ctx, fetchFull, claimed)
	}()
}

func (b *blob) runStreamingFill(ctx context.Context, fetchFull func(ctx context.Context) (io.ReadCloser, error), claimed []int) {
	body, err := fetchFull(ctx)
	if err != nil {
		// Network-level error before any data — treat as cancellation, not
		// a propagated fatal error, so subsequent requests can retry via
		// per-chunk Range.
		b.releaseClaimed(claimed, 0, nil)
		return
	}
	defer body.Close()

	buf := make([]byte, b.chunkSize)
	for i, idx := range claimed {
		size := b.expectedChunkSize(idx)
		if _, err := io.ReadFull(body, buf[:size]); err != nil {
			// Read error or stream cut short. Release this and remaining
			// chunks without an error — let recovery happen via Range.
			b.releaseClaimed(claimed, i, nil)
			return
		}
		if err := b.writeChunkFromBytes(idx, buf[:size]); err != nil {
			log.Printf("cache: streaming-fill write %s/%d: %v", b.key, idx, err)
			b.releaseClaimed(claimed, i, nil)
			return
		}
		b.markChunkComplete(idx)
	}
}

// releaseClaimed transitions chunks claimed[from:] back to empty, setting
// their err. Subsequent ensureChunk callers will re-attempt the fetch via
// per-chunk Range (when err is nil) or propagate the err.
func (b *blob) releaseClaimed(claimed []int, from int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, idx := range claimed[from:] {
		s := &b.chunks[idx]
		if s.state != chunkInflight {
			continue
		}
		s.state = chunkEmpty
		s.err = err
		if s.waiters != nil {
			close(s.waiters)
			s.waiters = nil
		}
	}
}

// markChunkComplete transitions a chunk to complete and signals waiters.
func (b *blob) markChunkComplete(idx int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := &b.chunks[idx]
	s.state = chunkComplete
	s.err = nil
	if s.waiters != nil {
		close(s.waiters)
		s.waiters = nil
	}
	b.chunkCompleted(idx)
}

// writeChunkFromBytes writes data to chunk idx atomically. Length must equal
// expectedChunkSize.
func (b *blob) writeChunkFromBytes(idx int, data []byte) error {
	want := b.expectedChunkSize(idx)
	if int64(len(data)) != want {
		return fmt.Errorf("chunk %d: %d bytes, want %d", idx, len(data), want)
	}
	tmp := b.chunkPath(idx) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		os.Remove(tmp) // best-effort cleanup; WriteFile may have created a partial file
		return err
	}
	if err := os.Rename(tmp, b.chunkPath(idx)); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// writeChunk consumes body fully and writes it atomically to chunk idx.
// Length validation: must equal expectedChunkSize.
func (b *blob) writeChunk(idx int, body io.Reader) error {
	tmp := b.chunkPath(idx) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, body)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	want := b.expectedChunkSize(idx)
	if n != want {
		os.Remove(tmp)
		return fmt.Errorf("chunk %d: got %d bytes, want %d", idx, n, want)
	}
	return os.Rename(tmp, b.chunkPath(idx))
}

func (b *blob) copyChunk(w io.Writer, idx int, from, to int64) error {
	f, err := os.Open(b.chunkPath(idx))
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return err
	}
	_, err = io.CopyN(w, f, to-from+1)
	return err
}

// --- helpers ---

func chunkCount(contentLen, chunkSize int64) int {
	if contentLen <= 0 {
		return 0
	}
	return int((contentLen + chunkSize - 1) / chunkSize)
}

func statOK(path string, wantSize int64) bool {
	st, err := os.Stat(path)
	if err != nil {
		return false
	}
	return st.Size() == wantSize
}

func writeJSONAtomic(path string, v any) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func timeNowUnixNano() int64 {
	return time.Now().UnixNano()
}
