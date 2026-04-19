package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// chunkCache is a content-addressable HTTP response cache used by the xet
// adapter. Unlike Cache (which models ETag-revalidated origins), the keys
// here are pre-validated content hashes derived from the upstream URL — two
// requests that resolve to the same key are guaranteed to have identical
// bytes, so no freshness check is needed.
//
// On miss, the upstream response is tee'd simultaneously to a temp file on
// disk and to the client — no full-body buffering, no serial "download then
// write" stall. A rename promotes the temp file into the cache atomically
// once both the client write and the disk write succeed. On hit the file
// is parsed back with http.ReadResponse and streamed directly. Per-key
// singleflight dedups concurrent fetches.
type chunkCache struct {
	dir string

	mu       sync.Mutex
	inflight map[string]chan struct{}
}

func newChunkCache(dir string) (*chunkCache, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &chunkCache{dir: dir, inflight: map[string]chan struct{}{}}, nil
}

func (cc *chunkCache) path(key string) string {
	return filepath.Join(cc.dir, key[:2], key)
}

// serve attempts a cache hit; on miss it invokes fetch, caches the response
// if successful, and streams to the client either way.
func (cc *chunkCache) serve(w http.ResponseWriter, r *http.Request, key string, fetch func() (*http.Response, error)) {
	// Loop: cached, inflight (wait), or claim.
	for {
		if cc.tryServe(w, key) {
			return
		}
		cc.mu.Lock()
		waiter, busy := cc.inflight[key]
		if busy {
			cc.mu.Unlock()
			<-waiter
			continue
		}
		done := make(chan struct{})
		cc.inflight[key] = done
		cc.mu.Unlock()
		defer func() {
			cc.mu.Lock()
			delete(cc.inflight, key)
			close(done)
			cc.mu.Unlock()
		}()
		break
	}

	resp, err := fetch()
	if err != nil {
		http.Error(w, "proxy: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Only 200/206 responses are safe to cache as content-addressable. Pass
	// everything else through unchanged.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		streamResponse(w, resp, nil)
		return
	}

	cc.streamAndCache(w, key, resp)
}

// streamAndCache writes the upstream response to both a temp file on disk
// and to the client in one pass via io.TeeReader. On successful copy the
// temp file is renamed into the cache. On client-disconnect or copy error
// the temp file is discarded and no cache entry is created — next request
// will re-fetch.
func (cc *chunkCache) streamAndCache(w http.ResponseWriter, key string, resp *http.Response) {
	p := cc.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		log.Printf("chunkcache: mkdir: %v", err)
		cc.stream(w, resp)
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), "chunk-*.tmp")
	if err != nil {
		log.Printf("chunkcache: createtemp: %v", err)
		cc.stream(w, resp)
		return
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpPath)
	}

	// HTTP wire-format header: matches what http.ReadResponse parses on
	// the hit path.
	if _, err := fmt.Fprintf(tmp, "HTTP/1.1 %d %s\r\n", resp.StatusCode, http.StatusText(resp.StatusCode)); err != nil {
		log.Printf("chunkcache: write status: %v", err)
		cleanup()
		cc.stream(w, resp)
		return
	}
	if err := resp.Header.Write(tmp); err != nil {
		log.Printf("chunkcache: write headers: %v", err)
		cleanup()
		cc.stream(w, resp)
		return
	}
	if _, err := tmp.Write([]byte("\r\n")); err != nil {
		cleanup()
		cc.stream(w, resp)
		return
	}

	// Client sees headers now; body streams as tee progresses.
	for k, vs := range resp.Header {
		if isHopByHop(k, resp.Header.Get("Connection")) {
			continue
		}
		w.Header()[k] = append([]string{}, vs...)
	}
	w.WriteHeader(resp.StatusCode)

	tee := io.TeeReader(resp.Body, tmp)
	if _, err := io.Copy(w, tee); err != nil {
		cleanup()
		return
	}
	if err := tmp.Close(); err != nil {
		log.Printf("chunkcache: close tmp: %v", err)
		os.Remove(tmpPath)
		return
	}
	if err := os.Rename(tmpPath, p); err != nil {
		log.Printf("chunkcache: rename: %v", err)
		os.Remove(tmpPath)
	}
}

// stream is the fallback path when disk setup fails — forward the response
// without caching.
func (cc *chunkCache) stream(w http.ResponseWriter, resp *http.Response) {
	for k, vs := range resp.Header {
		if isHopByHop(k, resp.Header.Get("Connection")) {
			continue
		}
		w.Header()[k] = append([]string{}, vs...)
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (cc *chunkCache) tryServe(w http.ResponseWriter, key string) bool {
	p := cc.path(key)
	f, err := os.Open(p)
	if err != nil {
		return false
	}
	defer f.Close()
	resp, err := http.ReadResponse(bufio.NewReader(f), nil)
	if err != nil {
		// Corrupt cache file — delete and miss.
		os.Remove(p)
		return false
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		if isHopByHop(k, resp.Header.Get("Connection")) {
			continue
		}
		w.Header()[k] = append([]string{}, vs...)
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return true
}

// xetChunkKey returns a stable cache key for content-addressable xet URLs,
// or ok=false if the request isn't cacheable.
//
// Paths we cache:
//   - /xorbs/default/<hash>              (transfer.xethub.hf.co)
//   - /xet-bridge-us/<file>/<chunk>      (cas-bridge.xethub.hf.co)
//
// Key inputs: host + path + X-Xet-Signed-Range. The signed-range pins the
// exact byte slice of the xorb this URL returns; two signed URLs with the
// same (host, path, signed-range) serve identical bytes regardless of
// signature expiry.
func xetChunkKey(host, path, rawQuery string) (string, bool) {
	if !isCacheableXetPath(path) {
		return "", false
	}
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", false
	}
	rangeSpec := vals.Get("X-Xet-Signed-Range")
	if rangeSpec == "" {
		// Unsigned URL: skip caching (may be dynamically generated / private).
		return "", false
	}
	h := sha256.New()
	h.Write([]byte(host))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write([]byte(rangeSpec))
	return hex.EncodeToString(h.Sum(nil)), true
}

func isCacheableXetPath(path string) bool {
	return strings.HasPrefix(path, "/xorbs/") || strings.HasPrefix(path, "/xet-bridge-us/")
}
