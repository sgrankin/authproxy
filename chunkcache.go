package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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
// On miss the upstream response is buffered into memory, written to disk in
// HTTP wire format (status + headers + body), then served to the client. On
// hit the file is parsed back with http.ReadResponse and streamed directly.
// Per-key singleflight dedups concurrent fetches.
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "proxy: read body: "+err.Error(), http.StatusBadGateway)
		return
	}

	cc.writeCache(key, resp, body)
	for k, vs := range resp.Header {
		if isHopByHop(k, resp.Header.Get("Connection")) {
			continue
		}
		w.Header()[k] = append([]string{}, vs...)
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := w.Write(body); err != nil {
		log.Printf("chunkcache: write client: %v", err)
	}
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

func (cc *chunkCache) writeCache(key string, resp *http.Response, body []byte) {
	var buf bytes.Buffer
	// Rebuild a writable response: resp.Body was already drained.
	r := &http.Response{
		StatusCode:    resp.StatusCode,
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        resp.Header.Clone(),
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	if err := r.Write(&buf); err != nil {
		log.Printf("chunkcache: serialize: %v", err)
		return
	}
	p := cc.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		log.Printf("chunkcache: mkdir: %v", err)
		return
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o600); err != nil {
		log.Printf("chunkcache: write tmp: %v", err)
		os.Remove(tmp)
		return
	}
	if err := os.Rename(tmp, p); err != nil {
		log.Printf("chunkcache: rename: %v", err)
		os.Remove(tmp)
	}
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
