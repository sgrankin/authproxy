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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	dir     string
	maxSize int64 // 0 → no eviction

	mu       sync.Mutex
	inflight map[string]chan struct{}
	entries  map[string]*chunkEntry // key → metadata for LRU eviction

	sizeBytes atomic.Int64

	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

type chunkEntry struct {
	size       int64
	lastAccess atomic.Int64 // time.Now().UnixNano at last hit
}

const chunkEvictionInterval = time.Minute

func newChunkCache(dir string, maxSize int64) (*chunkCache, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	cc := &chunkCache{
		dir:      dir,
		maxSize:  maxSize,
		inflight: map[string]chan struct{}{},
		entries:  map[string]*chunkEntry{},
		closeCh:  make(chan struct{}),
	}
	cc.load()
	if maxSize > 0 {
		cc.wg.Add(1)
		go cc.evictLoop()
	}
	return cc, nil
}

// Close stops background eviction. Safe to call multiple times.
func (cc *chunkCache) Close() {
	cc.closeOnce.Do(func() { close(cc.closeCh) })
	cc.wg.Wait()
}

// load scans the cache dir on startup, populating entries and sizeBytes
// from existing files. Orphaned temp files (from crashes mid-fill) are
// reaped.
func (cc *chunkCache) load() {
	tops, err := os.ReadDir(cc.dir)
	if err != nil {
		return
	}
	for _, top := range tops {
		if !top.IsDir() {
			continue
		}
		sub := filepath.Join(cc.dir, top.Name())
		subs, err := os.ReadDir(sub)
		if err != nil {
			continue
		}
		for _, e := range subs {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasSuffix(name, ".tmp") {
				os.Remove(filepath.Join(sub, name))
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			entry := &chunkEntry{size: info.Size()}
			entry.lastAccess.Store(info.ModTime().UnixNano())
			cc.entries[name] = entry
			cc.sizeBytes.Add(info.Size())
		}
	}
}

func (cc *chunkCache) evictLoop() {
	defer cc.wg.Done()
	t := time.NewTicker(chunkEvictionInterval)
	defer t.Stop()
	for {
		select {
		case <-cc.closeCh:
			return
		case <-t.C:
			cc.checkEviction()
		}
	}
}

// checkEviction deletes oldest-accessed entries until sizeBytes <= maxSize.
// Deletes are idempotent from the reader's POV — an in-progress read holds
// an open fd on the unlinked file and continues until it closes.
func (cc *chunkCache) checkEviction() {
	if cc.sizeBytes.Load() <= cc.maxSize {
		return
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	type cand struct {
		key string
		e   *chunkEntry
	}
	cands := make([]cand, 0, len(cc.entries))
	for k, e := range cc.entries {
		cands = append(cands, cand{k, e})
	}
	sort.Slice(cands, func(i, j int) bool {
		return cands[i].e.lastAccess.Load() < cands[j].e.lastAccess.Load()
	})
	for _, c := range cands {
		if cc.sizeBytes.Load() <= cc.maxSize {
			break
		}
		p := cc.path(c.key)
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			log.Printf("chunkcache: evict %s: %v", c.key, err)
			continue
		}
		delete(cc.entries, c.key)
		cc.sizeBytes.Add(-c.e.size)
	}
}

// touch records access for a key, so recently-used entries survive
// eviction.
func (cc *chunkCache) touch(key string) {
	cc.mu.Lock()
	e, ok := cc.entries[key]
	cc.mu.Unlock()
	if ok {
		e.lastAccess.Store(time.Now().UnixNano())
	}
}

// admit registers a freshly-written entry in the LRU bookkeeping.
func (cc *chunkCache) admit(key string, size int64) {
	entry := &chunkEntry{size: size}
	entry.lastAccess.Store(time.Now().UnixNano())
	cc.mu.Lock()
	cc.entries[key] = entry
	cc.mu.Unlock()
	cc.sizeBytes.Add(size)
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
		return
	}
	if info, err := os.Stat(p); err == nil {
		cc.admit(key, info.Size())
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
		cc.mu.Lock()
		if e, ok := cc.entries[key]; ok {
			delete(cc.entries, key)
			cc.sizeBytes.Add(-e.size)
		}
		cc.mu.Unlock()
		return false
	}
	defer resp.Body.Close()
	cc.touch(key)
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
