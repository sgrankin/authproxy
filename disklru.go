package main

import (
	"maps"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DiskLRU is a shared LRU budget across storage layers. Each layer
// registers as a named "kind" and reports admits, size growth, and access.
// When the combined on-disk footprint exceeds maxSize the periodic
// evictLoop picks the globally-oldest entry and asks its kind to free it —
// so e.g. a stale blob can be evicted to make room for a hot xet chunk,
// and vice versa.
type DiskLRU struct {
	maxSize int64

	mu      sync.Mutex
	entries map[entryKey]*diskEntry
	kinds   map[string]DiskKind

	sizeBytes atomic.Int64

	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

type entryKey struct {
	kind, key string
}

type diskEntry struct {
	size       atomic.Int64
	lastAccess atomic.Int64
}

// DiskKind is the per-layer interface DiskLRU calls into to free entries.
// Implementations must be concurrency-safe and idempotent for repeated
// keys (eviction may race with concurrent deletes).
type DiskKind interface {
	// Evict frees underlying storage for key. Return ok=false if the entry
	// is in-use and can't be evicted right now (DiskLRU will skip and try
	// the next candidate; the entry is retried on the next tick).
	Evict(key string) (ok bool)
}

const diskLRUInterval = time.Minute

// NewDiskLRU opens a budget with the given byte cap. maxSize <= 0 disables
// eviction (entries still get tracked, but nothing is deleted).
func NewDiskLRU(maxSize int64) *DiskLRU {
	d := &DiskLRU{
		maxSize: maxSize,
		entries: map[entryKey]*diskEntry{},
		kinds:   map[string]DiskKind{},
		closeCh: make(chan struct{}),
	}
	if maxSize > 0 {
		d.wg.Go(d.evictLoop)
	}
	return d
}

// Close stops the eviction goroutine. Safe to call multiple times.
func (d *DiskLRU) Close() {
	d.closeOnce.Do(func() { close(d.closeCh) })
	d.wg.Wait()
}

// Register attaches a kind's Evict implementation. Must be called before
// any Admit for that kind.
func (d *DiskLRU) Register(name string, k DiskKind) {
	d.mu.Lock()
	d.kinds[name] = k
	d.mu.Unlock()
}

// Admit records a new entry with its starting size and sets lastAccess to
// now. A repeat Admit for the same (kind,key) replaces the prior record.
func (d *DiskLRU) Admit(kind, key string, size int64) {
	ek := entryKey{kind, key}
	e := &diskEntry{}
	e.size.Store(size)
	e.lastAccess.Store(time.Now().UnixNano())
	d.mu.Lock()
	if old, ok := d.entries[ek]; ok {
		d.sizeBytes.Add(-old.size.Load())
	}
	d.entries[ek] = e
	d.mu.Unlock()
	d.sizeBytes.Add(size)
}

// Grow adjusts an entry's size by delta (positive or negative). Use when
// content accumulates over time (e.g., chunks filling into a cached blob).
// No-op if the entry isn't tracked.
func (d *DiskLRU) Grow(kind, key string, delta int64) {
	ek := entryKey{kind, key}
	d.mu.Lock()
	e, ok := d.entries[ek]
	d.mu.Unlock()
	if !ok {
		return
	}
	e.size.Add(delta)
	d.sizeBytes.Add(delta)
}

// Touch stamps an entry as just-accessed. No-op if not tracked.
func (d *DiskLRU) Touch(kind, key string) {
	ek := entryKey{kind, key}
	d.mu.Lock()
	e, ok := d.entries[ek]
	d.mu.Unlock()
	if ok {
		e.lastAccess.Store(time.Now().UnixNano())
	}
}

// Forget removes an entry from tracking without calling Evict. Used when
// the caller has already freed the resource, or discovered it's missing
// or corrupt. Returns the size that was being tracked.
func (d *DiskLRU) Forget(kind, key string) int64 {
	ek := entryKey{kind, key}
	d.mu.Lock()
	e, ok := d.entries[ek]
	if ok {
		delete(d.entries, ek)
	}
	d.mu.Unlock()
	if !ok {
		return 0
	}
	s := e.size.Load()
	d.sizeBytes.Add(-s)
	return s
}

// Size returns the current tracked total across all kinds.
func (d *DiskLRU) Size() int64 { return d.sizeBytes.Load() }

func (d *DiskLRU) evictLoop() {
	t := time.NewTicker(diskLRUInterval)
	defer t.Stop()
	for {
		select {
		case <-d.closeCh:
			return
		case <-t.C:
			d.checkEviction()
		}
	}
}

// checkEviction walks entries oldest-first and asks each kind to free them
// until Size() fits under maxSize. Kinds that report ok=false (in-use) are
// skipped; they'll be retried on the next tick.
func (d *DiskLRU) checkEviction() {
	if d.sizeBytes.Load() <= d.maxSize {
		return
	}
	d.mu.Lock()
	type cand struct {
		kind, key string
		e         *diskEntry
	}
	cands := make([]cand, 0, len(d.entries))
	for ek, e := range d.entries {
		cands = append(cands, cand{ek.kind, ek.key, e})
	}
	kinds := maps.Clone(d.kinds)
	d.mu.Unlock()

	sort.Slice(cands, func(i, j int) bool {
		return cands[i].e.lastAccess.Load() < cands[j].e.lastAccess.Load()
	})
	for _, c := range cands {
		if d.sizeBytes.Load() <= d.maxSize {
			return
		}
		k, ok := kinds[c.kind]
		if !ok {
			continue
		}
		if !k.Evict(c.key) {
			continue
		}
		d.Forget(c.kind, c.key)
	}
}
