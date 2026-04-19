# authproxy

HTTP proxy that injects auth headers for outbound API calls. Each configured
service is exposed as its own Tailscale `tsnet` node, so clients hit
`https://hf/...` instead of `https://huggingface.co/...` with credentials.
Optional per-service caching for content-addressable upstreams (HuggingFace,
etc.).

## Cache architecture

The cache is generic — no service-specific logic. It works for any upstream
that returns a strong `ETag` and supports `Range`; HF works because LFS blobs
do both. Responses without a strong validator pass through without caching.

Layout: per-blob directory with `meta.json` + `chunks/<idx>` files. Chunks are
fetched on-demand via `Range`; concurrent requests for the same chunk dedupe
via per-chunk singleflight. New blobs additionally trigger a streaming-tee
fill (one big GET, sliced into chunks as bytes arrive). Per-blob refcount
gates LRU eviction — in-use blobs never get evicted.

### Shared LRU budget

Both the blob `Cache` and the xet `chunkCache` register as "kinds" on a
single `DiskLRU` (see `disklru.go`). The LRU maintains one access-time-
ordered pool across all entries and evicts the globally-oldest first,
regardless of which layer owns it — so a stale xet chunk can make room
for a hot HTTP blob, and vice versa. Combined disk usage stays under
`cache.max-size`, not 2× that.

**Freshness model:** every request issues a HEAD with `If-None-Match` to
revalidate. We do **not** honor `Cache-Control: max-age` — always check
upstream. Trade one round-trip for correctness; revisit if HEAD overhead
becomes meaningful in practice.

**Redirect handling:** discover's HEAD does *not* auto-follow. On a 3xx we do
a second HEAD against `Location` to capture the final resource's size /
content-type / ETag, then merge the origin's response headers (for
provider-specific metadata like HF's `X-Repo-Commit`, `X-Linked-Etag`,
`X-Linked-Size`) with the final response's. `Location` is stripped before
storage — we always serve 200 from cache. Body GETs still auto-follow via a
separate `http.Client`.

Canonical ETag (served to clients, used as cache key) prefers
`X-Linked-Etag` over the final response's ETag — HF's linked-etag is a
content hash that's stable across CDN boundary crossings where the CDN's
ETag is not. The CDN's ETag is stashed as `UpstreamETag` and used for
`If-Match` on chunked Range GETs (the CDN doesn't know X-Linked-Etag).

## Xet adapter (HF-specific)

HF's HEAD responses advertise xet storage via `X-Xet-Hash` and a `Link`
header with `rel="xet-auth"` / `rel="xet-reconstruction-info"`. Natively,
`hf_xet` uses these to bypass `huggingface.co` and talk directly to
`cas-server.xethub.hf.co` (reconstruction metadata) and
`transfer.xethub.hf.co` / `cas-bridge.xethub.hf.co` (chunk bytes via
presigned URLs). That's ~7× faster than the HTTP/LFS path but bypasses
this proxy.

Enable `adapter "xet"` on a service to keep that traffic on the proxy:

1. `/_proxy/<host>/<path>` requests are forwarded to `https://<host>/<path>`
   for any host under `.xethub.hf.co` (SSRF guard via suffix match).
2. The `X-Xet-Cas-Url` header on `xet-read-token` responses gets rewritten
   to `<clientBase>/_proxy/cas-server.xethub.hf.co`. Since `hf_xet` reads
   this header to learn its CAS endpoint, the proxy becomes invisible
   infrastructure to the client.
3. Response bodies on `xet-read-token` and `/v1/reconstructions/...` are
   byte-scanned and rewritten to redirect any `https://<xet-host>/…` URL
   through `/_proxy/<xet-host>/…`. The S3 presigned signatures stay valid:
   they sign `host` only, and the proxy restores the original `Host` on
   the forwarded request.

See `xet.go` / `chunkcache.go`.

### Chunk cache

Chunk GETs (paths `/xorbs/...` and `/xet-bridge-us/...`) go through a
content-addressable cache keyed by `sha256(host + path + X-Xet-Signed-Range)`.
Hits skip the upstream entirely. Different ranges of the same xorb get
separate entries (same as `hf_xet`'s client-side cache). The cache uses
`sync.Map`-style singleflight to dedup concurrent fetches and independent
LRU eviction against `cache.max-size`. Fills stream via `io.TeeReader` —
client sees bytes as they arrive; disk write runs concurrently; temp files
are cleaned up on client disconnect or copy error.

### One tsnet hostname, path-multiplexed

All xet traffic goes through the same hostname as the parent service
(`hf.tail172cc.ts.net`). Extra xet hostnames were rejected because each
tsnet device needs Tailscale admin approval — the path-prefix approach
stays under one approval and doesn't clutter the admin view.

## Config

KDL file (see `examples/config.kdl`). Parsed via `kdl.Unmarshal` into tagged
structs in `config.go`. Secrets come from `${env:VAR}` interpolation in header
values — no separate secrets file.

## VCS

This repo uses `jj` (colocated with git). Load `/jj:workflow` skill before any
VCS operation.

## Platform

macOS (BSD userland). Don't assume GNU flags.
