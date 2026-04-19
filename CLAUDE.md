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

## Xet suppression (HF-specific)

HF's HEAD responses carry xet hints (`X-Xet-Hash`, `Link: …; rel="xet-auth"
/ "xet-reconstruction-info"`). When present and `hf_xet` is installed, the
client bypasses this proxy on the body fetch and talks directly to
`cas-server.xethub.hf.co` / `cas-bridge.xethub.hf.co`. That defeats both
the cache and — for locked-down clients — the network boundary.

The example config strips those headers via `block-response-header`,
forcing hf_hub onto the `http_get` path that goes through this proxy and
populates the cache. If you ever want xet passthrough, you'd need to also
proxy the two xethub hosts and rewrite CAS reconstruction responses; see
the discussion around commit 19424606 / cache.go's discover.

## Config

KDL file (see `examples/config.kdl`). Parsed via `kdl.Unmarshal` into tagged
structs in `config.go`. Secrets come from `${env:VAR}` interpolation in header
values — no separate secrets file.

## VCS

This repo uses `jj` (colocated with git). Load `/jj:workflow` skill before any
VCS operation.

## Platform

macOS (BSD userland). Don't assume GNU flags.
