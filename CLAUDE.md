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

## Config

KDL file (see `examples/config.kdl`). Parsed via `kdl.Unmarshal` into tagged
structs in `config.go`. Secrets come from `${env:VAR}` interpolation in header
values — no separate secrets file.

## VCS

This repo uses `jj` (colocated with git). Load `/jj:workflow` skill before any
VCS operation.

## Platform

macOS (BSD userland). Don't assume GNU flags.
