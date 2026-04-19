# authproxy

HTTP proxy that injects auth headers for outbound API calls. Each configured
service is exposed as its own Tailscale `tsnet` node (own hostname, default
ports), so clients hit `https://hf/...` instead of `https://huggingface.co/...`
with credentials.

Optional per-service caching via a chunked slice store (nginx-slice pattern).

## Stack

- `tailscale.com/tsnet` — one `tsnet.Server` per service, listens on :80 and :443.
- `github.com/sblinch/kdl-go` — config parser. Walk the `document.Document` tree
  directly; don't bother with the struct-tag unmarshal API.

## Config

KDL file. Sample at `examples/config.kdl`. Schema lives in `config.go`.

Secrets in header values: `${env:VAR}` interpolation. No keychain, no separate
secrets file. If env-var indirection becomes clumsy, add `${cmd:...}` later.

## Cache

Generic RFC 7234-ish: honor `Cache-Control: max-age` + strong `ETag`,
revalidate with `If-None-Match`. Skip caching for responses without a strong
validator. No service-specific logic — HF works because LFS blobs send
`Cache-Control: max-age=31536000` + sha256 `ETag`.

Chunked slice pattern: split blobs into fixed-size chunks, fetch missing chunks
via Range, singleflight per chunk, refcount in-flight chunks (never evict an
in-use chunk). LRU eviction by blob, not chunk.

## VCS

This repo uses `jj` (colocated with git). Load `/jj:workflow` skill before any
VCS operation.

## Platform

macOS (BSD userland). Don't assume GNU flags.
