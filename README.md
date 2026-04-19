# authproxy

An HTTP proxy that injects auth headers for outbound API calls. Each configured
service is exposed as its own [Tailscale `tsnet`](https://tailscale.com/kb/1244/tsnet)
node — clients hit `https://hf/...` instead of `https://huggingface.co/...` with
credentials. Optional per-service caching for content-addressable upstreams
(HuggingFace blobs, etc.).

Inspired by ["The HTTP proxy is the best secrets manager"](https://blog.exe.dev/http-proxy-secrets).

## What it does

- **Header injection** — outbound requests get `Authorization` (or any header)
  set per service, with `${env:VAR}` interpolation so secrets stay out of the
  config file.
- **Tailnet exposure** — one `tsnet.Server` per service, each its own hostname,
  bindable to `:80` and `:443` with auto-provisioned TLS via MagicDNS.
- **Optional caching** — slice/chunked store backed by per-chunk files. Honors
  strong `ETag` for revalidation; supports `Range` requests against the cache;
  per-blob LRU eviction; URL→ETag map persisted across restarts.

## Quick start

```kdl
// config.kdl
cache {
    dir "~/.cache/authproxy"
    max-size "100GB"
    chunk-size "4MiB"
}

service "huggingface" {
    hostname "hf"
    upstream "https://huggingface.co"
    cache true
    listen "http" "https"
    header "Authorization" "Bearer ${env:HF_TOKEN}"
}
```

```sh
HF_TOKEN=hf_xxx go run . -config config.kdl
```

For testing without joining a tailnet, use `-local` to bind on localhost:

```sh
go run . -config config.kdl -local -local-port 8080
```

## License

MIT — see [LICENSE](LICENSE).
