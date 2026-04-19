package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// xet adapter: lets HuggingFace clients use xet storage through the proxy.
//
// HF's HEAD responses on resolve-paths include:
//   - X-Xet-Hash: content hash of the file's xet reconstruction
//   - Link: <https://huggingface.co/api/models/<repo>/xet-read-token/<sha>>; rel="xet-auth"
//
// When xet is enabled, the client (hf_xet) exchanges the xet-auth URL for a
// short-lived CAS access token + endpoint. The returned X-Xet-Cas-Endpoint
// header points at cas-server.xethub.hf.co — a host the client normally
// talks to directly, bypassing this proxy.
//
// We keep clients on the proxy by:
//  1. Rewriting X-Xet-Cas-Endpoint to a /_proxy/<host>/ URL on our hostname.
//  2. Routing /_proxy/cas-server.xethub.hf.co/ requests to the real cas-server.
//  3. Parsing cas-server's reconstruction JSON responses and rewriting
//     cas-bridge.xethub.hf.co URLs (signed S3 URLs for chunk bytes) to
//     /_proxy/cas-bridge.xethub.hf.co/.
//  4. Routing /_proxy/cas-bridge.xethub.hf.co/ to the S3 CDN, preserving
//     presigned signatures (X-Amz-SignedHeaders=host stays valid because we
//     restore the original Host on the forwarded request).

// proxyPathPrefix is the path namespace reserved for cross-host forwarding.
// Leading underscore avoids collision with upstream API paths.
const proxyPathPrefix = "/_proxy/"

// xetAllowedHostSuffix is the SSRF guard: only hosts ending in this suffix
// can be targets of /_proxy/<host>/ routing. Narrow enough to keep the
// proxy from relaying to arbitrary third parties while forward-compatible
// with HF's occasional rehosting (cas-bridge → transfer, future renames).
const xetAllowedHostSuffix = ".xethub.hf.co"

// xetAllowedHosts enumerates hosts we *expect* to see in xet responses.
// Used by the body rewriter's string-replace rules (we need the exact host
// string to swap). Hosts we discover at runtime but haven't enumerated
// here still pass the SSRF guard — they just won't get rewritten in bodies.
var xetAllowedHosts = []string{
	"cas-server.xethub.hf.co",
	"transfer.xethub.hf.co", // where reconstruction responses point for chunk bytes
	"cas-bridge.xethub.hf.co",
}

func xetHostAllowed(host string) bool {
	return strings.HasSuffix(host, xetAllowedHostSuffix)
}

// xetResponseModifier rewrites headers and bodies so clients stay on the
// proxy for xet protocol traffic. clientBase is the scheme+host clients see
// (e.g. "https://hf.tail172cc.ts.net").
func xetResponseModifier(clientBase string) func(*http.Response) error {
	return func(resp *http.Response) error {
		rewriteXetCasURLHeader(clientBase, resp.Header)
		if shouldRewriteBody(resp) {
			if err := rewriteBodyHostURLs(clientBase, resp); err != nil {
				return err
			}
		}
		return nil
	}
}

// rewriteXetCasURLHeader rewrites X-Xet-Cas-Url from the raw xet hostname to
// the proxied equivalent. hf_hub reads this header to learn which CAS
// endpoint to use for reconstruction requests (constants.py:
// HUGGINGFACE_HEADER_X_XET_ENDPOINT = "X-Xet-Cas-Url").
func rewriteXetCasURLHeader(clientBase string, h http.Header) {
	v := h.Get("X-Xet-Cas-Url")
	if v == "" {
		return
	}
	u, err := url.Parse(v)
	if err != nil || !xetHostAllowed(u.Host) {
		return
	}
	h.Set("X-Xet-Cas-Url", clientBase+proxyPathPrefix+u.Host+u.Path)
}

// shouldRewriteBody returns true for responses whose JSON bodies may embed
// xet-host URLs we need to redirect through /_proxy/:
//   - xet-read-token responses (/api/.../xet-read-token/...) carry a casUrl
//     field that hf_xet may consult alongside the header.
//   - CAS reconstruction responses (/v1/reconstructions/<hash>) embed
//     cas-bridge presigned S3 URLs for chunk bytes.
func shouldRewriteBody(resp *http.Response) bool {
	if resp.Request == nil {
		return false
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		return false
	}
	p := resp.Request.URL.Path
	if strings.HasPrefix(p, "/v1/reconstructions/") {
		return true
	}
	if strings.Contains(p, "/xet-read-token/") {
		return true
	}
	return false
}

// rewriteBodyHostURLs replaces allowed xet-host URLs inside the response
// body with their /_proxy/<host>/ equivalents. Byte-level replace keeps us
// independent of the (private, evolving) JSON schemas.
func rewriteBodyHostURLs(clientBase string, resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	for _, host := range xetAllowedHosts {
		old := []byte("https://" + host)
		new := []byte(clientBase + proxyPathPrefix + host)
		body = bytes.ReplaceAll(body, old, new)
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	return nil
}

// xetProxyRouter handles /_proxy/<host>/<path> requests by forwarding to
// https://<host>/<path> (preserving method, headers, body, query). Enforces
// the xetAllowedHosts SSRF guard. Applies the response modifier so CAS
// responses get their embedded URLs rewritten.
func xetProxyRouter(client *http.Client, modifier func(*http.Response) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, proxyPathPrefix)
		slash := strings.IndexByte(rest, '/')
		if slash < 0 {
			http.Error(w, "proxy: malformed path", http.StatusBadRequest)
			return
		}
		host, path := rest[:slash], rest[slash:]
		if !xetHostAllowed(host) {
			http.Error(w, "proxy: host not allowed", http.StatusForbidden)
			return
		}

		target := &url.URL{
			Scheme:   "https",
			Host:     host,
			Path:     path,
			RawQuery: r.URL.RawQuery,
		}
		out, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), r.Body)
		if err != nil {
			http.Error(w, "proxy: build: "+err.Error(), http.StatusBadGateway)
			return
		}
		connHdr := r.Header.Get("Connection")
		for k, vs := range r.Header {
			if isHopByHop(k, connHdr) {
				continue
			}
			out.Header[k] = append([]string{}, vs...)
		}
		out.Host = host

		resp, err := client.Do(out)
		if err != nil {
			http.Error(w, "proxy: upstream: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		if modifier != nil {
			if err := modifier(resp); err != nil {
				http.Error(w, "proxy: modify: "+err.Error(), http.StatusBadGateway)
				return
			}
		}
		streamResponse(w, resp, nil)
	})
}
