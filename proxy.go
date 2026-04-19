package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"path/filepath"
	"strings"
	"time"

	"tailscale.com/tsnet"
)

type Service struct {
	cfg       ServiceConfig
	ts        *tsnet.Server // nil in local mode
	localAddr string        // non-empty in local mode
	cache     *Cache
}

func NewService(cfg ServiceConfig, stateDir string, cache *Cache) *Service {
	s := &Service{
		cfg: cfg,
		ts: &tsnet.Server{
			Hostname: cfg.Hostname,
			Dir:      filepath.Join(stateDir, cfg.Hostname),
			Logf:     func(string, ...any) {},
		},
	}
	if cfg.Cache {
		s.cache = cache
	}
	return s
}

// NewLocalService binds the service to a plain localhost addr instead of tsnet.
// `listen` config is ignored — local always serves plain HTTP on the given addr.
func NewLocalService(cfg ServiceConfig, addr string, cache *Cache) *Service {
	s := &Service{cfg: cfg, localAddr: addr}
	if cfg.Cache {
		s.cache = cache
	}
	return s
}

func (s *Service) Start(ctx context.Context) error {
	handler := s.handler()
	if s.localAddr != "" {
		ln, err := net.Listen("tcp", s.localAddr)
		if err != nil {
			return fmt.Errorf("%s: listen: %w", s.cfg.Name, err)
		}
		go s.serve(ln, handler, "http")
		return nil
	}
	if err := s.ts.Start(); err != nil {
		return fmt.Errorf("%s: tsnet start: %w", s.cfg.Name, err)
	}
	if _, err := s.ts.Up(ctx); err != nil {
		return fmt.Errorf("%s: tsnet up: %w", s.cfg.Name, err)
	}
	for _, scheme := range s.cfg.Listen {
		ln, err := s.tsnetListen(scheme)
		if err != nil {
			return fmt.Errorf("%s: listen %s: %w", s.cfg.Name, scheme, err)
		}
		go s.serve(ln, handler, scheme)
	}
	return nil
}

func (s *Service) serve(ln net.Listener, handler http.Handler, scheme string) {
	srv := &http.Server{Handler: handler}
	if s.localAddr != "" {
		log.Printf("%s: local %s://%s -> %s", s.cfg.Name, scheme, ln.Addr(), s.cfg.Upstream)
	} else {
		log.Printf("%s: tsnet %s://%s%s -> %s", s.cfg.Name, scheme, s.cfg.Hostname, ln.Addr(), s.cfg.Upstream)
	}
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Printf("%s: serve %s: %v", s.cfg.Name, scheme, err)
	}
}

func (s *Service) Close() error {
	if s.ts != nil {
		return s.ts.Close()
	}
	return nil
}

func (s *Service) tsnetListen(scheme string) (net.Listener, error) {
	switch scheme {
	case "http":
		return s.ts.Listen("tcp", ":80")
	case "https":
		return s.ts.ListenTLS("tcp", ":443")
	default:
		return nil, fmt.Errorf("unknown scheme %q", scheme)
	}
}

func (s *Service) handler() http.Handler {
	var modify ModifyResponse
	var proxyRouter http.Handler
	if s.cfg.Adapter == "xet" {
		clientBase := s.clientBaseURL()
		modify = xetResponseModifier(clientBase)
		// Separate http.Client for /_proxy/ forwarding: unlike c.client we
		// don't want ResponseHeaderTimeout biting on streaming CAS body
		// fetches (reconstruction downloads can take a while).
		proxyRouter = xetProxyRouter(&http.Client{}, modify)
	}

	var inner http.Handler
	if s.cache != nil {
		inner = s.cache.Handler(s.cfg.Upstream, s.cfg.Headers, s.cfg.BlockResponseHeaders, modify)
	} else {
		upstream := s.cfg.Upstream
		headers := s.cfg.Headers
		block := s.cfg.BlockResponseHeaders
		inner = &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(upstream)
				pr.Out.Host = upstream.Host
				for _, h := range headers {
					pr.Out.Header.Set(h.Name, h.Value)
				}
			},
			ModifyResponse: func(resp *http.Response) error {
				if modify != nil {
					if err := modify(resp); err != nil {
						return err
					}
				}
				for _, name := range block {
					resp.Header.Del(name)
				}
				return nil
			},
			ErrorLog: log.New(log.Writer(), s.cfg.Name+": ", log.LstdFlags),
		}
	}

	if proxyRouter != nil {
		inner = dispatchProxyPrefix(proxyRouter, inner)
	}
	return accessLog(s.cfg.Name, inner)
}

// dispatchProxyPrefix routes requests under proxyPathPrefix to the adapter
// router; everything else goes to the normal handler.
func dispatchProxyPrefix(proxyRouter, normal http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, proxyPathPrefix) {
			proxyRouter.ServeHTTP(w, r)
			return
		}
		normal.ServeHTTP(w, r)
	})
}

// clientBaseURL returns scheme://host as clients see it. For tsnet services
// we pick the first configured listen scheme; for local services we use http.
// The scheme is what gets embedded in rewritten X-Xet-Cas-Endpoint values
// and reconstruction-JSON URLs.
func (s *Service) clientBaseURL() string {
	if s.localAddr != "" {
		return "http://" + s.localAddr
	}
	scheme := "https"
	if len(s.cfg.Listen) > 0 {
		scheme = s.cfg.Listen[0]
	}
	return scheme + "://" + s.cfg.Hostname
}

// accessLog wraps h with per-request logging:
//
//	<service> <client> <method> <path> <status> <bytes> <duration>
func accessLog(service string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lw := &loggingResponseWriter{ResponseWriter: w, status: 200}
		start := time.Now()
		h.ServeHTTP(lw, r)
		log.Printf("%s %s %s %s %d %d %s",
			service, clientAddr(r), r.Method, r.URL.RequestURI(),
			lw.status, lw.bytes, time.Since(start).Round(time.Millisecond))
	})
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (w *loggingResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *loggingResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

// Flush proxies through to the underlying writer if it supports flushing.
// Streaming responses (e.g. tee'd cache fills) need this to work.
func (w *loggingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func clientAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
