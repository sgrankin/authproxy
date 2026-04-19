package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"path/filepath"

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
	if s.cache != nil {
		return s.cache.Handler(s.cfg.Upstream, s.cfg.Headers)
	}
	upstream := s.cfg.Upstream
	headers := s.cfg.Headers
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstream)
			pr.Out.Host = upstream.Host
			for _, h := range headers {
				pr.Out.Header.Set(h.Name, h.Value)
			}
		},
		ErrorLog: log.New(log.Writer(), s.cfg.Name+": ", log.LstdFlags),
	}
}
