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
	cfg ServiceConfig
	ts  *tsnet.Server
}

func NewService(cfg ServiceConfig, stateDir string) *Service {
	return &Service{
		cfg: cfg,
		ts: &tsnet.Server{
			Hostname: cfg.Hostname,
			Dir:      filepath.Join(stateDir, cfg.Hostname),
			Logf:     func(string, ...any) {}, // silence verbose backend logs
		},
	}
}

func (s *Service) Start(ctx context.Context) error {
	if err := s.ts.Start(); err != nil {
		return fmt.Errorf("%s: tsnet start: %w", s.cfg.Name, err)
	}
	if _, err := s.ts.Up(ctx); err != nil {
		return fmt.Errorf("%s: tsnet up: %w", s.cfg.Name, err)
	}
	handler := s.handler()
	for _, scheme := range s.cfg.Listen {
		scheme := scheme
		ln, err := s.listen(scheme)
		if err != nil {
			return fmt.Errorf("%s: listen %s: %w", s.cfg.Name, scheme, err)
		}
		go func() {
			srv := &http.Server{Handler: handler}
			log.Printf("%s: listening on %s://%s%s", s.cfg.Name, scheme, s.cfg.Hostname, ln.Addr())
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
				log.Printf("%s: serve %s: %v", s.cfg.Name, scheme, err)
			}
		}()
	}
	return nil
}

func (s *Service) Close() error {
	return s.ts.Close()
}

func (s *Service) listen(scheme string) (net.Listener, error) {
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
