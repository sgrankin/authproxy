package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	configPath := flag.String("config", "config.kdl", "path to config file")
	local := flag.Bool("local", false, "bind on localhost ports instead of tsnet (for testing)")
	localPort := flag.Int("local-port", 8080, "first localhost port when -local is set; subsequent services use port+1, port+2, ...")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	stateDir, err := defaultStateDir()
	if err != nil {
		log.Fatalf("state dir: %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		log.Fatalf("mkdir state: %v", err)
	}

	if err := os.MkdirAll(cfg.Cache.Dir, 0o700); err != nil {
		log.Fatalf("mkdir cache: %v", err)
	}
	cache, err := NewCache(cfg.Cache.Dir, cfg.Cache.MaxSize, cfg.Cache.ChunkSize)
	if err != nil {
		log.Fatalf("cache: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var services []*Service
	for i, sc := range cfg.Services {
		var svc *Service
		if *local {
			addr := fmt.Sprintf("127.0.0.1:%d", *localPort+i)
			svc = NewLocalService(sc, addr, cache)
		} else {
			svc = NewService(sc, stateDir, cache)
		}
		if err := svc.Start(ctx); err != nil {
			log.Fatalf("%s: %v", sc.Name, err)
		}
		services = append(services, svc)
	}

	<-ctx.Done()
	log.Printf("shutting down")
	for _, s := range services {
		s.Close()
	}
	cache.Close()
}

func defaultStateDir() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "authproxy"), nil
}
