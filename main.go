package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	configPath := flag.String("config", "config.kdl", "path to config file")
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

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var services []*Service
	for _, sc := range cfg.Services {
		svc := NewService(sc, stateDir)
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
}

func defaultStateDir() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "authproxy"), nil
}
