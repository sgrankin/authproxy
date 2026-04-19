package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {
	configPath := flag.String("config", "config.kdl", "path to config file")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	fmt.Printf("loaded %d service(s):\n", len(cfg.Services))
	for _, s := range cfg.Services {
		fmt.Printf("  %s -> %s (cache=%v, listen=%v, %d header(s))\n",
			s.Hostname, s.Upstream, s.Cache, s.Listen, len(s.Headers))
	}
}
