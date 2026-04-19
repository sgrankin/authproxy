package main

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/sblinch/kdl-go"
)

type Config struct {
	Cache    CacheConfig
	Services []ServiceConfig
}

type CacheConfig struct {
	Dir       string
	MaxSize   int64
	ChunkSize int64
}

type ServiceConfig struct {
	Name     string
	Hostname string
	Upstream *url.URL
	Cache    bool
	Listen   []string
	Headers  []Header
}

type Header struct {
	Name, Value string
}

// raw* mirror the on-disk KDL shape; the public Config is produced after
// validation, size parsing, env interpolation, and URL parsing.
type rawConfig struct {
	Cache    rawCache     `kdl:"cache"`
	Services []rawService `kdl:"service,multiple"`
}

type rawCache struct {
	Dir       string `kdl:"dir"`
	MaxSize   string `kdl:"max-size"`
	ChunkSize string `kdl:"chunk-size"`
}

type rawService struct {
	Name     string            `kdl:",arg"`
	Hostname string            `kdl:"hostname"`
	Upstream string            `kdl:"upstream"`
	Cache    bool              `kdl:"cache"`
	Listen   []string          `kdl:"listen"`
	Headers  map[string]string `kdl:"header,multiple"`
}

func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		if abs, aerr := filepath.Abs(path); aerr == nil && abs != path {
			return nil, fmt.Errorf("%w (resolved to %s)", err, abs)
		}
		return nil, err
	}
	defer f.Close()
	return ParseConfig(f)
}

func ParseConfig(r io.Reader) (*Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	var raw rawConfig
	if err := kdl.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse kdl: %w", err)
	}

	c := &Config{
		Cache: CacheConfig{
			Dir:       "~/.cache/authproxy",
			MaxSize:   100 << 30, // 100 GiB
			ChunkSize: 4 << 20,   // 4 MiB
		},
	}
	if raw.Cache.Dir != "" {
		c.Cache.Dir = raw.Cache.Dir
	}
	if raw.Cache.MaxSize != "" {
		n, err := parseSize(raw.Cache.MaxSize)
		if err != nil {
			return nil, fmt.Errorf("cache.max-size: %w", err)
		}
		c.Cache.MaxSize = n
	}
	if raw.Cache.ChunkSize != "" {
		n, err := parseSize(raw.Cache.ChunkSize)
		if err != nil {
			return nil, fmt.Errorf("cache.chunk-size: %w", err)
		}
		c.Cache.ChunkSize = n
	}
	c.Cache.Dir = expandHome(c.Cache.Dir)

	if len(raw.Services) == 0 {
		return nil, fmt.Errorf("no services configured")
	}
	seenHost := map[string]bool{}
	for _, rs := range raw.Services {
		s, err := buildService(rs)
		if err != nil {
			return nil, err
		}
		if seenHost[s.Hostname] {
			return nil, fmt.Errorf("duplicate hostname %q", s.Hostname)
		}
		seenHost[s.Hostname] = true
		c.Services = append(c.Services, s)
	}
	return c, nil
}

func buildService(rs rawService) (ServiceConfig, error) {
	s := ServiceConfig{
		Name:     rs.Name,
		Hostname: rs.Hostname,
		Cache:    rs.Cache,
		Listen:   rs.Listen,
	}
	if s.Name == "" {
		return s, fmt.Errorf("service: missing name argument")
	}
	if s.Hostname == "" {
		return s, fmt.Errorf("service %q: missing hostname", s.Name)
	}
	if rs.Upstream == "" {
		return s, fmt.Errorf("service %q: missing upstream", s.Name)
	}
	u, err := url.Parse(rs.Upstream)
	if err != nil {
		return s, fmt.Errorf("service %q: upstream: %w", s.Name, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return s, fmt.Errorf("service %q: upstream %q missing scheme or host", s.Name, rs.Upstream)
	}
	s.Upstream = u
	for _, sch := range s.Listen {
		if sch != "http" && sch != "https" {
			return s, fmt.Errorf("service %q: listen: must be \"http\" or \"https\", got %q", s.Name, sch)
		}
	}
	if len(s.Listen) == 0 {
		s.Listen = []string{"https"}
	}
	for hname, hval := range rs.Headers {
		v, err := interpolateEnv(hval)
		if err != nil {
			return s, fmt.Errorf("service %q: header %q: %w", s.Name, hname, err)
		}
		s.Headers = append(s.Headers, Header{Name: hname, Value: v})
	}
	return s, nil
}

var sizeUnits = map[string]int64{
	"":  1,
	"b": 1,
	"k": 1 << 10, "kb": 1 << 10, "kib": 1 << 10,
	"m": 1 << 20, "mb": 1 << 20, "mib": 1 << 20,
	"g": 1 << 30, "gb": 1 << 30, "gib": 1 << 30,
	"t": 1 << 40, "tb": 1 << 40, "tib": 1 << 40,
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	num, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}
	unit := strings.ToLower(strings.TrimSpace(s[i:]))
	mul, ok := sizeUnits[unit]
	if !ok {
		return 0, fmt.Errorf("invalid size unit %q", unit)
	}
	return int64(num * float64(mul)), nil
}

var envRefRe = regexp.MustCompile(`\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}`)

func interpolateEnv(s string) (string, error) {
	var missing []string
	out := envRefRe.ReplaceAllStringFunc(s, func(match string) string {
		name := envRefRe.FindStringSubmatch(match)[1]
		v, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
		}
		return v
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("env vars not set: %s", strings.Join(missing, ", "))
	}
	return out, nil
}

func expandHome(p string) string {
	if !strings.HasPrefix(p, "~/") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	return filepath.Join(home, p[2:])
}
