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
	"github.com/sblinch/kdl-go/document"
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
	doc, err := kdl.Parse(r)
	if err != nil {
		return nil, fmt.Errorf("parse kdl: %w", err)
	}
	c := &Config{
		Cache: CacheConfig{
			Dir:       "~/.cache/authproxy",
			MaxSize:   100 << 30, // 100 GiB
			ChunkSize: 4 << 20,   // 4 MiB
		},
	}
	for _, n := range doc.Nodes {
		switch nodeName(n) {
		case "cache":
			if err := parseCacheBlock(n, &c.Cache); err != nil {
				return nil, err
			}
		case "service":
			s, err := parseServiceBlock(n)
			if err != nil {
				return nil, err
			}
			c.Services = append(c.Services, s)
		default:
			return nil, fmt.Errorf("unknown top-level node %q", nodeName(n))
		}
	}

	c.Cache.Dir = expandHome(c.Cache.Dir)
	if len(c.Services) == 0 {
		return nil, fmt.Errorf("no services configured")
	}
	seen := map[string]bool{}
	for _, s := range c.Services {
		if seen[s.Hostname] {
			return nil, fmt.Errorf("duplicate hostname %q", s.Hostname)
		}
		seen[s.Hostname] = true
	}
	return c, nil
}

func parseCacheBlock(n *document.Node, out *CacheConfig) error {
	for _, child := range n.Children {
		v, err := stringArg(child, 0)
		if err != nil {
			return fmt.Errorf("cache.%s: %w", nodeName(child), err)
		}
		switch nodeName(child) {
		case "dir":
			out.Dir = v
		case "max-size":
			n, err := parseSize(v)
			if err != nil {
				return fmt.Errorf("cache.max-size: %w", err)
			}
			out.MaxSize = n
		case "chunk-size":
			n, err := parseSize(v)
			if err != nil {
				return fmt.Errorf("cache.chunk-size: %w", err)
			}
			out.ChunkSize = n
		default:
			return fmt.Errorf("cache: unknown key %q", nodeName(child))
		}
	}
	return nil
}

func parseServiceBlock(n *document.Node) (ServiceConfig, error) {
	s := ServiceConfig{}
	name, err := stringArg(n, 0)
	if err != nil {
		return s, fmt.Errorf("service: %w", err)
	}
	s.Name = name

	for _, child := range n.Children {
		switch nodeName(child) {
		case "hostname":
			s.Hostname, err = stringArg(child, 0)
		case "upstream":
			raw, e := stringArg(child, 0)
			if e != nil {
				err = e
				break
			}
			s.Upstream, err = url.Parse(raw)
			if err == nil && (s.Upstream.Scheme == "" || s.Upstream.Host == "") {
				err = fmt.Errorf("upstream %q missing scheme or host", raw)
			}
		case "cache":
			s.Cache, err = boolArg(child, 0)
		case "listen":
			for i := range child.Arguments {
				v, e := stringArg(child, i)
				if e != nil {
					err = e
					break
				}
				if v != "http" && v != "https" {
					err = fmt.Errorf("listen: must be \"http\" or \"https\", got %q", v)
					break
				}
				s.Listen = append(s.Listen, v)
			}
		case "header":
			hname, e := stringArg(child, 0)
			if e != nil {
				err = e
				break
			}
			hval, e := stringArg(child, 1)
			if e != nil {
				err = e
				break
			}
			hval, err = interpolateEnv(hval)
			if err == nil {
				s.Headers = append(s.Headers, Header{Name: hname, Value: hval})
			}
		default:
			err = fmt.Errorf("unknown key %q", nodeName(child))
		}
		if err != nil {
			return s, fmt.Errorf("service %q: %w", s.Name, err)
		}
	}

	if s.Hostname == "" {
		return s, fmt.Errorf("service %q: missing hostname", s.Name)
	}
	if s.Upstream == nil {
		return s, fmt.Errorf("service %q: missing upstream", s.Name)
	}
	if len(s.Listen) == 0 {
		s.Listen = []string{"https"}
	}
	return s, nil
}

func nodeName(n *document.Node) string {
	if n.Name == nil {
		return ""
	}
	if s, ok := n.Name.Value.(string); ok {
		return s
	}
	return ""
}

func stringArg(n *document.Node, i int) (string, error) {
	if i >= len(n.Arguments) {
		return "", fmt.Errorf("missing argument %d", i)
	}
	v := n.Arguments[i].Value
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("argument %d: want string, got %T", i, v)
	}
	return s, nil
}

func boolArg(n *document.Node, i int) (bool, error) {
	if i >= len(n.Arguments) {
		return false, fmt.Errorf("missing argument %d", i)
	}
	b, ok := n.Arguments[i].Value.(bool)
	if !ok {
		return false, fmt.Errorf("argument %d: want bool, got %T", i, n.Arguments[i].Value)
	}
	return b, nil
}

var sizeUnits = map[string]int64{
	"":    1,
	"b":   1,
	"k":   1 << 10, "kb": 1 << 10, "kib": 1 << 10,
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
