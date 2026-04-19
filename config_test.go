package main

import (
	"strings"
	"testing"
)

func TestParseConfig(t *testing.T) {
	t.Setenv("HF_TOKEN", "hf_xyz")
	t.Setenv("OPENAI_API_KEY", "sk_abc")

	src := `
cache {
    dir "/tmp/cache"
    max-size "10GB"
    chunk-size "2MiB"
}

service "huggingface" {
    hostname "hf"
    upstream "https://huggingface.co"
    cache true
    listen "http" "https"
    header "Authorization" "Bearer ${env:HF_TOKEN}"
}

service "openai" {
    hostname "openai"
    upstream "https://api.openai.com"
    header "Authorization" "Bearer ${env:OPENAI_API_KEY}"
}
`
	c, err := ParseConfig(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	if c.Cache.Dir != "/tmp/cache" {
		t.Errorf("cache dir: %q", c.Cache.Dir)
	}
	if c.Cache.MaxSize != 10*(1<<30) {
		t.Errorf("max-size: %d", c.Cache.MaxSize)
	}
	if c.Cache.ChunkSize != 2*(1<<20) {
		t.Errorf("chunk-size: %d", c.Cache.ChunkSize)
	}
	if len(c.Services) != 2 {
		t.Fatalf("services: %d", len(c.Services))
	}

	hf := c.Services[0]
	if hf.Name != "huggingface" || hf.Hostname != "hf" {
		t.Errorf("hf: %+v", hf)
	}
	if hf.Upstream.String() != "https://huggingface.co" {
		t.Errorf("hf upstream: %q", hf.Upstream)
	}
	if !hf.Cache {
		t.Errorf("hf cache should be true")
	}
	if len(hf.Listen) != 2 || hf.Listen[0] != "http" || hf.Listen[1] != "https" {
		t.Errorf("hf listen: %v", hf.Listen)
	}
	if len(hf.Headers) != 1 || hf.Headers[0].Value != "Bearer hf_xyz" {
		t.Errorf("hf headers: %+v", hf.Headers)
	}

	oa := c.Services[1]
	if len(oa.Listen) != 1 || oa.Listen[0] != "https" {
		t.Errorf("openai listen default: %v", oa.Listen)
	}
	if oa.Cache {
		t.Errorf("openai cache should default false")
	}
}

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1024", 1024},
		{"1KiB", 1024},
		{"1KB", 1024},
		{"1MiB", 1 << 20},
		{"1.5GB", int64(1.5 * (1 << 30))},
		{"100GB", 100 << 30},
	}
	for _, c := range cases {
		got, err := parseSize(c.in)
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: got %d, want %d", c.in, got, c.want)
		}
	}
}

func TestInterpolateEnv_Missing(t *testing.T) {
	_, err := interpolateEnv("Bearer ${env:NOPE_NOT_SET_xyz}")
	if err == nil {
		t.Error("expected error for missing env var")
	}
}

func TestParseConfig_UnknownKey(t *testing.T) {
	cases := map[string]string{
		"top-level": `
unknown-top-level "x"
service "x" {
    hostname "x"
    upstream "https://x.example"
}
`,
		"in-service": `
service "x" {
    hostname "x"
    upstream "https://x.example"
    typoed-key "x"
}
`,
		"in-cache": `
cache {
    dirr "/tmp"
}
service "x" {
    hostname "x"
    upstream "https://x.example"
}
`,
	}
	for name, src := range cases {
		_, err := ParseConfig(strings.NewReader(src))
		if err == nil {
			t.Errorf("%s: expected error for unknown key, got nil", name)
		}
	}
}
