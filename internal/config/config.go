package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"
)

type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string: %w", err)
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}
func (d Duration) Value() time.Duration { return time.Duration(d) }

type Backend struct {
	ID            string `json:"id"`
	URL           string `json:"url"`
	Kind          string `json:"kind"`
	Capacity      int    `json:"capacity_tokens"`
	MaxConcurrent int    `json:"max_concurrent"`
}

type Config struct {
	Listen              string    `json:"listen"`
	Model               string    `json:"model"`
	Policy              string    `json:"policy"`
	QueueSize           int       `json:"queue_size"`
	QueueTimeout        Duration  `json:"queue_timeout"`
	RequestTimeout      Duration  `json:"request_timeout"`
	HealthInterval      Duration  `json:"health_interval"`
	HealthTimeout       Duration  `json:"health_timeout"`
	TelemetryTTL        Duration  `json:"telemetry_ttl"`
	CircuitCooldown     Duration  `json:"circuit_cooldown"`
	CancelGrace         Duration  `json:"cancel_grace"`
	AgingInterval       Duration  `json:"aging_interval"`
	StarvationLimit     Duration  `json:"starvation_limit"`
	FailureThreshold    int       `json:"failure_threshold"`
	MaxBodyBytes        int64     `json:"max_body_bytes"`
	MaxOutputTokens     int       `json:"max_output_tokens"`
	DefaultOutputTokens int       `json:"default_output_tokens"`
	MaxContextTokens    int       `json:"max_context_tokens"`
	EstimateMultiplier  float64   `json:"estimate_multiplier"`
	Backends            []Backend `json:"backends"`
}

func Default() Config {
	return Config{Listen: "127.0.0.1:8080", Model: "demo", Policy: "memory", QueueSize: 128,
		QueueTimeout: Duration(5 * time.Second), RequestTimeout: Duration(30 * time.Second),
		HealthInterval: Duration(500 * time.Millisecond), HealthTimeout: Duration(400 * time.Millisecond),
		TelemetryTTL: Duration(2 * time.Second), CircuitCooldown: Duration(2 * time.Second),
		CancelGrace: Duration(1 * time.Second), AgingInterval: Duration(500 * time.Millisecond),
		StarvationLimit: Duration(3 * time.Second), FailureThreshold: 3, MaxBodyBytes: 1 << 20,
		MaxOutputTokens: 2048, DefaultOutputTokens: 128, MaxContextTokens: 8192, EstimateMultiplier: 1.15}
}

func Load(path string) (Config, error) {
	c := Default()
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return c, err
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.Policy != "memory" && c.Policy != "round_robin" && c.Policy != "least_outstanding" {
		return fmt.Errorf("unknown policy %q", c.Policy)
	}
	if c.Listen == "" || c.Model == "" || len(c.Backends) == 0 {
		return fmt.Errorf("listen, model and backends are required")
	}
	if c.QueueSize < 0 || c.FailureThreshold < 1 || c.MaxBodyBytes < 1 || c.MaxContextTokens < 1 || c.MaxOutputTokens < 1 || c.DefaultOutputTokens < 1 || c.DefaultOutputTokens > c.MaxOutputTokens || c.EstimateMultiplier < 1 || c.EstimateMultiplier > 10 {
		return fmt.Errorf("invalid limits or estimate multiplier")
	}
	for _, d := range []Duration{c.QueueTimeout, c.RequestTimeout, c.HealthInterval, c.HealthTimeout, c.TelemetryTTL, c.CircuitCooldown, c.CancelGrace, c.AgingInterval, c.StarvationLimit} {
		if d <= 0 {
			return fmt.Errorf("durations must be positive")
		}
	}
	if c.TelemetryTTL <= c.HealthInterval {
		return fmt.Errorf("telemetry_ttl must exceed health_interval")
	}
	seen := map[string]bool{}
	for _, b := range c.Backends {
		u, err := url.Parse(b.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return fmt.Errorf("backend %q needs an HTTP origin URL", b.ID)
		}
		if b.ID == "" || seen[b.ID] || b.Capacity < 1 || b.MaxConcurrent < 1 || (b.Kind != "sim" && b.Kind != "ollama") {
			return fmt.Errorf("invalid or duplicate backend %q", b.ID)
		}
		seen[b.ID] = true
	}
	return nil
}
