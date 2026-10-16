package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"memorygateway/internal/config"
	"memorygateway/internal/protocol"
	"memorygateway/internal/simulator"
)

func setup(t *testing.T, kind string, h http.Handler, edit func(*config.Config)) (*Gateway, *httptest.Server) {
	t.Helper()
	up := httptest.NewServer(h)
	t.Cleanup(up.Close)
	c := config.Default()
	c.HealthInterval = config.Duration(10 * time.Millisecond)
	c.HealthTimeout = config.Duration(50 * time.Millisecond)
	c.TelemetryTTL = config.Duration(200 * time.Millisecond)
	c.CircuitCooldown = config.Duration(50 * time.Millisecond)
	c.CancelGrace = config.Duration(30 * time.Millisecond)
	c.Backends = []config.Backend{{ID: "a", URL: up.URL, Kind: kind, Capacity: 4096, MaxConcurrent: 8}}
	if edit != nil {
		edit(&c)
	}
	g, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	gw := httptest.NewServer(g.Handler())
	t.Cleanup(gw.Close)
	waitFor(t, func() bool { return g.scheduler.Snapshot().Backends[0].Healthy })
	return g, gw
}
func waitFor(t *testing.T, p func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !p() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for state")
		}
		time.Sleep(time.Millisecond)
	}
}
func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	r, err := http.Post(url+"/v1/generate", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Body.Close() })
	return r
}
func sim() http.Handler {
	return simulator.New(simulator.Config{Capacity: 4096, MaxConcurrent: 8, TokenDelay: time.Millisecond, OutputFraction: 1, AdminToken: "test"}).Handler()
}

func TestStreamingAndNonStreaming(t *testing.T) {
	g, gw := setup(t, "sim", sim(), nil)
	r := post(t, gw.URL, `{"prompt":"hello","max_tokens":3}`)
	if r.StatusCode != 200 || r.Header.Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("%d %v", r.StatusCode, r.Header)
	}
	d := json.NewDecoder(r.Body)
	chunks := 0
	for {
		var c protocol.Chunk
		if err := d.Decode(&c); err != nil {
			if err != io.EOF {
				t.Fatal(err)
			}
			break
		}
		chunks++
		if c.Done && c.OutputTokens != 3 {
			t.Fatal(c)
		}
	}
	if chunks != 4 {
		t.Fatalf("wanted three tokens plus terminal record, got %d", chunks)
	}
	r = post(t, gw.URL, `{"prompt":"hello","max_tokens":3,"stream":false}`)
	var c protocol.Chunk
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		t.Fatal(err)
	}
	if !c.Done || c.Response != "t0 t1 t2 " || !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("nonstream aggregation failed: %+v", c)
	}
	waitFor(t, func() bool { return g.scheduler.Snapshot().Backends[0].Reserved == 0 })
	metrics, err := http.Get(gw.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metrics.Body.Close()
	b, _ := io.ReadAll(metrics.Body)
	if !strings.Contains(string(b), "gateway_completed_total 2") || !strings.Contains(string(b), "gateway_ttft_seconds_count 2") {
		t.Fatal(string(b))
	}
}
func TestValidationAndRequestLimits(t *testing.T) {
	_, gw := setup(t, "sim", sim(), func(c *config.Config) { c.MaxBodyBytes = 256 })
	for _, tc := range []struct {
		body   string
		status int
	}{
		{`{"prompt":""}`, 400}, {`{"prompt":"hi","priority":3}`, 400}, {`{"prompt":"hi","max_tokens":-1}`, 400},
		{`{"prompt":"hi","max_tokens":99999}`, 400}, {`{"prompt":"hi","model":"other"}`, 400},
		{`{"prompt":"hi","extra":1}`, 400}, {`{"prompt":"hi"} {}`, 400}, {`{`, 400},
		{`{"prompt":"` + strings.Repeat("x", 300) + `"}`, 413},
	} {
		r := post(t, gw.URL, tc.body)
		if r.StatusCode != tc.status {
			t.Errorf("%s: status %d, want %d", tc.body, r.StatusCode, tc.status)
		}
		r.Body.Close()
	}
}
func TestOllamaMappingAndModelReadiness(t *testing.T) {
	var seen atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/tags" {
			fmt.Fprint(w, `{"models":[{"name":"demo:latest"}]}`)
			return
		}
		if r.URL.Path != "/api/generate" {
			http.NotFound(w, r)
			return
		}
		var body struct {
			Model   string
			Stream  bool
			Options struct {
				Predict int `json:"num_predict"`
				Context int `json:"num_ctx"`
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.Model != "demo" || !body.Stream || body.Options.Predict != 7 || body.Options.Context != 8192 {
			t.Errorf("bad mapping: %+v", body)
		}
		seen.Add(1)
		fmt.Fprintln(w, `{"response":"hello","done":false}`)
		fmt.Fprintln(w, `{"done":true,"eval_count":1}`)
	})
	_, gw := setup(t, "ollama", h, nil)
	r := post(t, gw.URL, `{"prompt":"hi","max_tokens":7,"stream":false}`)
	var c protocol.Chunk
	_ = json.NewDecoder(r.Body).Decode(&c)
	if !c.Done || c.Response != "hello" || seen.Load() != 1 {
		t.Fatal(c)
	}
}
func TestInterruptedStreamsAreNotReplayed(t *testing.T) {
	var calls atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			fmt.Fprint(w, `{"capacity_tokens":4096,"used_tokens":0}`)
			return
		}
		calls.Add(1)
		fmt.Fprintln(w, `{"response":"partial","done":false}`)
		_ = http.NewResponseController(w).Flush()
	})
	g, gw := setup(t, "sim", h, nil)
	r := post(t, gw.URL, `{"prompt":"hi","max_tokens":5}`)
	b, _ := io.ReadAll(r.Body)
	if calls.Load() != 1 || !strings.Contains(string(b), "upstream_stream_interrupted") {
		t.Fatalf("calls=%d body=%s", calls.Load(), b)
	}
	waitFor(t, func() bool { return g.scheduler.Snapshot().Backends[0].CircuitOpen })
	waitFor(t, func() bool { return g.scheduler.Snapshot().Backends[0].Reserved == 0 })
}
func TestCancellationPropagatesAndReservationsReconcile(t *testing.T) {
	stopped := make(chan struct{}, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			fmt.Fprint(w, `{"capacity_tokens":4096,"used_tokens":0}`)
			return
		}
		fmt.Fprintln(w, `{"response":"first","done":false}`)
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
		stopped <- struct{}{}
	})
	g, gw := setup(t, "sim", h, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", gw.URL+"/v1/generate", strings.NewReader(`{"prompt":"hi","max_tokens":10}`))
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(r.Body)
	if !scanner.Scan() {
		t.Fatal("missing first token")
	}
	cancel()
	r.Body.Close()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive cancellation")
	}
	waitFor(t, func() bool { b := g.scheduler.Snapshot().Backends[0]; return b.Reserved == 0 && b.Running == 0 })
}
func TestDeadlineStopsHangingBackend(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			fmt.Fprint(w, `{"capacity_tokens":4096,"used_tokens":0}`)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	})
	g, gw := setup(t, "sim", h, func(c *config.Config) { c.RequestTimeout = config.Duration(40 * time.Millisecond) })
	r := post(t, gw.URL, `{"prompt":"hi","max_tokens":5}`)
	if r.StatusCode != 504 {
		t.Fatalf("deadline status=%d", r.StatusCode)
	}
	waitFor(t, func() bool { return g.scheduler.Snapshot().Backends[0].Reserved == 0 })
}
func TestBackendCrashAndAutomaticRecovery(t *testing.T) {
	g, gw := setup(t, "sim", sim(), func(c *config.Config) { c.FailureThreshold = 1 })
	url := g.cfg.Backends[0].URL + "/admin/fault"
	req, _ := http.NewRequest("POST", url, strings.NewReader(`{"down_ms":80}`))
	req.Header.Set("Authorization", "Bearer test")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	waitFor(t, func() bool { return g.scheduler.Snapshot().Backends[0].CircuitOpen })
	waitFor(t, func() bool { b := g.scheduler.Snapshot().Backends[0]; return b.Healthy && !b.CircuitOpen })
	r = post(t, gw.URL, `{"prompt":"recovered","max_tokens":1,"stream":false}`)
	var c protocol.Chunk
	_ = json.NewDecoder(r.Body).Decode(&c)
	if !c.Done {
		t.Fatal("recovered backend failed generation")
	}
}
