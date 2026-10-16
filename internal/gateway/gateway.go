package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"memorygateway/internal/config"
	"memorygateway/internal/metrics"
	"memorygateway/internal/protocol"
	"memorygateway/internal/scheduler"
)

type Gateway struct {
	cfg       config.Config
	scheduler *scheduler.Scheduler
	metrics   *metrics.Metrics
	client    *http.Client
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	ctx       context.Context
}

func New(c config.Config) (*Gateway, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{cfg: c, scheduler: scheduler.New(c), metrics: metrics.New(), cancel: cancel, ctx: ctx,
		client: &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext, MaxIdleConns: 128, MaxIdleConnsPerHost: 32, IdleConnTimeout: 60 * time.Second, ResponseHeaderTimeout: c.RequestTimeout.Value()}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	for _, b := range c.Backends {
		g.wg.Add(1)
		go g.healthLoop(ctx, b)
	}
	return g, nil
}
func (g *Gateway) Close() {
	g.cancel()
	g.scheduler.Close()
	g.wg.Wait()
	g.client.CloseIdleConnections()
}
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/generate", g.generate)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		s := g.scheduler.Snapshot()
		code := 503
		for _, b := range s.Backends {
			if b.Healthy && b.Fresh && !b.CircuitOpen && b.Capacity > 0 {
				code = 200
				break
			}
		}
		writeJSON(w, code, s)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		g.metrics.Write(w, g.scheduler.Snapshot())
	})
	return mux
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (g *Gateway) reject(w http.ResponseWriter, code int, reason string) {
	g.metrics.Inc("gateway_rejected_" + reason + "_total")
	if code == 429 || code == 503 {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, code, map[string]string{"error": reason})
}

// Estimate uses a byte heuristic plus template overhead, with a configurable
// safety multiplier. It is a logical token budget, not a measurement of VRAM.
func Estimate(prompt string, maxOutput int, multiplier float64) int {
	input := (len(prompt)+2)/3 + 16
	return int(math.Ceil(float64(input+maxOutput) * multiplier))
}
func (g *Gateway) generate(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	g.metrics.Inc("gateway_requests_total")
	defer func() { g.metrics.Observe("gateway_request_duration_seconds", time.Since(start)) }()
	r.Body = http.MaxBytesReader(w, r.Body, g.cfg.MaxBodyBytes)
	var req protocol.Request
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			g.reject(w, 413, "body_too_large")
		} else {
			g.reject(w, 400, "invalid_json")
		}
		return
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			g.reject(w, 413, "body_too_large")
		} else {
			g.reject(w, 400, "invalid_json")
		}
		return
	}
	if strings.TrimSpace(req.Prompt) == "" || req.Priority < 0 || req.Priority > 2 {
		g.reject(w, 400, "invalid_request")
		return
	}
	if req.Model == "" {
		req.Model = g.cfg.Model
	}
	if req.Model != g.cfg.Model {
		g.reject(w, 400, "unsupported_model")
		return
	}
	if req.MaxTokens == 0 {
		req.MaxTokens = g.cfg.DefaultOutputTokens
	}
	if req.MaxTokens < 1 || req.MaxTokens > g.cfg.MaxOutputTokens {
		g.reject(w, 400, "invalid_max_tokens")
		return
	}
	demand := Estimate(req.Prompt, req.MaxTokens, g.cfg.EstimateMultiplier)
	if demand > g.cfg.MaxContextTokens {
		g.reject(w, 413, "context_too_large")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), g.cfg.RequestTimeout.Value())
	defer cancel()
	// Gateway shutdown also cancels active upstream work.
	stop := context.AfterFunc(g.ctx, cancel)
	defer stop()
	lease, err := g.scheduler.Acquire(ctx, demand, req.Priority)
	if err != nil {
		switch {
		case errors.Is(err, scheduler.ErrFull):
			g.reject(w, 429, "queue_full")
		case errors.Is(err, scheduler.ErrTooLarge):
			g.reject(w, 413, "capacity_too_large")
		case errors.Is(err, scheduler.ErrQueueTimeout):
			g.reject(w, 503, "queue_timeout")
		case errors.Is(err, context.Canceled):
			g.reject(w, 408, "canceled")
		case errors.Is(err, context.DeadlineExceeded):
			g.reject(w, 504, "deadline_exceeded")
		default:
			g.reject(w, 503, "unavailable")
		}
		return
	}
	g.metrics.Observe("gateway_queue_wait_seconds", lease.QueueWait)
	g.metrics.Observe(fmt.Sprintf("gateway_queue_wait_priority_%d_seconds", req.Priority), lease.QueueWait)
	uncertain := false
	defer func() {
		if uncertain {
			g.scheduler.Quarantine(lease.Backend.ID)
			// Do not release uncertain backend work straight into the free pool.
			time.AfterFunc(g.cfg.CancelGrace.Value(), lease.Release)
		} else {
			lease.Release()
		}
	}()
	if ctx.Err() != nil {
		g.reject(w, 408, "canceled")
		return
	}
	w.Header().Set("X-Gateway-Backend", lease.Backend.ID)
	w.Header().Set("X-Gateway-Reserved-Tokens", fmt.Sprint(demand))
	w.Header().Set("X-Gateway-Queue-Ms", fmt.Sprintf("%.3f", float64(lease.QueueWait)/float64(time.Millisecond)))
	var body []byte
	path := "/v1/generate"
	if lease.Backend.Kind == "ollama" {
		path = "/api/generate"
		body, _ = json.Marshal(map[string]any{"model": req.Model, "prompt": req.Prompt, "stream": true, "options": map[string]int{"num_predict": req.MaxTokens, "num_ctx": g.cfg.MaxContextTokens}})
	} else {
		upstreamRequest := req
		upstreamRequest.Stream = nil
		body, _ = json.Marshal(upstreamRequest)
	}
	upstream, _ := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(lease.Backend.URL, "/")+path, bytes.NewReader(body))
	upstream.Header.Set("Content-Type", "application/json")
	resp, err := g.client.Do(upstream)
	if err != nil {
		uncertain = true
		g.scheduler.Failure(lease.Backend.ID)
		g.metrics.Inc("gateway_backend_failures_total")
		code := 502
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code = 504
		}
		g.reject(w, code, "upstream_unavailable")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			g.scheduler.Failure(lease.Backend.ID)
		}
		g.metrics.Inc("gateway_backend_failures_total")
		g.reject(w, 502, "upstream_rejected")
		return
	}
	stream := req.Stream == nil || *req.Stream
	if stream {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Cache-Control", "no-store")
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), int(g.cfg.MaxBodyBytes))
	first, done, committed := true, false, false
	var text strings.Builder
	var last protocol.Chunk
	for scanner.Scan() {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var chunk protocol.Chunk
		if err = json.Unmarshal(scanner.Bytes(), &chunk); err != nil {
			break
		}
		if chunk.Error != "" {
			err = errors.New("backend stream error")
			break
		}
		if first && chunk.Response != "" {
			g.metrics.Observe("gateway_ttft_seconds", time.Since(start))
			first = false
		}
		if stream {
			if err = json.NewEncoder(w).Encode(chunk); err != nil {
				break
			}
			committed = true
			if err = http.NewResponseController(w).Flush(); err != nil {
				break
			}
		} else {
			if text.Len()+len(chunk.Response) > int(g.cfg.MaxBodyBytes) {
				err = errors.New("response too large")
				break
			}
			text.WriteString(chunk.Response)
		}
		last = chunk
		if chunk.Done {
			done = true
			break
		}
	}
	if done {
		g.scheduler.Success(lease.Backend.ID)
		if !stream {
			last.Response = text.String()
			if ctx.Err() != nil {
				g.metrics.Inc("gateway_canceled_total")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(last); err != nil {
				g.metrics.Inc("gateway_canceled_total")
				return
			}
		}
		g.metrics.Inc("gateway_completed_total")
		g.metrics.Add("gateway_completed_estimated_tokens_total", uint64(demand))
		g.metrics.Add("gateway_completed_observed_tokens_total", uint64(max(0, last.PromptTokens)+max(0, last.OutputTokens)))
		g.metrics.Observe("gateway_completion_seconds", time.Since(start))
		return
	}
	_ = err // The public response deliberately avoids leaking upstream internals.
	uncertain = true
	g.scheduler.Failure(lease.Backend.ID)
	g.metrics.Inc("gateway_stream_failures_total")
	if ctx.Err() != nil {
		g.metrics.Inc("gateway_canceled_total")
	}
	if committed {
		_ = json.NewEncoder(w).Encode(protocol.Chunk{Error: "upstream_stream_interrupted", Done: false})
		_ = http.NewResponseController(w).Flush()
	} else {
		code := 502
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code = 504
		}
		g.reject(w, code, "upstream_stream_interrupted")
	}
}

func (g *Gateway) healthLoop(ctx context.Context, b config.Backend) {
	defer g.wg.Done()
	tick := time.NewTicker(g.cfg.HealthInterval.Value())
	defer tick.Stop()
	for {
		probeCtx, cancel := context.WithTimeout(ctx, g.cfg.HealthTimeout.Value())
		path := "/health"
		if b.Kind == "ollama" {
			path = "/api/tags"
		}
		req, _ := http.NewRequestWithContext(probeCtx, http.MethodGet, strings.TrimRight(b.URL, "/")+path, nil)
		resp, err := g.client.Do(req)
		capacity, used := b.Capacity, 0
		if err == nil {
			if resp.StatusCode != 200 {
				err = errors.New("unhealthy backend")
			} else if b.Kind == "sim" {
				var health struct {
					Capacity int `json:"capacity_tokens"`
					Used     int `json:"used_tokens"`
				}
				err = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&health)
				capacity = health.Capacity
				used = health.Used
				if capacity < 0 || used < 0 {
					err = errors.New("invalid telemetry")
				}
			} else {
				var tags struct {
					Models []struct {
						Name string `json:"name"`
					} `json:"models"`
				}
				err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tags)
				found := false
				for _, m := range tags.Models {
					if m.Name == g.cfg.Model || m.Name == g.cfg.Model+":latest" {
						found = true
					}
				}
				if !found {
					err = errors.New("configured model is not installed")
				}
			}
			resp.Body.Close()
		}
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				g.scheduler.HealthFailure(b.ID)
				g.metrics.Inc("gateway_health_failures_total")
			}
		} else {
			g.scheduler.UpdateHealth(b.ID, capacity, used)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
