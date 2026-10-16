// Package benchmark generates a deterministic open-loop workload. Arrival
// times do not slow down when the gateway is overloaded (no coordinated omission).
package benchmark

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"memorygateway/internal/protocol"
)

type Config struct {
	URL         string
	Requests    int
	Rate        float64
	Seed        int64
	BurstEvery  int
	BurstSize   int
	Timeout     time.Duration
	SLO         time.Duration
	MaxInflight int
}
type Sample struct {
	ID          int     `json:"id"`
	Class       string  `json:"class"`
	Priority    int     `json:"priority"`
	Status      int     `json:"status"`
	Outcome     string  `json:"outcome"`
	Backend     string  `json:"backend,omitempty"`
	LatencyMS   float64 `json:"latency_ms"`
	TTFTMS      float64 `json:"ttft_ms,omitempty"`
	QueueMS     float64 `json:"queue_ms,omitempty"`
	ScheduledMS float64 `json:"scheduled_ms"`
	LaunchLagMS float64 `json:"launch_lag_ms"`
}
type Percentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}
type Group struct {
	Offered   int         `json:"offered"`
	Completed int         `json:"completed"`
	WithinSLO int         `json:"within_slo"`
	LatencyMS Percentiles `json:"completed_latency_ms"`
	QueueMS   Percentiles `json:"completed_queue_ms"`
}
type Summary struct {
	Seed                 int64            `json:"seed"`
	Requests             int              `json:"requests"`
	ElapsedSeconds       float64          `json:"elapsed_seconds"`
	ArrivalWindowSeconds float64          `json:"arrival_window_seconds"`
	Outcomes             map[string]int   `json:"outcomes"`
	Statuses             map[int]int      `json:"statuses"`
	Completed            int              `json:"completed"`
	WithinSLO            int              `json:"within_slo"`
	SLOMS                float64          `json:"slo_ms"`
	CompletionRate       float64          `json:"completion_fraction"`
	Goodput              float64          `json:"slo_goodput_per_second"`
	LatencyMS            Percentiles      `json:"completed_latency_ms"`
	TTFTMS               Percentiles      `json:"completed_ttft_ms"`
	LaunchLagMS          Percentiles      `json:"launch_lag_ms"`
	Groups               map[string]Group `json:"groups"`
}
type item struct {
	request protocol.Request
	class   string
	at      time.Duration
}

func workload(c Config) []item {
	rng := rand.New(rand.NewSource(c.Seed))
	items := make([]item, c.Requests)
	for i := range items {
		words, out, class := 10+rng.Intn(20), 16+rng.Intn(24), "short"
		if rng.Intn(100) < 30 {
			words, out, class = 150+rng.Intn(150), 96+rng.Intn(96), "long"
		}
		priority := 0
		if rng.Intn(100) < 20 {
			priority = 2
		}
		slot := i
		if c.BurstEvery > 0 && c.BurstSize > 1 {
			// Collapse the first burst_size arrivals in each cycle to the same instant.
			base := i / c.BurstEvery * c.BurstEvery
			position := i % c.BurstEvery
			if position < c.BurstSize {
				slot = base
			}
		}
		items[i] = item{request: protocol.Request{Prompt: strings.Repeat("trace ", words), MaxTokens: out, Priority: priority}, class: class, at: time.Duration(float64(slot) / c.Rate * float64(time.Second))}
	}
	return items
}
func (c Config) Validate() error {
	if c.Requests < 1 || c.Requests > 1000000 || c.Rate <= 0 || c.Timeout <= 0 || c.SLO <= 0 || c.MaxInflight < 1 || c.BurstEvery < 0 || c.BurstSize < 0 || (c.BurstEvery > 0 && c.BurstSize > c.BurstEvery) {
		return fmt.Errorf("invalid benchmark settings")
	}
	return nil
}
func Run(ctx context.Context, c Config, output io.Writer) (Summary, error) {
	if err := c.Validate(); err != nil {
		return Summary{}, err
	}
	items := workload(c)
	client := &http.Client{Timeout: c.Timeout, Transport: &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 512}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	start := time.Now()
	samples := make([]Sample, c.Requests)
	sem := make(chan struct{}, c.MaxInflight)
	var wg sync.WaitGroup
	for i, it := range items {
		wait := time.Until(start.Add(it.at))
		if wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				wg.Wait()
				return Summary{}, ctx.Err()
			case <-t.C:
			}
		}
		samples[i] = Sample{ID: i, Class: it.class, Priority: it.request.Priority, ScheduledMS: float64(it.at) / float64(time.Millisecond)}
		select {
		case sem <- struct{}{}:
			wg.Add(1)
			go func(i int, it item) {
				defer wg.Done()
				defer func() { <-sem }()
				samples[i] = execute(ctx, client, c, start, it, samples[i])
			}(i, it)
		default:
			samples[i].Outcome = "loadgen_saturated"
		}
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	summary := summarize(c, samples, elapsed, items[len(items)-1].at.Seconds())
	enc := json.NewEncoder(output)
	for _, s := range samples {
		if err := enc.Encode(s); err != nil {
			return summary, err
		}
	}
	return summary, nil
}
func execute(ctx context.Context, client *http.Client, c Config, start time.Time, it item, s Sample) Sample {
	begin := time.Now()
	s.LaunchLagMS = float64(begin.Sub(start.Add(it.at))) / float64(time.Millisecond)
	body, _ := json.Marshal(it.request)
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(c.URL, "/")+"/v1/generate", bytes.NewReader(body))
	if err != nil {
		s.Outcome = "client_error"
		return s
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		s.Outcome = "transport_error"
		s.LatencyMS = float64(time.Since(begin)) / float64(time.Millisecond)
		return s
	}
	defer resp.Body.Close()
	s.Status = resp.StatusCode
	s.Backend = resp.Header.Get("X-Gateway-Backend")
	_, _ = fmt.Sscan(resp.Header.Get("X-Gateway-Queue-Ms"), &s.QueueMS)
	s.Outcome = "http_error"
	if resp.StatusCode == 200 {
		s.Outcome = "interrupted_stream"
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			var chunk protocol.Chunk
			if json.Unmarshal(scanner.Bytes(), &chunk) != nil || chunk.Error != "" {
				break
			}
			if chunk.Response != "" && s.TTFTMS == 0 {
				s.TTFTMS = float64(time.Since(begin)) / float64(time.Millisecond)
			}
			if chunk.Done {
				s.Outcome = "completed"
				break
			}
		}
	} else {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	}
	s.LatencyMS = float64(time.Since(begin)) / float64(time.Millisecond)
	return s
}
func percentiles(values []float64) Percentiles {
	if len(values) == 0 {
		return Percentiles{}
	}
	sort.Float64s(values)
	q := func(p float64) float64 { return values[max(0, int(math.Ceil(p*float64(len(values))))-1)] }
	return Percentiles{q(.5), q(.95), q(.99)}
}
func summarize(c Config, samples []Sample, elapsed, arrival float64) Summary {
	s := Summary{Seed: c.Seed, Requests: len(samples), ElapsedSeconds: elapsed, ArrivalWindowSeconds: arrival, Outcomes: map[string]int{}, Statuses: map[int]int{}, SLOMS: float64(c.SLO) / float64(time.Millisecond), Groups: map[string]Group{}}
	var latencies, ttfts, lags []float64
	groupLat, groupQueue := map[string][]float64{}, map[string][]float64{}
	for _, v := range samples {
		s.Outcomes[v.Outcome]++
		s.Statuses[v.Status]++
		lags = append(lags, v.LaunchLagMS)
		key := fmt.Sprintf("%s/priority_%d", v.Class, v.Priority)
		group := s.Groups[key]
		group.Offered++
		if v.Outcome == "completed" {
			s.Completed++
			group.Completed++
			latencies = append(latencies, v.LatencyMS)
			ttfts = append(ttfts, v.TTFTMS)
			groupLat[key] = append(groupLat[key], v.LatencyMS)
			groupQueue[key] = append(groupQueue[key], v.QueueMS)
			if v.LatencyMS <= s.SLOMS {
				s.WithinSLO++
				group.WithinSLO++
			}
		}
		s.Groups[key] = group
	}
	for key, g := range s.Groups {
		g.LatencyMS = percentiles(groupLat[key])
		g.QueueMS = percentiles(groupQueue[key])
		s.Groups[key] = g
	}
	s.LatencyMS = percentiles(latencies)
	s.TTFTMS = percentiles(ttfts)
	s.LaunchLagMS = percentiles(lags)
	s.CompletionRate = float64(s.Completed) / float64(len(samples))
	s.Goodput = float64(s.WithinSLO) / elapsed
	return s
}
