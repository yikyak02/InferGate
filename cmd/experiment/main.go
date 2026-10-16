// experiment gives each policy a fresh three-backend simulation and the exact
// same seeded arrival trace. It never connects to a real inference service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"memorygateway/internal/benchmark"
	"memorygateway/internal/config"
	"memorygateway/internal/gateway"
	"memorygateway/internal/simulator"
)

func main() {
	n := flag.Int("requests", 400, "requests per policy")
	rate := flag.Float64("rate", 120, "arrival rate per second")
	seed := flag.Int64("seed", 42, "workload seed")
	out := flag.String("output", "results/comparison", "output directory")
	faultAt := flag.Duration("fault-at", 0, "crash backend B this long after workload start; 0 disables")
	faultFor := flag.Duration("fault-for", time.Second, "backend B downtime")
	fraction := flag.Float64("output-fraction", 1, "simulator output fraction for estimation sensitivity")
	multiplier := flag.Float64("estimate-multiplier", 1.15, "gateway estimator safety multiplier")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *n, *rate, *seed, *out, *faultAt, *faultFor, *fraction, *multiplier); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context, n int, rate float64, seed int64, out string, faultAt, faultFor time.Duration, fraction, multiplier float64) error {
	bc := benchmark.Config{Requests: n, Rate: rate, Seed: seed, BurstEvery: 100, BurstSize: 20, Timeout: 12 * time.Second, SLO: time.Second, MaxInflight: 4096}
	if err := bc.Validate(); err != nil {
		return err
	}
	if faultAt < 0 || faultFor < 0 || faultFor > 5*time.Minute {
		return fmt.Errorf("invalid fault timing")
	}
	if err := os.MkdirAll(out, 0755); err != nil {
		return err
	}
	var summaries []map[string]any
	fmt.Println("policy               completed  within-SLO  goodput/s  p95-ms  HTTP-errors  interrupted")
	for _, policy := range []string{"round_robin", "least_outstanding", "memory"} {
		s, err := runPolicy(ctx, bc, policy, out, faultAt, faultFor, fraction, multiplier)
		if err != nil {
			return err
		}
		summaries = append(summaries, map[string]any{"policy": policy, "summary": s})
		fmt.Printf("%-20s %5d/%-5d %5d       %7.2f  %7.1f   %5d       %5d\n", policy, s.Completed, n, s.WithinSLO, s.Goodput, s.LatencyMS.P95, s.Outcomes["http_error"], s.Outcomes["interrupted_stream"])
	}
	data, _ := json.MarshalIndent(map[string]any{"environment": "synthetic simulation; not GPU measurements", "requests": n, "rate": rate, "seed": seed, "fault_at": faultAt.String(), "fault_for": faultFor.String(), "output_fraction": fraction, "estimate_multiplier": multiplier, "policies": summaries}, "", "  ")
	return os.WriteFile(filepath.Join(out, "comparison.json"), append(data, '\n'), 0644)
}
func runPolicy(ctx context.Context, bc benchmark.Config, policy, out string, faultAt, faultFor time.Duration, fraction, multiplier float64) (benchmark.Summary, error) {
	c := config.Default()
	c.Policy = policy
	c.QueueSize = 128
	c.EstimateMultiplier = multiplier
	var sims []*httptest.Server
	defer func() {
		for _, s := range sims {
			s.Close()
		}
	}()
	for i, capacity := range []int{512, 1024, 2048} {
		sc := simulator.Config{Capacity: capacity, MaxConcurrent: 32, TokenDelay: time.Duration(2+i) * time.Millisecond, PrefillPerToken: 100 * time.Microsecond, OutputFraction: fraction, AdminToken: "experiment"}
		if err := sc.Validate(); err != nil {
			return benchmark.Summary{}, err
		}
		s := httptest.NewServer(simulator.New(sc).Handler())
		sims = append(sims, s)
		c.Backends = append(c.Backends, config.Backend{ID: fmt.Sprintf("backend-%c", 'a'+i), URL: s.URL, Kind: "sim", Capacity: capacity, MaxConcurrent: 32})
	}
	g, err := gateway.New(c)
	if err != nil {
		return benchmark.Summary{}, err
	}
	defer g.Close()
	gw := httptest.NewServer(g.Handler())
	defer gw.Close()
	bc.URL = gw.URL
	// Readiness is established explicitly; startup races do not count as policy failures.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(gw.URL + "/readyz")
		ready := false
		if err == nil {
			var state struct {
				Backends []struct {
					Healthy bool `json:"healthy"`
				} `json:"backends"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&state)
			resp.Body.Close()
			ready = len(state.Backends) == 3
			for _, b := range state.Backends {
				ready = ready && b.Healthy
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			return benchmark.Summary{}, fmt.Errorf("simulation did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	faultCtx, cancelFault := context.WithCancel(ctx)
	faultDone := make(chan struct{})
	if faultAt > 0 {
		go func() {
			defer close(faultDone)
			t := time.NewTimer(faultAt)
			defer t.Stop()
			select {
			case <-faultCtx.Done():
				return
			case <-t.C:
			}
			req, _ := http.NewRequestWithContext(faultCtx, "POST", sims[1].URL+"/admin/fault", strings.NewReader(fmt.Sprintf(`{"down_ms":%d}`, faultFor.Milliseconds())))
			req.Header.Set("Authorization", "Bearer experiment")
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	} else {
		close(faultDone)
	}
	defer func() { cancelFault(); <-faultDone }()
	f, err := os.Create(filepath.Join(out, policy+".jsonl"))
	if err != nil {
		return benchmark.Summary{}, err
	}
	defer f.Close()
	s, err := benchmark.Run(ctx, bc, f)
	if err != nil {
		return s, err
	}
	data, _ := json.MarshalIndent(s, "", "  ")
	if err = os.WriteFile(filepath.Join(out, policy+".json"), append(data, '\n'), 0644); err != nil {
		return s, err
	}
	// Capture final accounting for investigation; samples retain arrival times.
	resp, err := http.Get(gw.URL + "/metrics")
	if err == nil {
		defer resp.Body.Close()
		metrics, _ := io.ReadAll(resp.Body)
		err = os.WriteFile(filepath.Join(out, policy+".prom"), metrics, 0644)
	}
	return s, err
}
