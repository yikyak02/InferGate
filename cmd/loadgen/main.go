package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"memorygateway/internal/benchmark"
)

func main() {
	c := benchmark.Config{}
	flag.StringVar(&c.URL, "url", "http://127.0.0.1:8080", "gateway origin")
	flag.IntVar(&c.Requests, "requests", 500, "number of offered requests")
	flag.Float64Var(&c.Rate, "rate", 100, "scheduled arrivals per second")
	flag.Int64Var(&c.Seed, "seed", 42, "repeatable workload seed")
	flag.IntVar(&c.BurstEvery, "burst-every", 100, "arrival cycle length; 0 disables bursts")
	flag.IntVar(&c.BurstSize, "burst-size", 20, "arrivals collapsed at each cycle start")
	flag.DurationVar(&c.Timeout, "timeout", 15*time.Second, "per request client deadline")
	flag.DurationVar(&c.SLO, "slo", time.Second, "completion latency target")
	flag.IntVar(&c.MaxInflight, "max-inflight", 2048, "load generator safety limit")
	output := flag.String("output", "samples.jsonl", "per-request samples destination")
	summaryPath := flag.String("summary", "summary.json", "summary destination")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, c, *output, *summaryPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context, c benchmark.Config, output, summaryPath string) error {
	if err := c.Validate(); err != nil {
		return err
	}
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	s, err := benchmark.Run(ctx, c, f)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(summaryPath, append(data, '\n'), 0644); err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
