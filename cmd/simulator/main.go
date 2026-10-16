package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"memorygateway/internal/runhttp"
	"memorygateway/internal/simulator"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9001", "listen address")
	capacity := flag.Int("capacity", 1024, "synthetic token capacity")
	concurrency := flag.Int("max-concurrent", 32, "maximum concurrent requests")
	delay := flag.Duration("token-delay", 3*time.Millisecond, "time per generated token")
	prefill := flag.Duration("prefill-per-token", 100*time.Microsecond, "prefill time per actual input token")
	fraction := flag.Float64("output-fraction", 1, "fraction of requested output generated, (0,1]")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	c := simulator.Config{Capacity: *capacity, MaxConcurrent: *concurrency, TokenDelay: *delay, PrefillPerToken: *prefill, OutputFraction: *fraction, AdminToken: os.Getenv("SIM_ADMIN_TOKEN")}
	if err := c.Validate(); err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	if err := runhttp.Serve(*addr, simulator.New(c).Handler(), 2*time.Minute); err != nil {
		slog.Error("server", "error", err)
		os.Exit(1)
	}
}
