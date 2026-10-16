package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"memorygateway/internal/config"
	"memorygateway/internal/gateway"
	"memorygateway/internal/runhttp"
)

func main() {
	path := flag.String("config", "configs/local.json", "JSON configuration")
	policy := flag.String("policy", "", "override policy: memory, round_robin, least_outstanding")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	c, err := config.Load(*path)
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	if *policy != "" {
		c.Policy = *policy
	}
	g, err := gateway.New(c)
	if err != nil {
		slog.Error("gateway", "error", err)
		os.Exit(1)
	}
	err = runhttp.Serve(c.Listen, g.Handler(), c.RequestTimeout.Value()+2*time.Second)
	g.Close()
	if err != nil {
		slog.Error("server", "error", err)
		os.Exit(1)
	}
}
