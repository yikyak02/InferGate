// Package metrics exposes bounded-cardinality Prometheus text without a runtime dependency.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"memorygateway/internal/scheduler"
)

var bounds = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

type histogram struct {
	buckets []uint64
	count   uint64
	sum     float64
}
type Metrics struct {
	mu         sync.Mutex
	counters   map[string]uint64
	histograms map[string]*histogram
}

func New() *Metrics {
	m := &Metrics{counters: map[string]uint64{}, histograms: map[string]*histogram{}}
	for _, name := range []string{"requests", "completed", "canceled", "stream_failures", "backend_failures", "health_failures", "completed_estimated_tokens", "completed_observed_tokens"} {
		m.counters["gateway_"+name+"_total"] = 0
	}
	for _, reason := range []string{"body_too_large", "invalid_json", "invalid_request", "unsupported_model", "invalid_max_tokens", "context_too_large", "queue_full", "capacity_too_large", "queue_timeout", "canceled", "deadline_exceeded", "unavailable", "upstream_unavailable", "upstream_rejected", "upstream_stream_interrupted"} {
		m.counters["gateway_rejected_"+reason+"_total"] = 0
	}
	return m
}
func (m *Metrics) Inc(name string) { m.Add(name, 1) }
func (m *Metrics) Add(name string, value uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name] += value
}
func (m *Metrics) Observe(name string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.histograms[name]
	if h == nil {
		h = &histogram{buckets: make([]uint64, len(bounds))}
		m.histograms[name] = h
	}
	v := d.Seconds()
	h.count++
	h.sum += v
	for i, b := range bounds {
		if v <= b {
			h.buckets[i]++
		}
	}
}
func (m *Metrics) Write(w io.Writer, s scheduler.Snapshot) {
	// Never hold the request-path mutex while writing to a potentially slow scraper.
	m.mu.Lock()
	counters := make(map[string]uint64, len(m.counters))
	for k, v := range m.counters {
		counters[k] = v
	}
	histograms := make(map[string]histogram, len(m.histograms))
	for k, h := range m.histograms {
		histograms[k] = histogram{buckets: append([]uint64(nil), h.buckets...), count: h.count, sum: h.sum}
	}
	m.mu.Unlock()
	fmt.Fprintln(w, "# TYPE gateway_queue_depth gauge")
	fmt.Fprintf(w, "gateway_queue_depth %d\n", s.Queued)
	for _, b := range s.Backends {
		ready := 0
		if b.Healthy && b.Fresh && !b.CircuitOpen && b.Capacity > 0 {
			ready = 1
		}
		open := 0
		if b.CircuitOpen {
			open = 1
		}
		fmt.Fprintf(w, "gateway_backend_ready{backend=%q} %d\ngateway_backend_circuit_open{backend=%q} %d\ngateway_backend_reserved_tokens{backend=%q} %d\ngateway_backend_capacity_tokens{backend=%q} %d\ngateway_backend_external_used_tokens{backend=%q} %d\ngateway_backend_running{backend=%q} %d\n", b.ID, ready, b.ID, open, b.ID, b.Reserved, b.ID, b.Capacity, b.ID, b.ExternalUsed, b.ID, b.Running)
	}
	keys := make([]string, 0, len(counters))
	for k := range counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", k, k, counters[k])
	}
	keys = keys[:0]
	for k := range histograms {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h := histograms[k]
		fmt.Fprintf(w, "# TYPE %s histogram\n", k)
		for i, b := range bounds {
			fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", k, fmt.Sprint(b), h.buckets[i])
		}
		fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", k, h.count, k, h.sum, k, h.count)
	}
}
