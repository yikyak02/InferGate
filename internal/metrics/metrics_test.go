package metrics

import (
	"sync"
	"testing"
	"time"

	"memorygateway/internal/scheduler"
)

type slowWriter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (w *slowWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(b), nil
}
func TestSlowScraperDoesNotBlockRequestMetrics(t *testing.T) {
	m := New()
	m.Inc("gateway_requests_total")
	m.Observe("gateway_ttft_seconds", time.Second)
	w := &slowWriter{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { m.Write(w, scheduler.Snapshot{}); close(done) }()
	<-w.entered
	updated := make(chan struct{})
	go func() {
		m.Inc("gateway_requests_total")
		m.Observe("gateway_ttft_seconds", time.Second)
		close(updated)
	}()
	select {
	case <-updated:
	case <-time.After(time.Second):
		close(w.release)
		<-done
		t.Fatal("slow scraper blocked request path")
	}
	close(w.release)
	<-done
}
