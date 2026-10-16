package benchmark

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestTraceIsRepeatableAndContainsMixedBursts(t *testing.T) {
	c := Config{Requests: 100, Rate: 100, Seed: 42, BurstEvery: 50, BurstSize: 10}
	a, b := workload(c), workload(c)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("trace changed for identical seed")
	}
	if a[9].at != 0 || a[10].at <= 0 {
		t.Fatal("burst arrival times are wrong")
	}
	classes := map[string]bool{}
	priorities := map[int]bool{}
	for _, it := range a {
		classes[it.class] = true
		priorities[it.request.Priority] = true
	}
	if len(classes) != 2 || len(priorities) != 2 {
		t.Fatal("missing request class or priority")
	}
}
func TestFailuresDoNotDisappearFromSummary(t *testing.T) {
	c := Config{Seed: 42, SLO: time.Second}
	s := summarize(c, []Sample{
		{Outcome: "completed", Status: 200, LatencyMS: 100, TTFTMS: 20, Class: "short"},
		{Outcome: "completed", Status: 200, LatencyMS: 1500, TTFTMS: 40, Class: "long"},
		{Outcome: "http_error", Status: 429, Class: "long"},
		{Outcome: "interrupted_stream", Status: 200, Class: "short"},
		{Outcome: "loadgen_saturated", Class: "short"},
	}, 2, 1)
	if s.Completed != 2 || s.WithinSLO != 1 || s.CompletionRate != .4 || s.Goodput != .5 || s.Outcomes["http_error"] != 1 || s.LatencyMS.P95 != 1500 {
		t.Fatalf("misleading summary: %+v", s)
	}
}
func TestOpenLoopReportsClientSaturation(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		fmt.Fprintln(w, `{"response":"ok","done":true}`)
	}))
	defer server.Close()
	c := Config{URL: server.URL, Requests: 20, Rate: 100000, Seed: 42, BurstEvery: 20, BurstSize: 20, Timeout: time.Second, SLO: time.Second, MaxInflight: 1}
	s, err := Run(context.Background(), c, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if s.Outcomes["loadgen_saturated"] != 19 || calls.Load() != 1 || s.Completed != 1 {
		t.Fatalf("generator silently slowed arrivals: %+v", s)
	}
}
