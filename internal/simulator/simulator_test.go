package simulator

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestIndependentCostAndOverload(t *testing.T) {
	s := New(Config{Capacity: 20, MaxConcurrent: 1, TokenDelay: time.Millisecond, OutputFraction: 1})
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	r, err := http.Post(server.URL+"/v1/generate", "application/json", strings.NewReader(`{"prompt":"hello","max_tokens":20}`))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 503 {
		t.Fatal("oversized request admitted")
	}
	if ActualInputTokens("hello 世界") != 13 {
		t.Fatal("unexpected independent lexical cost")
	}
}
func TestCrashInterruptsActiveGenerationAndReleasesResources(t *testing.T) {
	s := New(Config{Capacity: 1024, MaxConcurrent: 4, TokenDelay: time.Millisecond, OutputFraction: 1, AdminToken: "secret"})
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	r, err := http.Post(server.URL+"/v1/generate", "application/json", strings.NewReader(`{"prompt":"hello","max_tokens":200}`))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	scanner := bufio.NewScanner(r.Body)
	if !scanner.Scan() {
		t.Fatal("generation did not start")
	}
	req, _ := http.NewRequest("POST", server.URL+"/admin/fault", strings.NewReader(`{"down_ms":50}`))
	req.Header.Set("Authorization", "Bearer secret")
	fault, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	fault.Body.Close()
	for scanner.Scan() {
		var v map[string]any
		_ = json.Unmarshal(scanner.Bytes(), &v)
		if v["done"] == true {
			t.Fatal("crashed generation reported success")
		}
	}
	s.mu.Lock()
	used, running, canceled := s.used, s.running, s.canceled
	s.mu.Unlock()
	if used != 0 || running != 0 || canceled != 1 {
		t.Fatalf("crash leaked resources: %d %d %d", used, running, canceled)
	}
}
func TestFaultInjectionRequiresToken(t *testing.T) {
	s := New(Config{Capacity: 100, MaxConcurrent: 1, TokenDelay: time.Millisecond, OutputFraction: 1, AdminToken: "secret"})
	r := httptest.NewRequest("POST", "/admin/fault", strings.NewReader(`{"down_ms":50}`))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	s.cfg.AdminToken = ""
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	_, _ = io.Copy(io.Discard, w.Result().Body)
	if w.Code != 404 {
		t.Fatal(w.Code)
	}
}
