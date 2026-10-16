// Package simulator implements a deliberately independent, lexical resource
// model. These capacity units are synthetic and do not represent measured VRAM.
package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"

	"memorygateway/internal/protocol"
)

type Config struct {
	Capacity        int
	MaxConcurrent   int
	TokenDelay      time.Duration
	PrefillPerToken time.Duration
	OutputFraction  float64
	// AdminToken protects fault injection; an empty token disables /admin/fault.
	AdminToken string
}
type Server struct {
	mu                            sync.Mutex
	cfg                           Config
	used, running                 int
	failedUntil                   time.Time
	generation                    uint64
	dropAfter                     int
	completed, rejected, canceled uint64
}

func New(c Config) *Server { return &Server{cfg: c} }

// ActualInputTokens intentionally does not call the gateway's byte estimator.
// Each ASCII word costs ceil(length/5); non-ASCII runes cost two units.
func ActualInputTokens(prompt string) int {
	n, word := 8, 0
	flush := func() {
		if word > 0 {
			n += (word + 4) / 5
			word = 0
		}
	}
	for _, r := range prompt {
		if r > 127 {
			flush()
			n += 2
		} else if unicode.IsSpace(r) {
			flush()
		} else {
			word++
		}
	}
	flush()
	return n
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /v1/generate", s.generate)
	mux.HandleFunc("POST /admin/fault", s.fault)
	return mux
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := 200
	if time.Now().Before(s.failedUntil) {
		status = 503
	}
	respond(w, status, map[string]any{"capacity_tokens": s.cfg.Capacity, "used_tokens": s.used, "running": s.running, "completed": s.completed, "rejected": s.rejected, "canceled": s.canceled})
}
func (s *Server) fault(w http.ResponseWriter, r *http.Request) {
	if s.cfg.AdminToken == "" {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+s.cfg.AdminToken {
		respond(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	var f struct {
		DownMS    int `json:"down_ms"`
		DropAfter int `json:"drop_after_tokens"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil || f.DownMS < 0 || f.DownMS > 300000 || f.DropAfter < 0 {
		respond(w, 400, map[string]string{"error": "invalid_fault"})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedUntil = time.Now().Add(time.Duration(f.DownMS) * time.Millisecond)
	s.dropAfter = f.DropAfter
	// Crash changes the epoch; every already-running request terminates at its
	// next cancellation-aware work step, even if recovery happens quickly.
	if f.DownMS > 0 {
		s.generation++
	}
	respond(w, 200, map[string]string{"status": "configured"})
}
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (s *Server) alive(epoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return epoch == s.generation && !time.Now().Before(s.failedUntil)
}
func (s *Server) generate(w http.ResponseWriter, r *http.Request) {
	var req protocol.Request
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MaxTokens < 1 || req.MaxTokens > 8192 || req.Prompt == "" {
		respond(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	input := ActualInputTokens(req.Prompt)
	output := max(1, int(float64(req.MaxTokens)*s.cfg.OutputFraction))
	cost := input + output
	s.mu.Lock()
	if time.Now().Before(s.failedUntil) {
		s.rejected++
		s.mu.Unlock()
		respond(w, 503, map[string]string{"error": "backend_down"})
		return
	}
	if s.used+cost > s.cfg.Capacity || s.running >= s.cfg.MaxConcurrent {
		s.rejected++
		s.mu.Unlock()
		respond(w, 503, map[string]string{"error": "backend_overloaded"})
		return
	}
	s.used += cost
	s.running++
	epoch, drop := s.generation, s.dropAfter
	s.mu.Unlock()
	done := false
	defer func() {
		s.mu.Lock()
		s.used -= cost
		s.running--
		if done {
			s.completed++
		} else {
			s.canceled++
		}
		s.mu.Unlock()
	}()
	if !sleep(r.Context(), time.Duration(input)*s.cfg.PrefillPerToken) || !s.alive(epoch) {
		respond(w, 503, map[string]string{"error": "prefill_interrupted"})
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	for i := 0; i < output; i++ {
		if !sleep(r.Context(), s.cfg.TokenDelay) || !s.alive(epoch) {
			return
		}
		if drop > 0 && i >= drop {
			return
		}
		if err := json.NewEncoder(w).Encode(protocol.Chunk{Model: req.Model, Response: fmt.Sprintf("t%d ", i), Done: false}); err != nil {
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
	}
	if err := json.NewEncoder(w).Encode(protocol.Chunk{Model: req.Model, Done: true, PromptTokens: input, OutputTokens: output}); err != nil {
		return
	}
	done = true
}

func (c Config) Validate() error {
	if c.Capacity <= 0 || c.MaxConcurrent <= 0 || c.TokenDelay <= 0 || c.PrefillPerToken < 0 || c.OutputFraction <= 0 || c.OutputFraction > 1 || strings.ContainsAny(c.AdminToken, "\r\n") {
		return fmt.Errorf("invalid simulator configuration")
	}
	return nil
}
