// Package scheduler owns all queue and reservation state under one mutex.
// Network operations never run while that mutex is held.
package scheduler

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"memorygateway/internal/config"
)

var (
	ErrFull         = errors.New("queue is full")
	ErrTooLarge     = errors.New("request cannot fit any configured backend")
	ErrQueueTimeout = errors.New("queue deadline exceeded")
	ErrClosed       = errors.New("scheduler is closed")
)

type backend struct {
	Config        config.Backend
	Healthy       bool
	LastSeen      time.Time
	Capacity      int
	ExternalUsed  int
	Reserved      int
	Running       int
	Failures      int
	ProbeFailures int
	OpenUntil     time.Time
}
type BackendView struct {
	ID           string `json:"id"`
	Healthy      bool   `json:"healthy"`
	Fresh        bool   `json:"fresh"`
	Capacity     int    `json:"capacity_tokens"`
	Reserved     int    `json:"reserved_tokens"`
	ExternalUsed int    `json:"external_used_tokens"`
	Running      int    `json:"running"`
	CircuitOpen  bool   `json:"circuit_open"`
}
type Snapshot struct {
	Policy   string        `json:"policy"`
	Queued   int           `json:"queued"`
	Backends []BackendView `json:"backends"`
}
type job struct {
	ctx                context.Context
	demand, priority   int
	enqueued, deadline time.Time
	result             chan outcome
}
type outcome struct {
	lease *Lease
	err   error
}
type Lease struct {
	Backend   config.Backend
	Demand    int
	QueueWait time.Duration
	s         *Scheduler
	index     int
	once      sync.Once
}

// Release must only follow a terminal backend result, or the caller's conservative
// cancellation quarantine. It is safe to call more than once.
func (l *Lease) Release() {
	l.once.Do(func() {
		l.s.mu.Lock()
		defer l.s.mu.Unlock()
		b := l.s.backends[l.index]
		b.Reserved -= l.Demand
		b.Running--
		l.s.dispatch(time.Now())
	})
}

type Scheduler struct {
	mu       sync.Mutex
	cfg      config.Config
	backends []*backend
	queue    []*job
	rr       int
	closed   bool
	done     chan struct{}
}

func New(c config.Config) *Scheduler {
	s := &Scheduler{cfg: c, done: make(chan struct{})}
	for _, b := range c.Backends {
		s.backends = append(s.backends, &backend{Config: b, Capacity: b.Capacity})
	}
	go s.loop()
	return s
}
func (s *Scheduler) loop() {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-t.C:
			s.mu.Lock()
			s.dispatch(now)
			s.mu.Unlock()
		}
	}
}
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.done)
	for _, j := range s.queue {
		j.result <- outcome{err: ErrClosed}
	}
	s.queue = nil
}
func (s *Scheduler) Acquire(ctx context.Context, demand, priority int) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if demand <= 0 || priority < 0 || priority > 2 {
		return nil, ErrTooLarge
	}
	now := time.Now()
	j := &job{ctx: ctx, demand: demand, priority: priority, enqueued: now, deadline: now.Add(s.cfg.QueueTimeout.Value()), result: make(chan outcome, 1)}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	maxCap := 0
	for _, b := range s.backends {
		if b.Config.Capacity > maxCap {
			maxCap = b.Config.Capacity
		}
	}
	if demand > maxCap {
		s.mu.Unlock()
		return nil, ErrTooLarge
	}
	// Drain expired work before checking the bound. Permit an immediate dispatch
	// with queue_size=0, without allowing arrivals to bypass existing queued jobs.
	s.dispatch(now)
	wasFull := len(s.queue) >= s.cfg.QueueSize
	s.queue = append(s.queue, j)
	s.dispatch(now)
	if wasFull {
		for i, q := range s.queue {
			if q == j {
				s.queue = append(s.queue[:i], s.queue[i+1:]...)
				s.mu.Unlock()
				return nil, ErrFull
			}
		}
	}
	s.mu.Unlock()
	select {
	case o := <-j.result:
		return o.lease, o.err
	case <-ctx.Done():
		s.mu.Lock()
		for i, q := range s.queue {
			if q == j {
				s.queue = append(s.queue[:i], s.queue[i+1:]...)
				s.dispatch(time.Now())
				s.mu.Unlock()
				return nil, ctx.Err()
			}
		}
		s.mu.Unlock()
		// Dispatch won the race; return its lease so the caller can release it.
		o := <-j.result
		return o.lease, o.err
	}
}
func (s *Scheduler) eligible(b *backend, now time.Time) bool {
	return b.Healthy && b.Capacity > 0 && now.Sub(b.LastSeen) <= s.cfg.TelemetryTTL.Value() && b.OpenUntil.IsZero() && b.Running < b.Config.MaxConcurrent
}
func (s *Scheduler) choose(demand int, now time.Time) int {
	best := -1
	bestValue := math.Inf(1)
	for offset := range s.backends {
		i := (s.rr + offset) % len(s.backends)
		b := s.backends[i]
		if !s.eligible(b, now) {
			continue
		}
		available := b.Capacity - b.Reserved - b.ExternalUsed
		if s.cfg.Policy == "memory" && available < demand {
			continue
		}
		value := float64(b.Running)
		if s.cfg.Policy == "round_robin" {
			return i
		}
		if s.cfg.Policy == "memory" {
			value = float64(b.Reserved+b.ExternalUsed+demand) / float64(b.Capacity)
		}
		if value < bestValue {
			best = i
			bestValue = value
		}
	}
	return best
}
func (s *Scheduler) dispatch(now time.Time) {
	if s.closed {
		return
	}
	kept := s.queue[:0]
	for _, j := range s.queue {
		var err error
		if j.ctx.Err() != nil {
			err = j.ctx.Err()
		} else if !now.Before(j.deadline) {
			err = ErrQueueTimeout
		}
		if err != nil {
			j.result <- outcome{err: err}
		} else {
			kept = append(kept, j)
		}
	}
	s.queue = kept
	for len(s.queue) > 0 {
		// Protect the oldest aged request from indefinite backfilling. This can
		// deliberately leave capacity idle while enough capacity drains for it.
		protected := -1
		for i, j := range s.queue {
			if now.Sub(j.enqueued) >= s.cfg.StarvationLimit.Value() && (protected < 0 || j.enqueued.Before(s.queue[protected].enqueued)) {
				protected = i
			}
		}
		selected, bi := -1, -1
		score := math.Inf(-1)
		for i, j := range s.queue {
			if protected >= 0 && i != protected {
				continue
			}
			b := s.choose(j.demand, now)
			if b < 0 {
				continue
			}
			v := float64(j.priority) + float64(now.Sub(j.enqueued))/float64(s.cfg.AgingInterval.Value())
			if v > score {
				score = v
				selected = i
				bi = b
			}
		}
		if selected < 0 {
			return
		}
		j := s.queue[selected]
		s.queue = append(s.queue[:selected], s.queue[selected+1:]...)
		b := s.backends[bi]
		b.Reserved += j.demand
		b.Running++
		s.rr = (bi + 1) % len(s.backends)
		j.result <- outcome{lease: &Lease{Backend: b.Config, Demand: j.demand, QueueWait: now.Sub(j.enqueued), s: s, index: bi}}
	}
}

// UpdateHealth accepts a fresh successful probe. Capacity is capped by the
// configured safety budget. Reported use above local reservations is deducted;
// local reservations are never subtracted twice.
func (s *Scheduler) UpdateHealth(id string, capacity, used int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for _, b := range s.backends {
		if b.Config.ID == id {
			if !b.OpenUntil.IsZero() && now.Before(b.OpenUntil) {
				return
			}
			b.Healthy = true
			b.LastSeen = now
			b.ProbeFailures = 0
			if !b.OpenUntil.IsZero() {
				b.Failures = 0
			}
			b.OpenUntil = time.Time{}
			b.Capacity = min(b.Config.Capacity, max(0, capacity))
			b.ExternalUsed = max(0, used-b.Reserved)
			break
		}
	}
	s.dispatch(now)
}
func (s *Scheduler) Failure(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.backends {
		if b.Config.ID == id {
			b.Failures++
			if b.Failures >= s.cfg.FailureThreshold && b.OpenUntil.IsZero() {
				b.Healthy = false
				b.OpenUntil = time.Now().Add(s.cfg.CircuitCooldown.Value())
			}
			break
		}
	}
}

// Successful probes must not erase consecutive inference failures: a process
// can respond to /health while its generation path is broken.
func (s *Scheduler) HealthFailure(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.backends {
		if b.Config.ID == id {
			b.ProbeFailures++
			if b.ProbeFailures >= s.cfg.FailureThreshold && b.OpenUntil.IsZero() {
				b.Healthy = false
				b.OpenUntil = time.Now().Add(s.cfg.CircuitCooldown.Value())
			}
			break
		}
	}
}
func (s *Scheduler) Success(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.backends {
		if b.Config.ID == id {
			b.Failures = 0
			break
		}
	}
}

// Quarantine prevents immediate capacity reuse when backend termination is
// uncertain (a canceled stream or network failure). A fresh probe after the
// cooldown is required before this backend receives new work.
func (s *Scheduler) Quarantine(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.backends {
		if b.Config.ID == id {
			b.Healthy = false
			b.OpenUntil = time.Now().Add(s.cfg.CircuitCooldown.Value())
			break
		}
	}
}
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	v := Snapshot{Policy: s.cfg.Policy, Queued: len(s.queue), Backends: make([]BackendView, 0, len(s.backends))}
	for _, b := range s.backends {
		v.Backends = append(v.Backends, BackendView{b.Config.ID, b.Healthy, now.Sub(b.LastSeen) <= s.cfg.TelemetryTTL.Value(), b.Capacity, b.Reserved, b.ExternalUsed, b.Running, !b.OpenUntil.IsZero()})
	}
	return v
}
