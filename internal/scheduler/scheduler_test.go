package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"memorygateway/internal/config"
)

func testConfig() config.Config {
	c := config.Default()
	c.QueueSize = 256
	c.QueueTimeout = config.Duration(time.Second)
	c.TelemetryTTL = config.Duration(10 * time.Second)
	c.Backends = []config.Backend{{ID: "a", URL: "http://localhost", Kind: "sim", Capacity: 100, MaxConcurrent: 100}}
	return c
}
func newReady(t *testing.T, c config.Config) *Scheduler {
	t.Helper()
	s := New(c)
	t.Cleanup(s.Close)
	s.UpdateHealth("a", 100, 0)
	return s
}
func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}
func get(t *testing.T, s *Scheduler, demand int) *Lease {
	t.Helper()
	l, err := s.Acquire(context.Background(), demand, 0)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestAtomicReservationsUnderConcurrency(t *testing.T) {
	s := newReady(t, testConfig())
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	stop := make(chan struct{})
	checked := make(chan struct{})
	go func() {
		defer close(checked)
		for {
			select {
			case <-stop:
				return
			default:
			}
			b := s.Snapshot().Backends[0]
			if b.Reserved > 100 || b.Reserved < 0 || b.Running > 4 {
				errs <- errors.New("reservation invariant violated")
				return
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := s.Acquire(context.Background(), 25, 0)
			if err != nil {
				errs <- err
				return
			}
			time.Sleep(time.Millisecond)
			l.Release()
			l.Release()
		}()
	}
	wg.Wait()
	close(stop)
	<-checked
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if b := s.Snapshot().Backends[0]; b.Reserved != 0 || b.Running != 0 {
		t.Fatalf("leaked reservation: %+v", b)
	}
}
func TestQueueBoundCancellationAndExpiry(t *testing.T) {
	c := testConfig()
	c.QueueSize = 1
	c.QueueTimeout = config.Duration(80 * time.Millisecond)
	s := newReady(t, c)
	l := get(t, s, 100)
	defer l.Release()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := s.Acquire(ctx, 10, 0); result <- err }()
	eventually(t, func() bool { return s.Snapshot().Queued == 1 })
	if _, err := s.Acquire(context.Background(), 10, 0); !errors.Is(err, ErrFull) {
		t.Fatalf("expected full, got %v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.Snapshot().Queued != 0 {
		t.Fatal("canceled job remains queued")
	}
	if _, err := s.Acquire(context.Background(), 10, 0); !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("expected queue expiry, got %v", err)
	}
	if _, err := s.Acquire(context.Background(), 101, 0); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
}
func TestZeroQueueAllowsOnlyImmediateDispatch(t *testing.T) {
	c := testConfig()
	c.QueueSize = 0
	s := newReady(t, c)
	l := get(t, s, 100)
	if _, err := s.Acquire(context.Background(), 1, 0); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}
	l.Release()
	get(t, s, 100).Release()
}
func TestUrgentRequestsLeadBeforeAging(t *testing.T) {
	s := newReady(t, testConfig())
	held := get(t, s, 100)
	low, high := make(chan *Lease, 1), make(chan *Lease, 1)
	go func() { l, _ := s.Acquire(context.Background(), 100, 0); low <- l }()
	eventually(t, func() bool { return s.Snapshot().Queued == 1 })
	go func() { l, _ := s.Acquire(context.Background(), 100, 2); high <- l }()
	eventually(t, func() bool { return s.Snapshot().Queued == 2 })
	held.Release()
	select {
	case l := <-high:
		if l == nil {
			t.Fatal("no urgent lease")
		}
		l.Release()
	case <-time.After(time.Second):
		t.Fatal("urgent request did not run first")
	}
	l := <-low
	if l == nil {
		t.Fatal("no ordinary lease")
	}
	l.Release()
}
func TestAgedLargeRequestStopsBackfilling(t *testing.T) {
	c := testConfig()
	c.StarvationLimit = config.Duration(30 * time.Millisecond)
	s := newReady(t, c)
	a, b := get(t, s, 50), get(t, s, 50)
	large, small := make(chan *Lease, 1), make(chan *Lease, 1)
	go func() { l, _ := s.Acquire(context.Background(), 100, 0); large <- l }()
	eventually(t, func() bool { return s.Snapshot().Queued == 1 })
	time.Sleep(40 * time.Millisecond)
	go func() { l, _ := s.Acquire(context.Background(), 10, 2); small <- l }()
	eventually(t, func() bool { return s.Snapshot().Queued == 2 })
	a.Release()
	time.Sleep(15 * time.Millisecond)
	select {
	case <-small:
		t.Fatal("urgent backfill starved aged large request")
	default:
	}
	b.Release()
	l := <-large
	if l == nil {
		t.Fatal("aged request never ran")
	}
	l.Release()
	l = <-small
	if l == nil {
		t.Fatal("small request never ran")
	}
	l.Release()
}
func TestStaleTelemetryAndCircuitRecovery(t *testing.T) {
	c := testConfig()
	c.TelemetryTTL = config.Duration(20 * time.Millisecond)
	c.QueueTimeout = config.Duration(40 * time.Millisecond)
	c.CircuitCooldown = config.Duration(30 * time.Millisecond)
	c.FailureThreshold = 2
	s := newReady(t, c)
	time.Sleep(30 * time.Millisecond)
	if _, err := s.Acquire(context.Background(), 1, 0); !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("stale backend admitted request: %v", err)
	}
	s.UpdateHealth("a", 100, 0)
	s.Failure("a")
	s.UpdateHealth("a", 100, 0)
	s.Failure("a")
	if !s.Snapshot().Backends[0].CircuitOpen {
		t.Fatal("successful health erased inference failures")
	}
	s.UpdateHealth("a", 100, 0)
	if !s.Snapshot().Backends[0].CircuitOpen {
		t.Fatal("circuit closed before cooldown")
	}
	time.Sleep(35 * time.Millisecond)
	s.UpdateHealth("a", 100, 0)
	get(t, s, 100).Release()
}
func TestTelemetryDoesNotDoubleCountReservations(t *testing.T) {
	s := newReady(t, testConfig())
	a := get(t, s, 40)
	s.UpdateHealth("a", 100, 40)
	b := get(t, s, 60)
	b.Release()
	a.Release()
	s.UpdateHealth("a", 100, 30)
	l := get(t, s, 70)
	l.Release()
	if v := s.Snapshot().Backends[0]; v.ExternalUsed != 30 {
		t.Fatalf("external work not deducted: %+v", v)
	}
}
func TestBaselinesIgnoreTokenBudgetButRespectConcurrency(t *testing.T) {
	for _, policy := range []string{"round_robin", "least_outstanding"} {
		t.Run(policy, func(t *testing.T) {
			c := testConfig()
			c.Policy = policy
			c.Backends[0].MaxConcurrent = 2
			c.QueueSize = 0
			s := newReady(t, c)
			a, b := get(t, s, 100), get(t, s, 100)
			defer a.Release()
			defer b.Release()
			if s.Snapshot().Backends[0].Reserved != 200 {
				t.Fatal("baseline unexpectedly used memory budget")
			}
			if _, err := s.Acquire(context.Background(), 1, 0); !errors.Is(err, ErrFull) {
				t.Fatal("baseline ignored concurrency limit")
			}
		})
	}
}
func TestCloseUnblocksWaiters(t *testing.T) {
	s := New(testConfig())
	result := make(chan error, 1)
	go func() { _, err := s.Acquire(context.Background(), 10, 0); result <- err }()
	eventually(t, func() bool { return s.Snapshot().Queued == 1 })
	s.Close()
	s.Close()
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
