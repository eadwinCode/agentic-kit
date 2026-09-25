package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

func leaseDeps(lease time.Duration) ports.RuntimePorts {
	cfg := ports.DefaultConfig()
	cfg.RunLockLease = lease
	return ports.RuntimePorts{Kv: memory.NewKv(), Config: cfg}
}

func TestParseLockValue(t *testing.T) {
	run, dispatch := core.ParseLockValue("r1/d1/n1")
	if run != "r1" || dispatch != "d1" {
		t.Fatalf("got %q %q", run, dispatch)
	}
	run, dispatch = core.ParseLockValue("r1")
	if run != "r1" || dispatch != "" {
		t.Fatalf("a bare run id from before dispatch ids: got %q %q", run, dispatch)
	}
}

func TestLease_OneHolderAtATime(t *testing.T) {
	ctx := context.Background()
	deps := leaseDeps(time.Minute)
	a, err := core.AcquireRunLock(ctx, deps, "t1", "r1", "d1")
	if err != nil || a == nil {
		t.Fatalf("first acquire: %v %v", a, err)
	}
	b, err := core.AcquireRunLock(ctx, deps, "t1", "r1", "d1")
	if err != nil || b != nil {
		t.Fatalf("a held lock is not taken again, even by the same job: %v %v", b, err)
	}
	a.Release()
	c, _ := core.AcquireRunLock(ctx, deps, "t1", "r1", "d1")
	if c == nil {
		t.Fatal("free once released")
	}
	c.Release()
}

// The case the nonce exists for: a stalled worker's lease lapses and the
// queue's redelivery of the SAME job takes the lock. The stalled worker must
// learn it lost the lock, and must not free the new holder's.
func TestLease_AStalledHolderCannotRenewOrFreeTheNewHoldersLock(t *testing.T) {
	ctx := context.Background()
	deps := leaseDeps(300 * time.Millisecond)
	stalled, _ := core.AcquireRunLock(ctx, deps, "t1", "r1", "d1")
	_ = deps.Kv.Del(ctx, core.RunLockKey("t1")) // the lease lapsed while it stalled
	fresh, _ := core.AcquireRunLock(ctx, deps, "t1", "r1", "d1")
	if fresh == nil {
		t.Fatal("the redelivery takes the lapsed lock")
	}
	lost := make(chan struct{})
	stalled.Keep(func() { close(lost) })
	select {
	case <-lost:
	case <-time.After(2 * time.Second):
		t.Fatal("the stalled holder learns it lost the lock")
	}
	if !stalled.Lost() {
		t.Fatal("Lost reports it")
	}
	stalled.Release()
	value, held, _ := deps.Kv.Get(ctx, core.RunLockKey("t1"))
	if !held {
		t.Fatal("the stalled holder's release left the new holder's lock alone")
	}
	if run, _ := core.ParseLockValue(value); run != "r1" {
		t.Fatalf("still the new holder's: %q", value)
	}
	fresh.Release()
}

func TestLease_KeepOutlivesTheLease(t *testing.T) {
	ctx := context.Background()
	deps := leaseDeps(300 * time.Millisecond)
	l, _ := core.AcquireRunLock(ctx, deps, "t1", "r1", "")
	l.Keep(nil)
	time.Sleep(700 * time.Millisecond)
	if _, held, _ := deps.Kv.Get(ctx, core.RunLockKey("t1")); !held || l.Lost() {
		t.Fatal("a kept lease is still held past its own length")
	}
	l.Release()
	if _, held, _ := deps.Kv.Get(ctx, core.RunLockKey("t1")); held {
		t.Fatal("released")
	}
}
