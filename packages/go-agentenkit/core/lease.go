package core

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The run lock's value is "<runID>/<dispatchID>/<nonce>" (§3.4).
//
//   - runID says which run holds the thread.
//   - dispatchID says which delivery of that run: it rides on the job, so a
//     queue that delivers the same job twice delivers the same dispatchID,
//     and the second copy is known for a duplicate.
//   - nonce is new on every acquire. Two holders of the same job (a stalled
//     worker whose lease lapsed, and the redelivery that took it over) differ
//     here and nowhere else, so the stalled one can neither renew nor free
//     the new holder's lock.
//
// A value written before dispatch ids existed is a bare run id; it parses as
// that run with no dispatch.
const lockSep = "/"

// ParseLockValue splits a run lock value into the run and the delivery that
// hold it. A bare run id has no dispatch.
func ParseLockValue(v string) (runID, dispatchID string) {
	parts := strings.SplitN(v, lockSep, 3)
	if len(parts) < 3 {
		return v, ""
	}
	return parts[0], parts[1]
}

// Lease is one holder's grip on a thread's run lock.
type Lease struct {
	deps  ports.RuntimePorts
	key   string
	value string
	lost  atomic.Bool

	mu      sync.Mutex
	stop    chan struct{}
	done    chan struct{}
	release sync.Once
}

// AcquireRunLock takes the thread's run lock for a run and one delivery of
// it. It returns nil, nil when someone else holds the lock.
func AcquireRunLock(ctx context.Context, deps ports.RuntimePorts, threadID, runID, dispatchID string) (*Lease, error) {
	if dispatchID == "" {
		dispatchID = NewID()
	}
	l := &Lease{
		deps:  deps,
		key:   RunLockKey(threadID),
		value: runID + lockSep + dispatchID + lockSep + NewID(),
	}
	ok, err := deps.Kv.Set(ctx, l.key, l.value, ports.SetOptions{OnlyIfNotExists: true, Expiry: deps.Config.RunLockLease})
	if err != nil || !ok {
		return nil, err
	}
	return l, nil
}

// Lost reports that the lease could not be kept. Another worker may own the
// thread by now, so the holder must stop writing to it.
func (l *Lease) Lost() bool { return l.lost.Load() }

// Keep renews the lease in the background until Release, the way a queue
// renews a job lease: an expired lock then means a dead worker and nothing
// else. onLost runs once, when the lease cannot be kept. That is when the
// key is gone or holds another holder's value, or when no renewal has
// landed for two thirds of the lease (the lease is then close to lapsing,
// and another worker may take it before the next try).
func (l *Lease) Keep(onLost func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stop != nil {
		return
	}
	l.stop, l.done = make(chan struct{}), make(chan struct{})
	lease := l.deps.Config.RunLockLease
	every := max(lease/6, time.Millisecond)
	go func() {
		defer close(l.done)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		lastOK := time.Now()
		for {
			select {
			case <-l.stop:
				return
			case <-ticker.C:
			}
			ctx, cancel := context.WithTimeout(context.Background(), every)
			ok, err := l.deps.Kv.SetIfValue(ctx, l.key, l.value, l.value, lease)
			cancel()
			switch {
			case err == nil && ok:
				lastOK = time.Now()
				continue
			case err == nil:
				Logger(l.deps).Error("run lock taken by another holder; ending the segment", "key", l.key)
			case time.Since(lastOK) > lease*2/3:
				Logger(l.deps).Error("run lock could not be renewed in time; ending the segment", "key", l.key, "err", err)
			default:
				Logger(l.deps).Warn("run lock renewal failed; retrying", "key", l.key, "err", err)
				continue
			}
			l.lost.Store(true)
			if onLost != nil {
				onLost()
			}
			return
		}
	}()
}

// Release stops the renewal and frees the lock, only while it is still this
// holder's. Safe to call more than once.
func (l *Lease) Release() {
	l.release.Do(func() {
		l.mu.Lock()
		stop, done := l.stop, l.done
		l.mu.Unlock()
		if stop != nil {
			close(stop)
			<-done
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = l.deps.Kv.DelIfValue(ctx, l.key, l.value)
	})
}
