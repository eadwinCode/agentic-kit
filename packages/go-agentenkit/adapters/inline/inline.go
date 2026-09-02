// Package inline holds an in-process queue that actually dispatches (§2.8).
package inline

import (
	"context"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Handler is what the queue hands a job to: the runtime's worker.
type Handler func(ctx context.Context, job ports.RunJob) error

// Queue dispatches in-process, on a later goroutine.
//
// The memory Queue only collects jobs, which is right for tests that drive
// the worker by hand but useless for running the platform. This one hands
// each job to the worker on its own goroutine, so Enqueue still returns
// immediately and a run keeps outliving the request that started it: the
// property the whole design rests on.
//
// It honours Delay, so the HITL expiry (§2.5) and the lock-conflict redrive
// (§2.8) work in development exactly as they do against a real queue.
//
// What it is NOT: durable. A process restart loses whatever was in flight,
// which is precisely what the durable adapters exist to fix. Development
// only.
type Queue struct {
	mu      sync.Mutex
	handler Handler
	pending map[*time.Timer]struct{}
	wg      sync.WaitGroup
	ctx     context.Context
}

// New makes a queue. Jobs run with a context derived from ctx; pass
// context.Background() for a queue that lives as long as the process.
func New(ctx context.Context) *Queue {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Queue{pending: map[*time.Timer]struct{}{}, ctx: ctx}
}

// Bind wires the worker. The queue and the worker each need the other, so
// the queue is attached once the runtime exists. Nothing dispatches until
// this runs.
func (q *Queue) Bind(handler Handler) {
	q.mu.Lock()
	q.handler = handler
	q.mu.Unlock()
}

func (q *Queue) Enqueue(_ context.Context, job ports.RunJob, opts *ports.EnqueueOptions) error {
	var delay time.Duration
	if opts != nil {
		delay = opts.Delay
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.wg.Add(1)
	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		defer q.wg.Done()
		q.mu.Lock()
		delete(q.pending, timer)
		handler := q.handler
		q.mu.Unlock()
		if handler == nil || q.ctx.Err() != nil {
			return
		}
		// Detached on purpose: a queue consumer's failure is the worker's
		// business (§2.8 redrive), never the enqueuer's.
		_ = handler(q.ctx, job)
	})
	q.pending[timer] = struct{}{}
	return nil
}

// Clear drops everything still scheduled: for tests and clean shutdown.
func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for t := range q.pending {
		if t.Stop() {
			q.wg.Done()
		}
		delete(q.pending, t)
	}
}

// Wait blocks until every scheduled job has run. For tests.
func (q *Queue) Wait() { q.wg.Wait() }
