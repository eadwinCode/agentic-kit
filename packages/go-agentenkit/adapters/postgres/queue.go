package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Handler is what the queue hands a job to: the runtime's worker.
type Handler func(ctx context.Context, job ports.RunJob) error

// QueueOptions tunes NewQueue.
type QueueOptions struct {
	// Poll is how often an idle consumer looks for work. Zero means 500ms.
	Poll time.Duration
	// Lease is how long a claimed job stays invisible to other consumers. It
	// is renewed while the handler runs, so it only needs to outlast one
	// renewal gap plus a crash. Zero means 2 minutes.
	Lease time.Duration
	// Concurrency is how many jobs one consumer runs at once. Zero means 4.
	// The per-thread run lock keeps two jobs on one thread apart regardless.
	Concurrency int
	// MaxAttempts is how many deliveries a job gets before it is dropped as
	// dead. The engine's own §2.8 policy re-enqueues a run that fails; this
	// only guards against a handler that keeps dying. Zero means 5.
	MaxAttempts int
}

// Queue is a durable Queue over one Postgres table.
//
// Enqueue is one INSERT; a delay is a future runAt. The consumer claims
// with SELECT ... FOR UPDATE SKIP LOCKED, so several processes can consume
// the same table, and each claim carries a lease that is renewed while the
// job runs and lapses when its worker dies. At-least-once: a job whose
// worker crashed is redelivered once its lease expires, and the engine's
// run lock makes the duplicate a no-op.
type Queue struct {
	db    *sql.DB
	table string
	opts  QueueOptions

	mu      sync.Mutex
	handler Handler
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewQueue creates the table if it is missing and returns the queue. Nothing
// is consumed until Bind.
func NewQueue(ctx context.Context, db *sql.DB, opts QueueOptions, storageOpts ...Option) (*Queue, error) {
	s := &Storage{prefix: "agentenkit_"}
	for _, o := range storageOpts {
		o(s)
	}
	if opts.Poll <= 0 {
		opts.Poll = 500 * time.Millisecond
	}
	if opts.Lease <= 0 {
		opts.Lease = 2 * time.Minute
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	q := &Queue{db: db, table: s.prefix + "jobs", opts: opts}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS ` + q.table + ` (
		   id TEXT PRIMARY KEY, payload JSONB NOT NULL,
		   "runAt" TIMESTAMPTZ NOT NULL DEFAULT now(), "lockedUntil" TIMESTAMPTZ,
		   attempts INT NOT NULL DEFAULT 0, "createdAt" TIMESTAMPTZ NOT NULL DEFAULT now())`,
		`CREATE INDEX IF NOT EXISTS ` + q.table + `_ready ON ` + q.table + `("runAt", "lockedUntil")`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("postgres queue schema: %w", err)
		}
	}
	return q, nil
}

func (q *Queue) Enqueue(ctx context.Context, job ports.RunJob, opts *ports.EnqueueOptions) error {
	var delay time.Duration
	if opts != nil && opts.Delay > 0 {
		delay = opts.Delay
	}
	payload, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = q.db.ExecContext(ctx,
		`INSERT INTO `+q.table+` (id, payload, "runAt") VALUES ($1, $2, now() + $3 * interval '1 millisecond')`,
		core.NewID(), string(payload), delay.Milliseconds())
	return err
}

// Bind wires the worker and starts consuming. Call once; Close stops it.
func (q *Queue) Bind(handler Handler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.cancel != nil {
		return
	}
	q.handler = handler
	ctx, cancel := context.WithCancel(context.Background())
	q.cancel = cancel
	slots := make(chan struct{}, q.opts.Concurrency)
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case slots <- struct{}{}:
			}
			job, ok, err := q.claim(ctx)
			if err != nil || !ok {
				<-slots
				select {
				case <-ctx.Done():
					return
				case <-time.After(q.opts.Poll):
				}
				continue
			}
			q.wg.Add(1)
			go func() {
				defer q.wg.Done()
				defer func() { <-slots }()
				q.execute(ctx, job)
			}()
		}
	}()
}

// Close stops the consumer and waits for running jobs to return.
func (q *Queue) Close() {
	q.mu.Lock()
	cancel := q.cancel
	q.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	q.wg.Wait()
}

type claimed struct {
	id       string
	job      ports.RunJob
	attempts int
}

// claim takes one ready job, leasing it for opts.Lease. The SKIP LOCKED
// select and the lease update run in one transaction, so two consumers
// cannot take the same row.
func (q *Queue) claim(ctx context.Context) (claimed, bool, error) {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return claimed{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	var c claimed
	var payload []byte
	err = tx.QueryRowContext(ctx,
		`SELECT id, payload, attempts FROM `+q.table+`
		 WHERE "runAt" <= now() AND ("lockedUntil" IS NULL OR "lockedUntil" <= now())
		 ORDER BY "runAt" LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&c.id, &payload, &c.attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return claimed{}, false, nil
	}
	if err != nil {
		return claimed{}, false, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE `+q.table+` SET "lockedUntil" = now() + $2 * interval '1 millisecond', attempts = attempts + 1 WHERE id = $1`,
		c.id, q.opts.Lease.Milliseconds()); err != nil {
		return claimed{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return claimed{}, false, err
	}
	if err := json.Unmarshal(payload, &c.job); err != nil {
		// A row that cannot be read will never run: drop it rather than
		// redeliver it for ever.
		_, _ = q.db.ExecContext(ctx, `DELETE FROM `+q.table+` WHERE id = $1`, c.id)
		return claimed{}, false, nil
	}
	c.attempts++
	return c, true, nil
}

// execute runs one claimed job, renewing its lease until the handler
// returns. The job is deleted when the handler returns nil (the engine has
// taken responsibility for the run, redrives included) or when it has used
// up its attempts; otherwise the lease lapses and it is redelivered.
func (q *Queue) execute(ctx context.Context, c claimed) {
	renewCtx, stopRenew := context.WithCancel(ctx)
	defer stopRenew()
	go func() {
		ticker := time.NewTicker(q.opts.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				_, _ = q.db.ExecContext(renewCtx,
					`UPDATE `+q.table+` SET "lockedUntil" = now() + $2 * interval '1 millisecond' WHERE id = $1`,
					c.id, q.opts.Lease.Milliseconds())
			}
		}
	}()
	q.mu.Lock()
	handler := q.handler
	q.mu.Unlock()
	err := handler(ctx, c.job)
	stopRenew()
	// The delete must land even while the process is shutting down.
	done := context.WithoutCancel(ctx)
	if err == nil || c.attempts >= q.opts.MaxAttempts {
		_, _ = q.db.ExecContext(done, `DELETE FROM `+q.table+` WHERE id = $1`, c.id)
		return
	}
	// Release early so the redelivery does not wait out the whole lease.
	_, _ = q.db.ExecContext(done, `UPDATE `+q.table+` SET "lockedUntil" = NULL WHERE id = $1`, c.id)
}

// Pending counts jobs not yet taken (ready or delayed). For tests and
// dashboards.
func (q *Queue) Pending(ctx context.Context) (int, error) {
	var n int
	err := q.db.QueryRowContext(ctx, `SELECT count(*) FROM `+q.table+` WHERE "lockedUntil" IS NULL OR "lockedUntil" <= now()`).Scan(&n)
	return n, err
}
