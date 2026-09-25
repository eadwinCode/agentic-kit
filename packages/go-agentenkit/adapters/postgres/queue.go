package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Handler is what the queue hands a job to: the runtime's worker.
type Handler func(ctx context.Context, job ports.RunJob) error

// DeadHandler is told about a job the queue gave up on: its attempts are
// spent, or its payload cannot be read. The row is kept, dead, for an
// operator; the handler is where the engine fails the run so its thread does
// not stay RUNNING for ever.
type DeadHandler func(ctx context.Context, job ports.RunJob, attempts int, cause error)

// QueueOptions tunes NewQueue.
type QueueOptions struct {
	// Poll is how often an idle consumer looks for work. Zero means 500ms.
	// With a Listener set, a NOTIFY wakes the consumer at once and the poll
	// is only the backstop for delayed rows and a missed notification.
	Poll time.Duration
	// Lease is how long a claimed job stays invisible to other consumers. It
	// is renewed while the handler runs, so it only needs to outlast one
	// renewal gap plus a crash. Zero means 2 minutes.
	Lease time.Duration
	// Concurrency is how many jobs one consumer runs at once. Zero means 4.
	// The per-thread run lock keeps two jobs on one thread apart regardless.
	Concurrency int
	// MaxAttempts is how many deliveries a job gets before it is kept as
	// dead. The engine's own §2.8 policy re-enqueues a run that fails; this
	// only guards against a handler that keeps dying. Zero means 5.
	MaxAttempts int
	// Namespace scopes every row this queue writes and every claim it makes,
	// so two deployments can share one table without taking each other's
	// jobs. Empty is the default namespace.
	Namespace string
	// MaxDepth refuses a fresh dispatch (ports.ErrQueueFull) once this many
	// first dispatches are waiting. Retries, redrives, resumes and expiries
	// always go in. Zero means unbounded.
	MaxDepth int
	// MaxPayloadBytes refuses a job whose encoded payload is larger
	// (ports.ErrPayloadTooLarge). Zero means 1 MiB.
	MaxPayloadBytes int
	// MaxAge keeps a fresh dispatch from running once it has been ready for
	// this long: the row is kept as dead instead, and the dead handler fails
	// its run. The wait counts from when the job was due, not when it was
	// written, so a delay never counts against it. Only JobDispatch is
	// capped: a resume, an expiry or a reclaim is the platform finishing work
	// it already took on, and failing it would lose that work. Zero means a
	// job waits for ever.
	MaxAge time.Duration
	// RetryBackoff is the delay before a job whose handler failed is offered
	// again, doubled on every further failure up to RetryBackoffMax. Zero
	// means one second; RetryBackoffMax zero means one minute.
	RetryBackoff    time.Duration
	RetryBackoffMax time.Duration
	// MaxRunTime cancels a handler that runs longer than this. The engine
	// bounds its own segments; this is the backstop for a handler that
	// ignores its context. Zero means no bound.
	MaxRunTime time.Duration
	// DrainTimeout is how long Close lets running handlers finish before it
	// cancels them. Zero cancels them at once.
	DrainTimeout time.Duration
	// Listener wakes the consumer on an enqueue instead of waiting out the
	// poll. Optional; pgxlisten implements it.
	Listener Listener
	// Log is where the queue reports what it did: a claim that failed, a
	// lease it could not renew, a job it gave up on. Nil means slog.Default().
	Log *slog.Logger
}

// Queue is a durable Queue over one Postgres table.
//
// Enqueue is one INSERT; a delay is a future runAt. The consumer claims
// with SELECT ... FOR UPDATE SKIP LOCKED, so several processes can consume
// the same table, and each claim carries a lease that is renewed while the
// job runs and lapses when its worker dies. At-least-once: a job whose
// worker crashed is redelivered once its lease expires, and the engine's
// run lock makes the duplicate a no-op.
//
// The claim picks across partitions: the partition with the fewest jobs in
// flight goes first, then priority, then dispatch time. One tenant's burst
// cannot hold the head of the line against everyone else.
type Queue struct {
	db      *sql.DB
	table   string
	control string
	channel string
	opts    QueueOptions
	log     *slog.Logger

	mu        sync.Mutex
	handler   Handler
	onDead    DeadHandler
	claimCtx  context.Context
	stopClaim context.CancelFunc
	jobsCtx   context.Context
	stopJobs  context.CancelFunc
	wg        sync.WaitGroup
	inFlight  sync.WaitGroup

	statsMu      sync.Mutex
	lastClaimAt  time.Time
	claimErrors  int
	lastClaimLog time.Time
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
	if opts.MaxPayloadBytes <= 0 {
		opts.MaxPayloadBytes = 1 << 20
	}
	if opts.RetryBackoff <= 0 {
		opts.RetryBackoff = time.Second
	}
	if opts.RetryBackoffMax <= 0 {
		opts.RetryBackoffMax = time.Minute
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	q := &Queue{db: db, table: s.prefix + "jobs", control: s.prefix + "jobs_control", channel: s.prefix + "jobs", opts: opts, log: log.With("queue", s.prefix+"jobs", "namespace", opts.Namespace)}
	for _, stmt := range q.schema() {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("postgres queue schema: %w", err)
		}
	}
	return q, nil
}

// schema is the table as it stands, written so an older table grows the
// columns it is missing: every ADD COLUMN is IF NOT EXISTS, and the new
// indexes carry new names so the old ones stay harmless.
func (q *Queue) schema() []string {
	t := q.table
	return []string{
		`CREATE TABLE IF NOT EXISTS ` + t + ` (
		   id TEXT PRIMARY KEY, payload JSONB NOT NULL,
		   "runAt" TIMESTAMPTZ NOT NULL DEFAULT now(), "lockedUntil" TIMESTAMPTZ,
		   attempts INT NOT NULL DEFAULT 0, "createdAt" TIMESTAMPTZ NOT NULL DEFAULT now())`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS partition TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS "sortAt" TIMESTAMPTZ NOT NULL DEFAULT now()`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS key TEXT`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS "threadId" TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS "runId" TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS "claimedBy" TEXT`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS "deadAt" TIMESTAMPTZ`,
		`ALTER TABLE ` + t + ` ADD COLUMN IF NOT EXISTS "lastError" TEXT`,
		`CREATE INDEX IF NOT EXISTS ` + t + `_claim ON ` + t + `(namespace, "runAt", "lockedUntil") WHERE "deadAt" IS NULL`,
		`CREATE INDEX IF NOT EXISTS ` + t + `_lease ON ` + t + `(namespace, partition, "lockedUntil") WHERE "deadAt" IS NULL`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ` + t + `_key ON ` + t + `(namespace, key) WHERE key IS NOT NULL AND "deadAt" IS NULL`,
		`CREATE INDEX IF NOT EXISTS ` + t + `_run ON ` + t + `("runId")`,
		`CREATE TABLE IF NOT EXISTS ` + q.control + ` (
		   namespace TEXT PRIMARY KEY, paused BOOLEAN NOT NULL DEFAULT false,
		   "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT now())`,
	}
}

// ready is the predicate for a row a worker may take now.
const ready = `"deadAt" IS NULL AND "runAt" <= now() AND ("lockedUntil" IS NULL OR "lockedUntil" <= now())`

func (q *Queue) Enqueue(ctx context.Context, job ports.RunJob, opts *ports.EnqueueOptions) error {
	var delay time.Duration
	var key sql.NullString
	priority := 0
	if opts != nil {
		if opts.Delay > 0 {
			delay = opts.Delay
		}
		if opts.Key != "" {
			key = sql.NullString{String: opts.Key, Valid: true}
		}
		priority = opts.Priority
	}
	payload, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if len(payload) > q.opts.MaxPayloadBytes {
		return fmt.Errorf("%w: %d bytes, cap %d", ports.ErrPayloadTooLarge, len(payload), q.opts.MaxPayloadBytes)
	}
	// The cap counts every row ready to run, but refuses only NEW work. A
	// retry, a resume or an expiry belongs to a run that is already under
	// way; refusing it would strand that run.
	if q.opts.MaxDepth > 0 && job.Kind == ports.JobDispatch {
		var waiting int
		if err := q.db.QueryRowContext(ctx,
			`SELECT count(*) FROM `+q.table+` WHERE namespace = $1 AND `+ready,
			q.opts.Namespace).Scan(&waiting); err != nil {
			return err
		}
		if waiting >= q.opts.MaxDepth {
			q.log.Warn("queue full: dispatch refused", "thread", job.ThreadID, "run", job.RunID, "waiting", waiting, "maxDepth", q.opts.MaxDepth)
			return fmt.Errorf("%w: %d waiting, cap %d", ports.ErrQueueFull, waiting, q.opts.MaxDepth)
		}
	}
	sortAt := time.Now()
	if job.DispatchedAt > 0 {
		sortAt = time.UnixMilli(job.DispatchedAt)
	}
	// The NOTIFY rides the same statement as the INSERT: a duplicate key
	// inserts nothing and so wakes nobody.
	var id sql.NullString
	err = q.db.QueryRowContext(ctx,
		`WITH ins AS (
		   INSERT INTO `+q.table+` (id, payload, "runAt", namespace, partition, priority, "sortAt", key, kind, "threadId", "runId")
		   VALUES ($1, $2, now() + $3 * interval '1 millisecond', $4, $5, $6, $7, $8, $9, $10, $11)
		   ON CONFLICT (namespace, key) WHERE key IS NOT NULL AND "deadAt" IS NULL DO NOTHING
		   RETURNING id)
		 SELECT ins.id, pg_notify($12, $4) FROM ins`,
		core.NewID(), string(payload), delay.Milliseconds(), q.opts.Namespace, job.PartitionKey, priority, sortAt, key,
		string(job.Kind), job.ThreadID, job.RunID, q.channel).Scan(&id, new(any))
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ports.ErrDuplicateJob, opts.Key)
	}
	return err
}

// Cancel drops every waiting row under key. A row a worker already holds is
// left to finish: it is a correct no-op by the engine's own rules.
func (q *Queue) Cancel(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	_, err := q.db.ExecContext(ctx,
		`DELETE FROM `+q.table+` WHERE namespace = $1 AND key = $2 AND ("lockedUntil" IS NULL OR "lockedUntil" <= now())`,
		q.opts.Namespace, key)
	return err
}

// Find returns the earliest live row for a run, or nil.
func (q *Queue) Find(ctx context.Context, runID string) (*ports.QueuedJob, error) {
	if runID == "" {
		return nil, nil
	}
	var j ports.QueuedJob
	var locked sql.NullTime
	var priority int
	var sortAt time.Time
	err := q.db.QueryRowContext(ctx,
		`SELECT id, "runId", "threadId", kind, attempts, "runAt", "lockedUntil", priority, "sortAt" FROM `+q.table+`
		 WHERE namespace = $1 AND "runId" = $2 AND "deadAt" IS NULL ORDER BY "sortAt" LIMIT 1`,
		q.opts.Namespace, runID).Scan(&j.ID, &j.RunID, &j.ThreadID, &j.Kind, &j.Attempts, &j.RunAt, &locked, &priority, &sortAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if locked.Valid {
		t := locked.Time
		j.LockedUntil = &t
	}
	j.Position = -1
	if err := q.db.QueryRowContext(ctx,
		`SELECT count(*) FROM `+q.table+` WHERE namespace = $1 AND `+ready+` AND (priority > $2 OR (priority = $2 AND "sortAt" < $3))`,
		q.opts.Namespace, priority, sortAt).Scan(&j.Position); err != nil {
		return &j, nil // the row is what matters; the position is a courtesy
	}
	return &j, nil
}

// Stats counts the namespace's rows and adds what this process knows about
// its own consumer.
func (q *Queue) Stats(ctx context.Context) (ports.QueueStats, error) {
	var s ports.QueueStats
	var oldest sql.NullFloat64
	var paused sql.NullBool
	err := q.db.QueryRowContext(ctx,
		`SELECT
		   count(*) FILTER (WHERE `+ready+`),
		   count(*) FILTER (WHERE "deadAt" IS NULL AND "runAt" > now() AND ("lockedUntil" IS NULL OR "lockedUntil" <= now())),
		   count(*) FILTER (WHERE "deadAt" IS NULL AND "lockedUntil" > now()),
		   count(*) FILTER (WHERE "deadAt" IS NOT NULL),
		   EXTRACT(EPOCH FROM now() - min("runAt") FILTER (WHERE `+ready+`)) * 1000,
		   (SELECT paused FROM `+q.control+` WHERE namespace = $1)
		 FROM `+q.table+` WHERE namespace = $1`, q.opts.Namespace).
		Scan(&s.Ready, &s.Delayed, &s.InFlight, &s.Dead, &oldest, &paused)
	if err != nil {
		return ports.QueueStats{}, err
	}
	if oldest.Valid && oldest.Float64 > 0 {
		s.OldestReadyMs = int64(oldest.Float64)
	}
	s.Paused = paused.Valid && paused.Bool
	q.statsMu.Lock()
	if !q.lastClaimAt.IsZero() {
		t := q.lastClaimAt
		s.LastClaimAt = &t
	}
	s.ClaimErrors = q.claimErrors
	q.statsMu.Unlock()
	return s, nil
}

// Pending counts jobs not yet taken (ready or delayed). For tests and
// dashboards; Stats says more.
func (q *Queue) Pending(ctx context.Context) (int, error) {
	var n int
	err := q.db.QueryRowContext(ctx,
		`SELECT count(*) FROM `+q.table+` WHERE namespace = $1 AND "deadAt" IS NULL AND ("lockedUntil" IS NULL OR "lockedUntil" <= now())`,
		q.opts.Namespace).Scan(&n)
	return n, err
}

// Pause tells every consumer of this namespace to stop claiming. Durable,
// so it holds across every process and a restart; Resume lifts it.
func (q *Queue) Pause(ctx context.Context) error {
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO `+q.control+` (namespace, paused) VALUES ($1, true)
		 ON CONFLICT (namespace) DO UPDATE SET paused = true, "updatedAt" = now()`, q.opts.Namespace)
	if err == nil {
		q.log.Warn("queue paused")
	}
	return err
}

// Resume lets consumers claim again and wakes them.
func (q *Queue) Resume(ctx context.Context) error {
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO `+q.control+` (namespace, paused) VALUES ($1, false)
		 ON CONFLICT (namespace) DO UPDATE SET paused = false, "updatedAt" = now()`, q.opts.Namespace)
	if err != nil {
		return err
	}
	q.log.Info("queue resumed")
	_, err = q.db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, q.channel, q.opts.Namespace)
	return err
}

// PurgeFilter picks waiting rows for Purge. Every set field narrows it; an
// empty filter is refused, so a typo cannot empty the queue.
type PurgeFilter struct {
	ThreadID string
	RunID    string
	Kind     *ports.JobKind
	// OlderThan keeps rows enqueued more recently than this.
	OlderThan time.Duration
}

// Purge deletes waiting rows that match. Rows a worker holds are left alone.
func (q *Queue) Purge(ctx context.Context, f PurgeFilter) (int64, error) {
	where := []string{`namespace = $1`, `("lockedUntil" IS NULL OR "lockedUntil" <= now())`}
	args := []any{q.opts.Namespace}
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.ThreadID != "" {
		add(`"threadId" = $%d`, f.ThreadID)
	}
	if f.RunID != "" {
		add(`"runId" = $%d`, f.RunID)
	}
	if f.Kind != nil {
		add(`kind = $%d`, string(*f.Kind))
	}
	if f.OlderThan > 0 {
		add(`"createdAt" < now() - $%d * interval '1 millisecond'`, f.OlderThan.Milliseconds())
	}
	if len(where) == 2 {
		return 0, errors.New("postgres queue: purge needs a filter")
	}
	res, err := q.db.ExecContext(ctx, `DELETE FROM `+q.table+` WHERE `+strings.Join(where, " AND "), args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if n > 0 {
		q.log.Warn("queue purged", "rows", n, "thread", f.ThreadID, "run", f.RunID)
	}
	return n, err
}

// Delete removes one row by id, whatever its state.
func (q *Queue) Delete(ctx context.Context, id string) (bool, error) {
	res, err := q.db.ExecContext(ctx, `DELETE FROM `+q.table+` WHERE namespace = $1 AND id = $2`, q.opts.Namespace, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// DeadJob is a row the queue gave up on, kept with the reason.
type DeadJob struct {
	ID        string       `json:"id"`
	Job       ports.RunJob `json:"job"`
	Attempts  int          `json:"attempts"`
	DeadAt    time.Time    `json:"deadAt"`
	LastError string       `json:"lastError"`
}

// ListDead lists dead rows, newest first.
func (q *Queue) ListDead(ctx context.Context, limit int) ([]DeadJob, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := q.db.QueryContext(ctx,
		`SELECT id, payload, attempts, "deadAt", COALESCE("lastError", '') FROM `+q.table+`
		 WHERE namespace = $1 AND "deadAt" IS NOT NULL ORDER BY "deadAt" DESC LIMIT $2`, q.opts.Namespace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []DeadJob{}
	for rows.Next() {
		var d DeadJob
		var payload []byte
		if err := rows.Scan(&d.ID, &payload, &d.Attempts, &d.DeadAt, &d.LastError); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(payload, &d.Job)
		out = append(out, d)
	}
	return out, rows.Err()
}

// Redrive puts a dead row back in line with a fresh attempt budget.
func (q *Queue) Redrive(ctx context.Context, id string) (bool, error) {
	res, err := q.db.ExecContext(ctx,
		`UPDATE `+q.table+` SET "deadAt" = NULL, "lastError" = NULL, attempts = 0, "runAt" = now(), "lockedUntil" = NULL, "claimedBy" = NULL
		 WHERE namespace = $1 AND id = $2 AND "deadAt" IS NOT NULL`, q.opts.Namespace, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if n > 0 {
		q.log.Info("dead job redriven", "id", id)
		_, _ = q.db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, q.channel, q.opts.Namespace)
	}
	return n > 0, err
}

// PurgeDead drops dead rows older than the given age.
func (q *Queue) PurgeDead(ctx context.Context, olderThan time.Duration) (int64, error) {
	res, err := q.db.ExecContext(ctx,
		`DELETE FROM `+q.table+` WHERE namespace = $1 AND "deadAt" IS NOT NULL AND "deadAt" < now() - $2 * interval '1 millisecond'`,
		q.opts.Namespace, olderThan.Milliseconds())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// BindOption tunes Bind.
type BindOption func(*Queue)

// WithDeadHandler is told about every job the queue gives up on.
func WithDeadHandler(fn DeadHandler) BindOption { return func(q *Queue) { q.onDead = fn } }

// Bind wires the worker and starts consuming. Call once; Close stops it.
func (q *Queue) Bind(handler Handler, opts ...BindOption) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopClaim != nil {
		return
	}
	q.handler = handler
	for _, o := range opts {
		o(q)
	}
	q.claimCtx, q.stopClaim = context.WithCancel(context.Background())
	q.jobsCtx, q.stopJobs = context.WithCancel(context.Background())
	claimCtx := q.claimCtx
	slots := make(chan struct{}, q.opts.Concurrency)
	wake := make(chan struct{}, 1)
	if q.opts.Listener != nil {
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			_ = q.opts.Listener.Listen(claimCtx, q.channel, func(payload string) {
				if payload != q.opts.Namespace {
					return
				}
				select {
				case wake <- struct{}{}:
				default:
				}
			})
		}()
	}
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		for {
			select {
			case <-claimCtx.Done():
				return
			case slots <- struct{}{}:
			}
			job, ok, err := q.claim(claimCtx)
			if err != nil {
				<-slots
				q.noteClaimError(err)
				if !q.idle(claimCtx, wake) {
					return
				}
				continue
			}
			if !ok {
				<-slots
				q.noteIdle()
				if !q.idle(claimCtx, wake) {
					return
				}
				continue
			}
			q.noteClaim()
			q.wg.Add(1)
			q.inFlight.Add(1)
			go func() {
				defer q.wg.Done()
				defer q.inFlight.Done()
				defer func() { <-slots }()
				q.execute(job)
			}()
		}
	}()
}

// idle waits for a wake-up or the poll, with jitter so several consumers do
// not tick in step. False once the consumer is stopped.
func (q *Queue) idle(ctx context.Context, wake <-chan struct{}) bool {
	poll := q.opts.Poll + time.Duration(rand.Int64N(int64(q.opts.Poll)/2+1)) - q.opts.Poll/4
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return true
	case <-time.After(poll):
		return true
	}
}

func (q *Queue) noteClaim() {
	q.statsMu.Lock()
	q.lastClaimAt = time.Now()
	q.claimErrors = 0
	q.statsMu.Unlock()
}

func (q *Queue) noteIdle() {
	q.statsMu.Lock()
	q.claimErrors = 0
	q.statsMu.Unlock()
}

// noteClaimError counts the failure and logs it, once per half minute, so
// a database that refuses connections is a log line and a stat, never an
// idle queue.
func (q *Queue) noteClaimError(err error) {
	q.statsMu.Lock()
	q.claimErrors++
	n := q.claimErrors
	logIt := time.Since(q.lastClaimLog) > 30*time.Second
	if logIt {
		q.lastClaimLog = time.Now()
	}
	q.statsMu.Unlock()
	if logIt {
		q.log.Error("queue claim failed", "err", err, "consecutive", n)
	}
}

// Close stops claiming, gives running handlers DrainTimeout to finish, then
// cancels the ones still running and waits for everything to return.
func (q *Queue) Close() {
	q.mu.Lock()
	stopClaim, stopJobs := q.stopClaim, q.stopJobs
	q.mu.Unlock()
	if stopClaim == nil {
		return
	}
	stopClaim()
	if q.opts.DrainTimeout > 0 {
		drained := make(chan struct{})
		go func() {
			q.inFlight.Wait()
			close(drained)
		}()
		select {
		case <-drained:
		case <-time.After(q.opts.DrainTimeout):
			q.log.Warn("queue drain timed out; cancelling running jobs", "after", q.opts.DrainTimeout)
		}
	}
	stopJobs()
	q.wg.Wait()
}

type claimed struct {
	id       string
	token    string
	job      ports.RunJob
	attempts int
}

// candidates is the head of the line: ready rows, the partition with the
// fewest jobs in flight first, then priority, then dispatch time.
func (q *Queue) candidates(ctx context.Context) ([]string, error) {
	rows, err := q.db.QueryContext(ctx,
		`WITH leased AS (
		   SELECT partition, count(*) AS n FROM `+q.table+`
		   WHERE namespace = $1 AND "deadAt" IS NULL AND "lockedUntil" > now()
		   GROUP BY partition)
		 SELECT j.id FROM `+q.table+` j LEFT JOIN leased l ON l.partition = j.partition
		 WHERE j.namespace = $1 AND j.`+ready+`
		   AND NOT EXISTS (SELECT 1 FROM `+q.control+` c WHERE c.namespace = $1 AND c.paused)
		 ORDER BY COALESCE(l.n, 0) ASC, j.priority DESC, j."sortAt" ASC
		 LIMIT 8`, q.opts.Namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// claim takes one ready job, leasing it for opts.Lease. The SKIP LOCKED
// select and the lease update run in one transaction, so two consumers
// cannot take the same row. A row that has waited past MaxAge, or whose
// payload cannot be read, is kept as dead instead of run.
func (q *Queue) claim(ctx context.Context) (claimed, bool, error) {
	ids, err := q.candidates(ctx)
	if err != nil {
		return claimed{}, false, err
	}
	for _, id := range ids {
		c, taken, err := q.take(ctx, id)
		if err != nil {
			return claimed{}, false, err
		}
		if taken {
			return c, true, nil
		}
	}
	return claimed{}, false, nil
}

func (q *Queue) take(ctx context.Context, id string) (claimed, bool, error) {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return claimed{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	var c claimed
	var payload []byte
	var kind string
	var ageMs float64
	// The age is measured on the database's clock, the one that stamped the
	// row: a worker's own clock can be seconds away from it. It counts from
	// "runAt", when the job became due, so a delayed job starts at zero.
	err = tx.QueryRowContext(ctx,
		`SELECT id, payload, attempts, kind, EXTRACT(EPOCH FROM now() - "runAt") * 1000 FROM `+q.table+`
		 WHERE id = $1 AND `+ready+` FOR UPDATE SKIP LOCKED`, id).Scan(&c.id, &payload, &c.attempts, &kind, &ageMs)
	if errors.Is(err, sql.ErrNoRows) {
		return claimed{}, false, nil // another consumer got here first
	}
	if err != nil {
		return claimed{}, false, err
	}
	// Every earlier claim spent an attempt. A job that already has them all
	// was claimed and never came back: its worker died with it (a panic past
	// recovery, the OOM killer, a kill -9). Running it again is how one bad
	// job takes down every worker in turn, so it is kept as dead instead.
	var cause error
	if c.attempts >= q.opts.MaxAttempts {
		cause = fmt.Errorf("job was claimed %d times and its worker never finished it", c.attempts)
	} else if age := time.Duration(ageMs) * time.Millisecond; q.opts.MaxAge > 0 && ports.JobKind(kind) == ports.JobDispatch && age > q.opts.MaxAge {
		cause = fmt.Errorf("job waited %s, longer than the %s cap", age.Round(time.Second), q.opts.MaxAge)
	}
	if cause != nil {
		if _, err := tx.ExecContext(ctx, `UPDATE `+q.table+` SET "deadAt" = now(), "lastError" = $2, "lockedUntil" = NULL WHERE id = $1`, c.id, cause.Error()); err != nil {
			return claimed{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return claimed{}, false, err
		}
		var job ports.RunJob
		_ = json.Unmarshal(payload, &job)
		q.log.Error("job kept as dead instead of run", "id", c.id, "thread", job.ThreadID, "run", job.RunID, "err", cause)
		q.dead(job, c.attempts, cause)
		return claimed{}, false, nil
	}
	c.token = core.NewID()
	if _, err := tx.ExecContext(ctx,
		`UPDATE `+q.table+` SET "lockedUntil" = now() + $2 * interval '1 millisecond', attempts = attempts + 1, "claimedBy" = $3 WHERE id = $1`,
		c.id, q.opts.Lease.Milliseconds(), c.token); err != nil {
		return claimed{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return claimed{}, false, err
	}
	c.attempts++
	if err := json.Unmarshal(payload, &c.job); err != nil {
		// A row that cannot be read will never run. It is kept, dead, with
		// the reason, so the loss has a trace and can be repaired.
		cause := fmt.Errorf("payload cannot be decoded: %w", err)
		if _, dbErr := q.db.ExecContext(ctx, `UPDATE `+q.table+` SET "deadAt" = now(), "lastError" = $2, "lockedUntil" = NULL WHERE id = $1`, c.id, cause.Error()); dbErr != nil {
			q.log.Error("unreadable job could not be marked dead", "id", c.id, "err", dbErr)
		}
		q.log.Error("job payload unreadable; kept as dead", "id", c.id, "err", err)
		return claimed{}, false, cause
	}
	return c, true, nil
}

// dead tells the dead handler, on a context that shutdown cannot cancel.
func (q *Queue) dead(job ports.RunJob, attempts int, cause error) {
	q.mu.Lock()
	fn := q.onDead
	q.mu.Unlock()
	if fn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fn(ctx, job, attempts, cause)
}

// backoff is how long a failed job waits before its next delivery: the base
// doubled per attempt, capped, with a quarter of jitter.
func (q *Queue) backoff(attempts int) time.Duration {
	d := q.opts.RetryBackoff
	for i := 1; i < attempts && d < q.opts.RetryBackoffMax; i++ {
		d *= 2
	}
	if d > q.opts.RetryBackoffMax {
		d = q.opts.RetryBackoffMax
	}
	return d + time.Duration(rand.Int64N(int64(d)/4+1))
}

// execute runs one claimed job, renewing its lease until the handler
// returns. The job is deleted when the handler returns nil (the engine has
// taken responsibility for the run, redrives included). A handler error
// releases it with a backoff, or keeps it as dead once its attempts are
// spent. A lease this worker can no longer hold cancels the handler: by
// then another consumer may be running the same job.
func (q *Queue) execute(c claimed) {
	q.mu.Lock()
	handler, jobsCtx := q.handler, q.jobsCtx
	q.mu.Unlock()
	log := q.log.With("id", c.id, "thread", c.job.ThreadID, "run", c.job.RunID, "kind", string(c.job.Kind), "attempt", c.attempts)

	var hctx context.Context
	var cancelHandler context.CancelFunc
	if q.opts.MaxRunTime > 0 {
		hctx, cancelHandler = context.WithTimeout(jobsCtx, q.opts.MaxRunTime)
	} else {
		hctx, cancelHandler = context.WithCancel(jobsCtx)
	}
	defer cancelHandler()
	renewCtx, stopRenew := context.WithCancel(context.Background())
	defer stopRenew()
	go func() {
		ticker := time.NewTicker(q.opts.Lease / 3)
		defer ticker.Stop()
		failures := 0
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				res, err := q.db.ExecContext(renewCtx,
					`UPDATE `+q.table+` SET "lockedUntil" = now() + $2 * interval '1 millisecond' WHERE id = $1 AND "claimedBy" = $3 AND "deadAt" IS NULL`,
					c.id, q.opts.Lease.Milliseconds(), c.token)
				if err != nil {
					failures++
					log.Warn("lease renewal failed", "err", err, "consecutive", failures)
					if failures >= 3 {
						log.Error("lease cannot be held; cancelling the job", "consecutive", failures)
						cancelHandler()
						return
					}
					continue
				}
				failures = 0
				if n, _ := res.RowsAffected(); n == 0 {
					log.Error("lease lost to another consumer; cancelling the job")
					cancelHandler()
					return
				}
			}
		}
	}()
	// A panic in the handler is a failed attempt like any other error: it is
	// retried with a backoff and kept as dead once the attempts are spent.
	err := core.CallSafely(func() error { return handler(hctx, c.job) })
	var panicked *core.PanicError
	if errors.As(err, &panicked) {
		log.Error("job handler panicked", "err", err, "stack", string(panicked.Stack))
	}
	stopRenew()
	// Every write below must land even while the process is shutting down.
	done, cancel := context.WithTimeout(context.WithoutCancel(hctx), 15*time.Second)
	defer cancel()
	if err == nil {
		if _, dbErr := q.db.ExecContext(done, `DELETE FROM `+q.table+` WHERE id = $1 AND "claimedBy" = $2`, c.id, c.token); dbErr != nil {
			log.Error("finished job not deleted; it will be redelivered", "err", dbErr)
		}
		return
	}
	if jobsCtx.Err() != nil && errors.Is(err, context.Canceled) {
		// A shutdown cut the handler off. That is not the job's fault, so
		// the delivery is given back rather than spent.
		if _, dbErr := q.db.ExecContext(done,
			`UPDATE `+q.table+` SET "lockedUntil" = NULL, attempts = GREATEST(attempts - 1, 0), "claimedBy" = NULL WHERE id = $1 AND "claimedBy" = $2`,
			c.id, c.token); dbErr != nil {
			log.Error("job not released after shutdown", "err", dbErr)
		}
		log.Info("job released for redelivery after shutdown")
		return
	}
	if c.attempts >= q.opts.MaxAttempts {
		if _, dbErr := q.db.ExecContext(done,
			`UPDATE `+q.table+` SET "deadAt" = now(), "lastError" = $3, "lockedUntil" = NULL WHERE id = $1 AND "claimedBy" = $2`,
			c.id, c.token, err.Error()); dbErr != nil {
			log.Error("dead job not marked", "err", dbErr)
		}
		log.Error("job attempts spent; kept as dead", "err", err, "attempts", c.attempts)
		q.dead(c.job, c.attempts, err)
		return
	}
	// Release early so the redelivery does not wait out the whole lease,
	// but not at once: a dependency that is down does not come back faster
	// for being hammered.
	wait := q.backoff(c.attempts)
	if _, dbErr := q.db.ExecContext(done,
		`UPDATE `+q.table+` SET "lockedUntil" = NULL, "claimedBy" = NULL, "runAt" = now() + $3 * interval '1 millisecond' WHERE id = $1 AND "claimedBy" = $2`,
		c.id, c.token, wait.Milliseconds()); dbErr != nil {
		log.Error("failed job not released", "err", dbErr)
	}
	log.Warn("job failed; redelivery scheduled", "err", err, "in", wait.Round(time.Millisecond), "attempts", c.attempts, "maxAttempts", q.opts.MaxAttempts)
}
