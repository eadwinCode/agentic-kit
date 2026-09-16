package agentenkit_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pgstorage "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres/pgxlisten"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The Postgres queue beyond Enqueue: keys, counts, caps, dead rows, pause,
// fairness, priority, age, drain and namespaces. Each test owns a prefix.

// recorder collects deliveries in order.
type recorder struct {
	mu   sync.Mutex
	seen []string
}

func (r *recorder) add(s string) {
	r.mu.Lock()
	r.seen = append(r.seen, s)
	r.mu.Unlock()
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

func (r *recorder) waitFor(t *testing.T, n int, within time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(r.list()) >= n {
			return r.list()
		}
		time.Sleep(10 * time.Millisecond)
	}
	return r.list()
}

func TestPostgresQueue_KeysDedupeAndCancel(t *testing.T) {
	p := openPgPlatform(t, "qk_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond})
	ctx := context.Background()
	job := ports.RunJob{ThreadID: "t1", RunID: "r1", Kind: ports.JobExpiry}
	if err := p.queue.Enqueue(ctx, job, &ports.EnqueueOptions{Key: "hitl-expiry:c1", Delay: time.Hour}); err != nil {
		t.Fatal(err)
	}
	err := p.queue.Enqueue(ctx, job, &ports.EnqueueOptions{Key: "hitl-expiry:c1", Delay: time.Hour})
	if !errors.Is(err, ports.ErrDuplicateJob) {
		t.Fatalf("a second row under the same key is refused: %v", err)
	}
	found, err := p.queue.Find(ctx, "r1")
	if err != nil || found == nil {
		t.Fatalf("find: %+v %v", found, err)
	}
	mustEqual(t, found.Kind, ports.JobExpiry, "kind")
	mustEqual(t, found.ThreadID, "t1", "thread")
	stats, _ := p.queue.Stats(ctx)
	mustEqual(t, stats.Delayed, 1, "a future row is delayed, not backlog")
	mustEqual(t, stats.Ready, 0, "nothing ready")
	if err := p.queue.Cancel(ctx, "hitl-expiry:c1"); err != nil {
		t.Fatal(err)
	}
	found, _ = p.queue.Find(ctx, "r1")
	if found != nil {
		t.Fatal("cancelled rows are gone")
	}
	if err := p.queue.Cancel(ctx, "nothing-here"); err != nil {
		t.Fatalf("cancelling a missing key is not an error: %v", err)
	}
}

func TestPostgresQueue_StatsAndTheDepthCapSpareRetries(t *testing.T) {
	p := openPgPlatform(t, "qd_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond, MaxDepth: 2})
	ctx := context.Background()
	for i, id := range []string{"a", "b"} {
		if err := p.queue.Enqueue(ctx, ports.RunJob{ThreadID: id, RunID: id}, nil); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}
	err := p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "c", RunID: "c"}, nil)
	if !errors.Is(err, ports.ErrQueueFull) {
		t.Fatalf("a third fresh dispatch is refused: %v", err)
	}
	if err := p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "a", RunID: "a", Kind: ports.JobRetry}, nil); err != nil {
		t.Fatalf("a retry always goes in: %v", err)
	}
	if err := p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "d", RunID: "d", Kind: ports.JobExpiry}, &ports.EnqueueOptions{Delay: time.Hour}); err != nil {
		t.Fatalf("a delayed expiry always goes in: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	stats, err := p.queue.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, stats.Ready, 3, "ready")
	mustEqual(t, stats.Delayed, 1, "delayed")
	mustEqual(t, stats.InFlight, 0, "nothing claimed: not bound")
	mustEqual(t, stats.Dead, 0, "dead")
	if stats.OldestReadyMs <= 0 {
		t.Fatalf("the oldest wait is measured: %+v", stats)
	}
	found, _ := p.queue.Find(ctx, "b")
	mustEqual(t, found.Position, 1, "one row ahead of b")
	pending, _ := p.queue.Pending(ctx)
	mustEqual(t, pending, 4, "pending counts ready and delayed")
}

func TestPostgresQueue_RefusesAnOversizedPayload(t *testing.T) {
	p := openPgPlatform(t, "qp_", pgstorage.QueueOptions{MaxPayloadBytes: 200})
	err := p.queue.Enqueue(context.Background(), ports.RunJob{ThreadID: "t", State: ports.AgentRunState{"blob": strings.Repeat("x", 500)}}, nil)
	if !errors.Is(err, ports.ErrPayloadTooLarge) {
		t.Fatalf("want ErrPayloadTooLarge, got %v", err)
	}
}

func TestPostgresQueue_DeadJobsAreKeptToldAboutAndRedriven(t *testing.T) {
	p := openPgPlatform(t, "qx_", pgstorage.QueueOptions{
		Poll: 20 * time.Millisecond, MaxAttempts: 2, RetryBackoff: 20 * time.Millisecond, RetryBackoffMax: 20 * time.Millisecond,
	})
	ctx := context.Background()
	var mu sync.Mutex
	failing := true
	deliveries := 0
	var dead []string
	p.queue.Bind(func(_ context.Context, job ports.RunJob) error {
		mu.Lock()
		defer mu.Unlock()
		deliveries++
		if failing {
			return errBoom
		}
		return nil
	}, pgstorage.WithDeadHandler(func(_ context.Context, job ports.RunJob, attempts int, cause error) {
		mu.Lock()
		defer mu.Unlock()
		dead = append(dead, job.RunID)
		if attempts != 2 || !errors.Is(cause, errBoom) {
			t.Errorf("dead handler told attempts=%d cause=%v", attempts, cause)
		}
	}))
	if err := p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "t", RunID: "r-dead"}, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(dead) == 1 })
	rows, err := p.queue.ListDead(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("dead rows: %+v %v", rows, err)
	}
	mustEqual(t, rows[0].Job.RunID, "r-dead", "the payload survives")
	mustEqual(t, rows[0].Attempts, 2, "attempts")
	if !strings.Contains(rows[0].LastError, "boom") {
		t.Fatalf("the reason is kept: %q", rows[0].LastError)
	}
	stats, _ := p.queue.Stats(ctx)
	mustEqual(t, stats.Dead, 1, "counted as dead")
	mustEqual(t, stats.Ready, 0, "not ready")
	found, _ := p.queue.Find(ctx, "r-dead")
	if found != nil {
		t.Fatal("a dead row is not a live job")
	}

	mu.Lock()
	failing = false
	mu.Unlock()
	if ok, err := p.queue.Redrive(ctx, rows[0].ID); err != nil || !ok {
		t.Fatalf("redrive: %v %v", ok, err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return deliveries == 3 })
	time.Sleep(50 * time.Millisecond)
	stats, _ = p.queue.Stats(ctx)
	mustEqual(t, stats.Dead, 0, "redriven and finished")
	pending, _ := p.queue.Pending(ctx)
	mustEqual(t, pending, 0, "gone")
}

func TestPostgresQueue_AnUnreadablePayloadIsKeptDeadNotDeleted(t *testing.T) {
	p := openPgPlatform(t, "qu_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond})
	ctx := context.Background()
	// A row an older release could have written: a field with the wrong type.
	if _, err := p.db.ExecContext(ctx, `INSERT INTO qu_jobs (id, payload, "runId", "threadId") VALUES ('bad', '{"threadId": 5}', 'r-bad', '5')`); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	delivered := 0
	p.queue.Bind(func(context.Context, ports.RunJob) error {
		mu.Lock()
		delivered++
		mu.Unlock()
		return nil
	})
	waitFor(t, func() bool {
		rows, _ := p.queue.ListDead(ctx, 10)
		return len(rows) == 1
	})
	rows, _ := p.queue.ListDead(ctx, 10)
	if !strings.Contains(rows[0].LastError, "decoded") {
		t.Fatalf("the reason is kept: %q", rows[0].LastError)
	}
	time.Sleep(60 * time.Millisecond)
	mu.Lock()
	mustEqual(t, delivered, 0, "never handed to the worker")
	mu.Unlock()
	var count int
	_ = p.db.QueryRowContext(ctx, `SELECT count(*) FROM qu_jobs WHERE id = 'bad'`).Scan(&count)
	mustEqual(t, count, 1, "the row is kept, not deleted")
}

func TestPostgresQueue_PauseHoldsEveryConsumerUntilResume(t *testing.T) {
	p := openPgPlatform(t, "qs_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond})
	ctx := context.Background()
	// A second consumer on the same table, as a second pod would be.
	other, err := pgstorage.NewQueue(ctx, p.db, pgstorage.QueueOptions{Poll: 20 * time.Millisecond}, pgstorage.WithPrefix("qs_"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.Close)
	rec := &recorder{}
	handler := func(_ context.Context, job ports.RunJob) error { rec.add(job.ThreadID); return nil }
	p.queue.Bind(handler)
	other.Bind(handler)
	if err := p.queue.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "held"}, nil)
	time.Sleep(150 * time.Millisecond)
	mustEqual(t, len(rec.list()), 0, "nobody claims while paused")
	stats, _ := p.queue.Stats(ctx)
	mustEqual(t, stats.Paused, true, "the stats say so")
	mustEqual(t, stats.Ready, 1, "the row waits")
	if err := other.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	seen := rec.waitFor(t, 1, 2*time.Second)
	mustStrings(t, seen, []string{"held"}, "delivered once resumed, by either consumer")
}

func TestPostgresQueue_ClaimsSpreadAcrossPartitions(t *testing.T) {
	p := openPgPlatform(t, "qf_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond, Concurrency: 2})
	ctx := context.Background()
	rec := &recorder{}
	release := make(chan struct{})
	p.queue.Bind(func(_ context.Context, job ports.RunJob) error {
		rec.add(job.ThreadID)
		<-release
		return nil
	})
	if err := p.queue.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a1", "a2", "a3"} {
		_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: id, PartitionKey: "team-a"}, nil)
		time.Sleep(2 * time.Millisecond) // distinct dispatch times
	}
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "b1", PartitionKey: "team-b"}, nil)
	if err := p.queue.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	first := rec.waitFor(t, 2, 2*time.Second)
	if len(first) != 2 || first[0] != "a1" || first[1] != "b1" {
		t.Fatalf("the second slot goes to the partition with nothing in flight: %v", first)
	}
	close(release)
	all := rec.waitFor(t, 4, 2*time.Second)
	mustStrings(t, all, []string{"a1", "b1", "a2", "a3"}, "then the rest in order")
}

func TestPostgresQueue_PriorityGoesFirst(t *testing.T) {
	p := openPgPlatform(t, "qo_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond, Concurrency: 1})
	ctx := context.Background()
	rec := &recorder{}
	p.queue.Bind(func(_ context.Context, job ports.RunJob) error {
		rec.add(job.ThreadID)
		time.Sleep(20 * time.Millisecond)
		return nil
	})
	_ = p.queue.Pause(ctx)
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "expiry", Kind: ports.JobExpiry}, &ports.EnqueueOptions{Priority: ports.PriorityLow})
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "user"}, nil)
	_ = p.queue.Resume(ctx)
	seen := rec.waitFor(t, 2, 2*time.Second)
	mustStrings(t, seen, []string{"user", "expiry"}, "a user's message goes before housekeeping that was queued earlier")
}

func TestPostgresQueue_AnOldRowIsKeptDeadInsteadOfRun(t *testing.T) {
	p := openPgPlatform(t, "qa_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond, MaxAge: 50 * time.Millisecond})
	ctx := context.Background()
	rec := &recorder{}
	var dead []string
	var mu sync.Mutex
	p.queue.Bind(func(_ context.Context, job ports.RunJob) error { rec.add(job.ThreadID); return nil },
		pgstorage.WithDeadHandler(func(_ context.Context, job ports.RunJob, _ int, cause error) {
			mu.Lock()
			dead = append(dead, job.ThreadID+": "+cause.Error())
			mu.Unlock()
		}))
	_ = p.queue.Pause(ctx)
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "stale", RunID: "r-stale"}, nil)
	time.Sleep(80 * time.Millisecond)
	_ = p.queue.Resume(ctx)
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(dead) == 1 })
	mustEqual(t, len(rec.list()), 0, "never run")
	mu.Lock()
	if !strings.Contains(dead[0], "longer than") {
		t.Fatalf("the dead handler is told why: %v", dead)
	}
	mu.Unlock()
	rows, _ := p.queue.ListDead(ctx, 10)
	mustEqual(t, len(rows), 1, "kept dead")
}

func TestPostgresQueue_CloseDrainsARunningJob(t *testing.T) {
	p := openPgPlatform(t, "qc_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond, DrainTimeout: 3 * time.Second})
	ctx := context.Background()
	started := make(chan struct{})
	var finished, cancelled bool
	var mu sync.Mutex
	p.queue.Bind(func(ctx context.Context, _ ports.RunJob) error {
		close(started)
		select {
		case <-time.After(300 * time.Millisecond):
			mu.Lock()
			finished = true
			mu.Unlock()
			return nil
		case <-ctx.Done():
			mu.Lock()
			cancelled = true
			mu.Unlock()
			return ctx.Err()
		}
	})
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "long"}, nil)
	<-started
	p.queue.Close()
	mu.Lock()
	defer mu.Unlock()
	if !finished || cancelled {
		t.Fatalf("the running job was allowed to finish: finished=%v cancelled=%v", finished, cancelled)
	}
	pending, _ := p.queue.Pending(ctx)
	mustEqual(t, pending, 0, "and its row was deleted")
}

func TestPostgresQueue_AShutdownDoesNotSpendAnAttempt(t *testing.T) {
	p := openPgPlatform(t, "qz_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond, MaxAttempts: 1})
	ctx := context.Background()
	started := make(chan struct{})
	p.queue.Bind(func(ctx context.Context, _ ports.RunJob) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "cut", RunID: "r-cut"}, nil)
	<-started
	p.queue.Close() // no drain window: the handler is cancelled at once
	var attempts int
	var locked *time.Time
	if err := p.db.QueryRowContext(ctx, `SELECT attempts, "lockedUntil" FROM qz_jobs WHERE "runId" = 'r-cut'`).Scan(&attempts, &locked); err != nil {
		t.Fatalf("the row is still there: %v", err)
	}
	mustEqual(t, attempts, 0, "the delivery was given back, not spent")
	if locked != nil {
		t.Fatal("released for another consumer")
	}
	rows, _ := p.queue.ListDead(ctx, 10)
	mustEqual(t, len(rows), 0, "not dead, though MaxAttempts is 1")
}

func TestPostgresQueue_NamespacesDoNotShareRows(t *testing.T) {
	p := openPgPlatform(t, "qn_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond, Namespace: "prod"})
	ctx := context.Background()
	staging, err := pgstorage.NewQueue(ctx, p.db, pgstorage.QueueOptions{Poll: 20 * time.Millisecond, Namespace: "staging"}, pgstorage.WithPrefix("qn_"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(staging.Close)
	prodSeen, stagingSeen := &recorder{}, &recorder{}
	p.queue.Bind(func(_ context.Context, job ports.RunJob) error { prodSeen.add(job.ThreadID); return nil })
	staging.Bind(func(_ context.Context, job ports.RunJob) error { stagingSeen.add(job.ThreadID); return nil })
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "prod-run", RunID: "r-prod"}, nil)
	_ = staging.Enqueue(ctx, ports.RunJob{ThreadID: "staging-run", RunID: "r-staging"}, nil)
	mustStrings(t, prodSeen.waitFor(t, 1, 2*time.Second), []string{"prod-run"}, "prod takes only prod rows")
	mustStrings(t, stagingSeen.waitFor(t, 1, 2*time.Second), []string{"staging-run"}, "staging takes only staging rows")
	time.Sleep(60 * time.Millisecond)
	mustEqual(t, len(prodSeen.list()), 1, "and never the other's")
	mustEqual(t, len(stagingSeen.list()), 1, "in either direction")
	if found, _ := p.queue.Find(ctx, "r-staging"); found != nil {
		t.Fatal("a namespace cannot even see the other's rows")
	}
}

func TestPostgresQueue_AListenerWakesTheConsumerWithoutPolling(t *testing.T) {
	p := openPgPlatform(t, "ql_", pgstorage.QueueOptions{Poll: 10 * time.Second, Listener: pgxlisten.New(os.Getenv("TEST_ADMIN_PG"))})
	ctx := context.Background()
	rec := &recorder{}
	p.queue.Bind(func(_ context.Context, job ports.RunJob) error { rec.add(job.ThreadID); return nil })
	time.Sleep(200 * time.Millisecond) // the LISTEN connection comes up
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "woken"}, nil)
	seen := rec.waitFor(t, 1, 2*time.Second)
	mustStrings(t, seen, []string{"woken"}, "delivered long before the 10s poll")
}
