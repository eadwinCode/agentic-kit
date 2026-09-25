package agentenkit_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/inline"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	pgstorage "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	memoryadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/memory"
	pgadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/postgres"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream I: every adapter keeps its port's promise under load and across
// processes. The cases both runtimes have run in the TS package too
// (test/adapters-hardening.test.ts), under the same names.

func TestSqlite_OpenTurnsOnWALAndABusyTimeout(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "wal.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var mode string
	var timeout int
	_ = db.QueryRow(`PRAGMA journal_mode`).Scan(&mode)
	_ = db.QueryRow(`PRAGMA busy_timeout`).Scan(&timeout)
	mustEqual(t, mode, "wal", "readers run beside the writer")
	mustEqual(t, timeout, 5000, "a writer waits for the lock rather than failing")
}

func TestSqliteStorage_AMissingThreadCannotBeDeleted(t *testing.T) {
	s := openSqlite(t)
	ctx := context.Background()
	if err := s.Threads().Delete(ctx, "nope", agentenkit.StorageContext{}); err == nil {
		t.Fatal("deleting a thread that is not there is an error")
	}
	th, _ := s.Threads().Create(ctx, ports.ThreadInit{}, agentenkit.StorageContext{})
	_, _ = s.Messages().Append(ctx, th.ID, ports.NewMessage{Role: ports.RoleUser, Content: agentenkit.TextContent("hi")}, agentenkit.StorageContext{})
	if err := s.Threads().Delete(ctx, th.ID, agentenkit.StorageContext{}); err != nil {
		t.Fatal(err)
	}
	msgs, _ := s.Messages().List(ctx, th.ID, nil, agentenkit.StorageContext{})
	mustEqual(t, len(msgs), 0, "its messages went with it")
}

func TestSqliteStorage_AMessageSeqCannotRepeat(t *testing.T) {
	db, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	th, _ := s.Threads().Create(ctx, ports.ThreadInit{}, agentenkit.StorageContext{})
	_, _ = s.Messages().Append(ctx, th.ID, ports.NewMessage{Role: ports.RoleUser, Content: agentenkit.TextContent("one")}, agentenkit.StorageContext{})
	_, err = db.Exec(`INSERT INTO messages (id,threadId,role,content,createdAt,seq) VALUES ('dup',?,'user','"two"',0,1)`, th.ID)
	if err == nil {
		t.Fatal("a second message under a seq the thread already has is refused")
	}
}

// adminStores are the three admin stores, fresh; Postgres when TEST_ADMIN_PG
// is set.
func adminStores(t *testing.T) map[string]func(t *testing.T) ports.AdminStore {
	return map[string]func(t *testing.T) ports.AdminStore{
		"memory": func(*testing.T) ports.AdminStore { return memoryadmin.New() },
		"sqlite": func(t *testing.T) ports.AdminStore {
			db, err := sqlite.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			store, err := sqliteadmin.New(db)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { store.Close() })
			return store
		},
		"postgres": func(t *testing.T) ports.AdminStore {
			db := openPostgres(t)
			for _, tbl := range []string{"agentic_steps", "agentic_runs", "agentic_threads", "agentic_migrations"} {
				_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+tbl)
			}
			store, err := pgadmin.Connect(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { store.Close() })
			return store
		},
	}
}

// Two segments that close together both count: the counters are added in
// the store, not read and written back.
func TestAdminStore_IncrementAddsInOneWrite(t *testing.T) {
	for name, open := range adminStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			runs := open(t).Runs()
			if _, err := runs.Start(ctx, ports.NewRunRecord{ID: "inc", ThreadID: "t-inc", Agent: "chat", Model: "m"}); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = runs.Increment(ctx, "inc", ports.RunDeltas{Steps: 1, TotalTokens: 10})
				}()
			}
			wg.Wait()
			rec, _ := runs.Get(ctx, "inc")
			mustEqual(t, rec.Steps, 20, "every step counted")
			mustEqual(t, rec.TotalTokens, 200, "every token counted")
		})
	}
}

// Stats count every run in the window, however many there are; only the
// percentiles are worked out over a sample, and they say so.
func TestAdminStore_StatsCountEveryRunPastTheSample(t *testing.T) {
	for name, open := range adminStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := open(t)
			for i := range 5 {
				id := fmt.Sprintf("stat-%d", i)
				_, _ = store.Runs().Start(ctx, ports.NewRunRecord{ID: id, ThreadID: "t-stat", Agent: "chat", Model: "m"})
				_ = store.Runs().Increment(ctx, id, ports.RunDeltas{TotalTokens: 10})
			}
			stats, err := core.RunStatsFor(ctx, ports.RuntimePorts{Admin: store}, core.StatsRange{Limit: 2})
			if err != nil {
				t.Fatal(err)
			}
			mustEqual(t, stats.Total, 5, "every run counted")
			mustEqual(t, stats.Tokens.TotalTokens, 50, "every run's tokens summed")
			mustEqual(t, stats.Sampled, true, "the percentiles say they are a sample")
		})
	}
}

// One job per key, withdrawn by key, found by run and counted.
func TestQueue_AKeyedJobIsQueuedOnce(t *testing.T) {
	queues := map[string]ports.Queue{"memory": memory.NewQueue(), "inline": inline.New(context.Background())}
	for name, q := range queues {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			job := ports.RunJob{ThreadID: "t1", RunID: "r1", Kind: ports.JobExpiry}
			if err := q.Enqueue(ctx, job, &ports.EnqueueOptions{Key: "hitl-expiry:c1", Delay: time.Hour}); err != nil {
				t.Fatal(err)
			}
			err := q.Enqueue(ctx, job, &ports.EnqueueOptions{Key: "hitl-expiry:c1", Delay: time.Hour})
			if !errors.Is(err, ports.ErrDuplicateJob) {
				t.Fatalf("a second job under the key is refused: %v", err)
			}
			found, _ := q.Find(ctx, "r1")
			if found == nil || found.ThreadID != "t1" {
				t.Fatalf("the run's job is found: %+v", found)
			}
			stats, _ := q.Stats(ctx)
			mustEqual(t, stats.Delayed, 1, "counted as delayed")
			_ = q.Cancel(ctx, "hitl-expiry:c1")
			found, _ = q.Find(ctx, "r1")
			if found != nil {
				t.Fatal("cancel withdrew it")
			}
		})
	}
}

func TestInlineQueue_AJobDueBeforeBindIsHeldNotDropped(t *testing.T) {
	q := inline.New(context.Background())
	_ = q.Enqueue(context.Background(), ports.RunJob{ThreadID: "early"}, nil)
	time.Sleep(20 * time.Millisecond) // due, and nobody bound yet
	got := make(chan string, 1)
	q.Bind(func(_ context.Context, job ports.RunJob) error { got <- job.ThreadID; return nil })
	select {
	case id := <-got:
		mustEqual(t, id, "early", "delivered once bound")
	case <-time.After(time.Second):
		t.Fatal("the job was dropped")
	}
}

func TestInlineQueue_ALongDelayDoesNotFireAtOnce(t *testing.T) {
	q := inline.New(context.Background())
	defer q.Clear()
	var ran atomic.Bool
	q.Bind(func(context.Context, ports.RunJob) error { ran.Store(true); return nil })
	_ = q.Enqueue(context.Background(), ports.RunJob{ThreadID: "later"}, &ports.EnqueueOptions{Delay: 30 * 24 * time.Hour})
	time.Sleep(30 * time.Millisecond)
	mustEqual(t, ran.Load(), false, "a 30-day delay waits")
	stats, _ := q.Stats(context.Background())
	mustEqual(t, stats.Delayed, 1, "and is counted as delayed")
}

// An answer withdraws the park's own expiry row, and a second answer's
// resume is refused by the queue.
func TestHitl_AnAnswerWithdrawsTheParksExpiryRow(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	if !slicesContain(h.queue.Keys(), "hitl-expiry:c1") {
		t.Fatalf("the park queued its expiry under its key: %v", h.queue.Keys())
	}
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true}); err != nil {
		t.Fatal(err)
	}
	keys := h.queue.Keys()
	if slicesContain(keys, "hitl-expiry:c1") || !slicesContain(keys, "hitl-resume:c1") {
		t.Fatalf("the answer queued its resume and withdrew the expiry: %v", keys)
	}
}

func slicesContain(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func TestMemoryBus_KeepsOnlyTheLatestEvents(t *testing.T) {
	bus := memory.NewBus()
	for i := range memory.PublishedKept + 5 {
		_ = bus.Publish(context.Background(), "t", ports.AgentEvent{ThreadID: "t", Seq: int64(i + 1)})
	}
	got := bus.Published()
	mustEqual(t, len(got), memory.PublishedKept, "bounded")
	mustEqual(t, got[0].Seq, int64(6), "the oldest are the ones dropped")
}

// Go only: the TS package has no Postgres storage or queue.

func TestPostgresStorage_AppendsThatRaceNeverShareASeq(t *testing.T) {
	p := openPgPlatform(t, "ms_", pgstorage.QueueOptions{})
	ctx := context.Background()
	th, _ := p.storage.Threads().Create(ctx, ports.ThreadInit{}, agentenkit.StorageContext{})
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.storage.Messages().Append(ctx, th.ID, ports.NewMessage{Role: ports.RoleUser, Content: agentenkit.TextContent(fmt.Sprint(i))}, agentenkit.StorageContext{})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("an append that raced another is retried, not failed: %v", err)
		}
	}
	var n, distinct int
	_ = p.db.QueryRow(`SELECT count(*), count(DISTINCT seq) FROM ms_messages WHERE "threadId" = $1`, th.ID).Scan(&n, &distinct)
	mustEqual(t, n, 20, "every message landed")
	mustEqual(t, distinct, 20, "each under its own seq")
}

func TestPostgresQueue_AnUnreadablePayloadReachesTheDeadHandler(t *testing.T) {
	p := openPgPlatform(t, "qu_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond})
	ctx := context.Background()
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "t-bad", RunID: "r-bad"}, nil)
	// Valid JSON, but not a job: a string where the object should be.
	if _, err := p.db.ExecContext(ctx, `UPDATE qu_jobs SET payload = '"not a job"'`); err != nil {
		t.Fatal(err)
	}
	dead := make(chan ports.RunJob, 1)
	p.queue.Bind(func(context.Context, ports.RunJob) error { return nil },
		pgstorage.WithDeadHandler(func(_ context.Context, job ports.RunJob, _ int, _ error) { dead <- job }))
	select {
	case job := <-dead:
		mustEqual(t, job.RunID, "r-bad", "the handler learns the run from the row itself")
		mustEqual(t, job.ThreadID, "t-bad", "and the thread")
	case <-time.After(3 * time.Second):
		t.Fatal("the dead handler never heard of the unreadable job")
	}
}

func TestPostgresQueue_CancelAndPurgeKeepDeadRows(t *testing.T) {
	p := openPgPlatform(t, "qd_", pgstorage.QueueOptions{})
	ctx := context.Background()
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "t-dead", RunID: "r-dead"}, &ports.EnqueueOptions{Key: "k-dead"})
	if _, err := p.db.ExecContext(ctx, `UPDATE qd_jobs SET "deadAt" = now()`); err != nil {
		t.Fatal(err)
	}
	_ = p.queue.Cancel(ctx, "k-dead")
	if _, err := p.queue.Purge(ctx, pgstorage.PurgeFilter{ThreadID: "t-dead"}); err != nil {
		t.Fatal(err)
	}
	rows, _ := p.queue.ListDead(ctx, 10)
	mustEqual(t, len(rows), 1, "kept for its operator")
	n, _ := p.queue.Purge(ctx, pgstorage.PurgeFilter{ThreadID: "t-dead", IncludeDead: true})
	mustEqual(t, n, int64(1), "unless the purge asks for it")
}

// The storage, kv and queue tables are set up once: a second start reads
// the ledger and runs no DDL.
func TestPostgresSchema_ASecondStartChangesNothing(t *testing.T) {
	p := openPgPlatform(t, "sc_", pgstorage.QueueOptions{})
	ctx := context.Background()
	var before int
	_ = p.db.QueryRow(`SELECT count(*) FROM sc_migrations`).Scan(&before)
	if before == 0 {
		t.Fatal("the steps are recorded in the prefix's ledger")
	}
	if _, err := pgstorage.New(ctx, p.db, pgstorage.WithPrefix("sc_")); err != nil {
		t.Fatal(err)
	}
	if _, err := pgstorage.NewQueue(ctx, p.db, pgstorage.QueueOptions{}, pgstorage.WithPrefix("sc_")); err != nil {
		t.Fatal(err)
	}
	var after int
	_ = p.db.QueryRow(`SELECT count(*) FROM sc_migrations`).Scan(&after)
	mustEqual(t, after, before, "nothing new to run")
	var names string
	_ = p.db.QueryRow(`SELECT string_agg(version, ',' ORDER BY version) FROM sc_migrations`).Scan(&names)
	for _, want := range []string{"storage_0001_init", "kv_0001_init", "queue_0002_head_indexes"} {
		if !strings.Contains(names, want) {
			t.Fatalf("%s is in the ledger: %s", want, names)
		}
	}
}
