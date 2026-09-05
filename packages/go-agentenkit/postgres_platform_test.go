package agentenkit_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	pgstorage "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres/pgxlisten"
	memoryadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// pgPlatform is the whole operational side on one Postgres: storage, kv,
// bus and queue, prefixed so the tables never collide with another test's.
type pgPlatform struct {
	db      *sql.DB
	storage *pgstorage.Storage

	kv    *pgstorage.Kv
	bus   *pgstorage.Bus
	queue *pgstorage.Queue
}

func openPgPlatform(t *testing.T, prefix string, queueOpts pgstorage.QueueOptions) pgPlatform {
	t.Helper()
	db := openPostgres(t)
	ctx := context.Background()
	for _, tbl := range []string{"jobs", "kv", "usage", "events", "messages", "threads"} {
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+prefix+tbl)
	}
	storage, err := pgstorage.New(ctx, db, pgstorage.WithPrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	kv, err := pgstorage.NewKv(ctx, db, pgstorage.WithPrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := pgstorage.NewQueue(ctx, db, queueOpts, pgstorage.WithPrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	bus := pgstorage.NewBus(db, pgxlisten.New(os.Getenv("TEST_ADMIN_PG")), storage.Events(), kv, pgstorage.BusOptions{Heartbeat: time.Hour})
	t.Cleanup(func() {
		queue.Close()
		db.Close()
	})
	return pgPlatform{db: db, storage: storage, kv: kv, bus: bus, queue: queue}
}

func TestPostgresKv_BehavesLikeRedis(t *testing.T) {
	p := openPgPlatform(t, "kvt_", pgstorage.QueueOptions{})
	kv := p.kv
	ctx := context.Background()
	if _, ok, _ := kv.Get(ctx, "missing"); ok {
		t.Fatal("missing key must be absent")
	}
	if ok, _ := kv.Set(ctx, "a", "1", ports.SetOptions{}); !ok {
		t.Fatal("set")
	}
	v, ok, _ := kv.Get(ctx, "a")
	mustEqual(t, v, "1", "get")
	mustEqual(t, ok, true, "found")
	if ok, _ := kv.Set(ctx, "lock", "r1", ports.SetOptions{OnlyIfNotExists: true, Expiry: time.Minute}); !ok {
		t.Fatal("first NX must win")
	}
	if ok, _ := kv.Set(ctx, "lock", "r2", ports.SetOptions{OnlyIfNotExists: true}); ok {
		t.Fatal("second NX must lose")
	}
	v, _, _ = kv.Get(ctx, "lock")
	mustEqual(t, v, "r1", "lock holder")
	_, _ = kv.Set(ctx, "short", "x", ports.SetOptions{Expiry: 20 * time.Millisecond})
	time.Sleep(40 * time.Millisecond)
	if _, ok, _ := kv.Get(ctx, "short"); ok {
		t.Fatal("expired key must be absent")
	}
	if ok, _ := kv.Set(ctx, "short", "y", ports.SetOptions{OnlyIfNotExists: true}); !ok {
		t.Fatal("NX over an expired key must win")
	}
	n1, _ := kv.Incr(ctx, "seq")
	n2, _ := kv.Incr(ctx, "seq")
	mustEqual(t, n1, int64(1), "incr 1")
	mustEqual(t, n2, int64(2), "incr 2")
	_ = kv.Del(ctx, "seq")
	n3, _ := kv.Incr(ctx, "seq")
	mustEqual(t, n3, int64(1), "incr after del")
	// Concurrent increments never collide: the counter is the seq source (§3.4)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := kv.Incr(ctx, "race"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	v, _, _ = kv.Get(ctx, "race")
	mustEqual(t, v, "20", "20 increments")
	// Concurrent NX: exactly one winner
	var wins atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := kv.Set(ctx, "nx-race", "me", ports.SetOptions{OnlyIfNotExists: true, Expiry: time.Minute}); ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	mustEqual(t, wins.Load(), int32(1), "one NX winner")
	deleted, _ := kv.DeleteExpired(ctx)
	_ = deleted
}

func TestPostgresBus_DeliversLiveAndResolvesOversizedFrames(t *testing.T) {
	p := openPgPlatform(t, "bust_", pgstorage.QueueOptions{})
	ctx := context.Background()
	sc := ports.StorageContext{}
	th, err := p.storage.Threads().Create(ctx, ports.ThreadInit{Model: "m"}, sc)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan ports.AgentEvent, 16)
	unsubscribe, err := p.bus.Subscribe(ctx, th.ID, func(e ports.AgentEvent) { got <- e })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unsubscribe() })
	// The LISTEN connection comes up asynchronously: publish until it hears us.
	small := ports.AgentEvent{ThreadID: th.ID, Seq: 1, Type: "SMALL", Payload: json.RawMessage(`{"a":1}`), CreatedAt: time.Now()}
	deadline := time.Now().Add(5 * time.Second)
	var first ports.AgentEvent
	for {
		if err := p.bus.Publish(ctx, th.ID, small); err != nil {
			t.Fatal(err)
		}
		select {
		case first = <-got:
		case <-time.After(200 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatal("the listener never delivered")
			}
			continue
		}
		break
	}
	mustEqual(t, first.Type, "SMALL", "live delivery")
	mustEqual(t, first.Seq, int64(1), "seq intact")
	// The publishes made before the listener was up may have been heard too;
	// drain them so the assertions below see only what they publish.
	for drained := false; !drained; {
		select {
		case <-got:
		case <-time.After(200 * time.Millisecond):
			drained = true
		}
	}

	// A durable event past the NOTIFY cap travels as a reference and is read
	// back from the log
	big := ports.AgentEvent{ThreadID: th.ID, Seq: 2, Type: "BIG", CreatedAt: time.Now(),
		Payload: json.RawMessage(`{"blob":"` + strings.Repeat("x", 20_000) + `"}`)}
	if err := p.storage.Events().Append(ctx, th.ID, big, sc); err != nil {
		t.Fatal(err)
	}
	if err := p.bus.Publish(ctx, th.ID, big); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-got:
		mustEqual(t, e.Type, "BIG", "oversized durable event")
		mustEqual(t, e.Seq, int64(2), "seq")
		if len(e.Payload) < 20_000 {
			t.Fatalf("payload was not resolved from the log: %d bytes", len(e.Payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("oversized durable event never arrived")
	}

	// An oversized notice (seq 0) is parked in the kv and resolved from there
	notice := ports.AgentEvent{ThreadID: th.ID, Seq: 0, Type: "PROGRESS", CreatedAt: time.Now(),
		Payload: json.RawMessage(`{"blob":"` + strings.Repeat("y", 20_000) + `"}`)}
	if err := p.bus.Publish(ctx, th.ID, notice); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-got:
		mustEqual(t, e.Type, "PROGRESS", "oversized notice")
		mustEqual(t, e.Seq, int64(0), "still a notice")
		if len(e.Payload) < 20_000 {
			t.Fatalf("notice was not resolved from the kv: %d bytes", len(e.Payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("oversized notice never arrived")
	}

	// A thread nobody here watches is ignored
	other, _ := p.storage.Threads().Create(ctx, ports.ThreadInit{Model: "m"}, sc)
	_ = p.bus.Publish(ctx, other.ID, small)
	select {
	case e := <-got:
		if e.ThreadID == other.ID {
			t.Fatalf("unexpected delivery for another thread: %+v", e)
		}
	case <-time.After(300 * time.Millisecond):
	}
}

func TestPostgresQueue_DeliversHonoursDelaysAndRedelivers(t *testing.T) {
	p := openPgPlatform(t, "qt_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond, Lease: 300 * time.Millisecond, MaxAttempts: 2})
	ctx := context.Background()
	var mu sync.Mutex
	var seen []string
	fail := map[string]int{"flaky": 1}
	p.queue.Bind(func(_ context.Context, job ports.RunJob) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, job.ThreadID)
		if fail[job.ThreadID] > 0 {
			fail[job.ThreadID]--
			return errBoom
		}
		return nil
	})
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "later"}, &ports.EnqueueOptions{Delay: 400 * time.Millisecond})
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "now"}, nil)
	_ = p.queue.Enqueue(ctx, ports.RunJob{ThreadID: "flaky"}, nil)

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 4 {
		t.Fatalf("deliveries: %v", seen)
	}
	// Immediate jobs before the delayed one; the failed job came back once
	mustEqual(t, seen[len(seen)-1], "later", "the delayed job ran last")
	count := map[string]int{}
	for _, s := range seen {
		count[s]++
	}
	mustEqual(t, count["now"], 1, "a successful job runs once")
	mustEqual(t, count["flaky"], 2, "a failed job is redelivered")
	pending, _ := p.queue.Pending(ctx)
	mustEqual(t, pending, 0, "nothing left")
}

func TestPostgresPlatform_RunsARunEndToEnd(t *testing.T) {
	p := openPgPlatform(t, "e2e_", pgstorage.QueueOptions{Poll: 20 * time.Millisecond})
	model := scripted(step{calls: []call{{"c1", "probe", `{}`}}}, step{text: "done"})
	cfg := agentenkit.DefaultConfig()
	cfg.StopPoll = 5 * time.Millisecond
	rt, err := agentenkit.SetupAgentCore(context.Background(), agentenkit.RuntimeOptions{
		Storage: p.storage, Admin: memoryadmin.New(), Bus: p.bus, Kv: p.kv, Queue: p.queue,
		ResolveModel: func(string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		},
		Config: &cfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	p.queue.Bind(rt.Worker.Handler())
	chat := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) { return "ok", nil })},
	})
	res, err := chat.Run(context.Background(), agentenkit.RunInput{Prompt: "hi"})
	if err != nil || !res.Accepted {
		t.Fatalf("run: %v %+v", err, res)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		snap, _ := rt.GetThreadSnapshot(context.Background(), res.ThreadID, nil)
		if snap.Thread.State == agentenkit.StateCompleted || time.Now().After(deadline) {
			mustEqual(t, snap.Thread.State, agentenkit.StateCompleted, "completed")
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	events, _ := rt.Events.Since(context.Background(), res.ThreadID, -1, nil)
	for i := 1; i < len(events); i++ {
		if events[i].Seq <= events[i-1].Seq {
			t.Fatalf("seq not ascending at %d", i)
		}
	}
	mustEqual(t, model.Calls(), 2, "two round trips")
}

// scopedEvents is a storage that needs a tenant on every read (§2.10):
// what a listener serving every tenant cannot supply.
type scopedEvents struct{ ports.EventStore }

func (s scopedEvents) ListSince(ctx context.Context, threadID string, sinceSeq int64, sc ports.StorageContext) ([]ports.AgentEvent, error) {
	if sc.State["tenant"] == nil {
		return nil, errors.New("tenant scope required")
	}
	return s.EventStore.ListSince(ctx, threadID, sinceSeq, sc)
}

// An oversized durable event must reach a subscriber whose storage cannot
// be read without a tenant, and a NOTIFY lost while the LISTEN connection
// was down must be replayed, from the subscriber's own scope. Each event
// arrives once, in order, on its own tenant's stream only.
func TestPostgresBus_OversizedEventsOnScopedStorageAndReplayAfterReconnect(t *testing.T) {
	p := openPgPlatform(t, "busscope_", pgstorage.QueueOptions{})
	ctx := context.Background()
	listener := pgxlisten.New(os.Getenv("TEST_ADMIN_PG"))
	listener.Backoff = 50 * time.Millisecond
	dropped := make(chan struct{}, 8)
	listener.OnError = func(error) { dropped <- struct{}{} }

	bus := pgstorage.NewBus(p.db, listener, scopedEvents{p.storage.Events()}, p.kv, pgstorage.BusOptions{Heartbeat: time.Hour})

	type tenant struct {
		name   string
		thread string
		sc     ports.StorageContext
		got    chan ports.AgentEvent
	}
	var tenants []*tenant
	for _, name := range []string{"a", "b"} {
		sc := ports.StorageContext{State: ports.AgentRunState{"tenant": name}}
		th, err := p.storage.Threads().Create(ctx, ports.ThreadInit{Model: "m"}, sc)
		if err != nil {
			t.Fatal(err)
		}
		tn := &tenant{name: name, thread: th.ID, sc: sc, got: make(chan ports.AgentEvent, 64)}
		unsubscribe, err := bus.Subscribe(agentenkit.ContextWithRunState(ctx, sc.State), th.ID, func(e ports.AgentEvent) { tn.got <- e })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = unsubscribe() })
		tenants = append(tenants, tn)
	}
	// One seq counter per thread, kept like core keeps it, so the bus can
	// anchor a replay on it.
	next := map[string]int64{}
	publish := func(tn *tenant, typ string, payload string) ports.AgentEvent {
		t.Helper()
		next[tn.thread]++
		_, _ = p.kv.Incr(ctx, agentenkit.SeqKey(tn.thread))
		e := ports.AgentEvent{ThreadID: tn.thread, Seq: next[tn.thread], Type: typ, Payload: json.RawMessage(payload), CreatedAt: time.Now()}
		if err := p.storage.Events().Append(ctx, tn.thread, e, tn.sc); err != nil {
			t.Fatal(err)
		}
		if err := bus.Publish(ctx, tn.thread, e); err != nil {
			t.Fatal(err)
		}
		return e
	}
	expect := func(tn *tenant, seq int64, minBytes int) {
		t.Helper()
		select {
		case e := <-tn.got:
			if e.Seq == 0 {
				t.Fatalf("tenant %s: unexpected notice %s", tn.name, e.Type)
			}
			mustEqual(t, e.ThreadID, tn.thread, "tenant "+tn.name+" thread")
			mustEqual(t, e.Seq, seq, "tenant "+tn.name+" order")
			if len(e.Payload) < minBytes {
				t.Fatalf("tenant %s seq %d: payload %d bytes, want at least %d", tn.name, seq, len(e.Payload), minBytes)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("tenant %s: seq %d never arrived", tn.name, seq)
		}
	}
	quiet := func(tn *tenant) {
		t.Helper()
		select {
		case e := <-tn.got:
			t.Fatalf("tenant %s: unexpected extra delivery seq %d %s", tn.name, e.Seq, e.Type)
		case <-time.After(300 * time.Millisecond):
		}
	}

	// The LISTEN connection comes up asynchronously: publish until it hears
	// us, then drain the copies made while it was not.
	deadline := time.Now().Add(5 * time.Second)
	for heard := false; !heard; {
		publish(tenants[0], "WARMUP", `{}`)
		select {
		case <-tenants[0].got:
			heard = true
		case <-time.After(200 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatal("the listener never delivered")
			}
		}
	}
	for drained := false; !drained; {
		select {
		case <-tenants[0].got:
		case <-time.After(300 * time.Millisecond):
			drained = true
		}
	}

	// Small, oversized, small: on a store that cannot be read from the
	// listener, the big one still lands, in order, from the kv reference.
	big := `{"blob":"` + strings.Repeat("x", 20_000) + `"}`
	for _, tn := range tenants {
		base := next[tn.thread]
		publish(tn, "SMALL", `{"n":1}`)
		publish(tn, "BIG", big)
		publish(tn, "SMALL", `{"n":3}`)
		expect(tn, base+1, 0)
		expect(tn, base+2, 20_000)
		expect(tn, base+3, 0)
	}

	// The LISTEN connection drops. What is published while it is down is
	// never notified; once it is back, each subscriber gets it from the log,
	// read with its own tenant.
	if _, err := p.db.ExecContext(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query ILIKE 'listen %'`); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dropped: // the connection is confirmed gone; nothing hears the next NOTIFY
	case <-time.After(5 * time.Second):
		t.Fatal("the LISTEN connection never dropped")
	}
	var missed []ports.AgentEvent

	for _, tn := range tenants {
		missed = append(missed, publish(tn, "MISSED", big))
	}
	for i, tn := range tenants {
		expect(tn, missed[i].Seq, 20_000)
	}
	// Live again: the replay must not have moved anything twice, and a fresh
	// event arrives once.
	for _, tn := range tenants {
		e := publish(tn, "AFTER", `{"n":5}`)
		expect(tn, e.Seq, 0)
		quiet(tn)
	}
}
