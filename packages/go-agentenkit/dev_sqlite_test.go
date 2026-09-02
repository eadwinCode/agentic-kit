package agentenkit_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"
	_ "modernc.org/sqlite"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/inline"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

func openSqlite(t *testing.T) *sqlite.Storage {
	t.Helper()
	db, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	s, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

var sc = ports.StorageContext{}

func TestSqliteStorage_OrdersMessagesByInsertionNotByClock(t *testing.T) {
	s := openSqlite(t)
	ctx := context.Background()
	th, _ := s.Threads().Create(ctx, ports.ThreadInit{}, sc)
	for _, txt := range []string{"a", "b", "c", "d"} {
		if _, err := s.Messages().Append(ctx, th.ID, ports.NewMessage{Role: ports.RoleUser, Content: agentenkit.TextContent(txt)}, sc); err != nil {
			t.Fatal(err)
		}
	}
	rows, _ := s.Messages().List(ctx, th.ID, nil, sc)
	var got []string
	for _, r := range rows {
		got = append(got, string(r.Content))
	}
	mustStrings(t, got, []string{`"a"`, `"b"`, `"c"`, `"d"`}, "order")
}

func TestSqliteStorage_ScopesMessagesByAgent(t *testing.T) {
	s := openSqlite(t)
	ctx := context.Background()
	th, _ := s.Threads().Create(ctx, ports.ThreadInit{}, sc)
	_, _ = s.Messages().Append(ctx, th.ID, ports.NewMessage{Role: ports.RoleUser, Content: agentenkit.TextContent("main")}, sc)
	_, _ = s.Messages().Append(ctx, th.ID, ports.NewMessage{Role: ports.RoleUser, AgentID: "sub_1", Content: agentenkit.TextContent("child")}, sc)
	all, _ := s.Messages().List(ctx, th.ID, nil, sc)
	main, _ := s.Messages().List(ctx, th.ID, ports.MainAgent, sc)
	child, _ := s.Messages().List(ctx, th.ID, ports.AgentScope("sub_1"), sc)
	mustEqual(t, len(all), 2, "all")
	mustEqual(t, len(main), 1, "main")
	mustEqual(t, main[0].AgentID, "", "main agent id")
	mustEqual(t, len(child), 1, "child")
	mustEqual(t, child[0].AgentID, "sub_1", "child agent id")
}

func TestSqliteStorage_DeleteFromAndClaimStateAndCascade(t *testing.T) {
	s := openSqlite(t)
	ctx := context.Background()
	th, _ := s.Threads().Create(ctx, ports.ThreadInit{Model: "m"}, sc)
	var ids []string
	for _, txt := range []string{"a", "b", "c"} {
		m, _ := s.Messages().Append(ctx, th.ID, ports.NewMessage{Role: ports.RoleUser, Content: agentenkit.TextContent(txt)}, sc)
		ids = append(ids, m.ID)
	}
	n, _ := s.Messages().DeleteFrom(ctx, th.ID, ids[1], sc)
	mustEqual(t, n, 2, "deleted")
	n, _ = s.Messages().DeleteFrom(ctx, th.ID, "nope", sc)
	mustEqual(t, n, 0, "unknown")

	ok, _ := s.Threads().ClaimState(ctx, th.ID, ports.StateIdle, ports.StateRunning, sc)
	if !ok {
		t.Fatal("first claim must win")
	}
	ok, _ = s.Threads().ClaimState(ctx, th.ID, ports.StateIdle, ports.StateRunning, sc)
	if ok {
		t.Fatal("second claim must lose")
	}
	got, _ := s.Threads().Get(ctx, th.ID, sc)
	mustEqual(t, got.State, ports.StateRunning, "state")
	mustEqual(t, got.Model, "m", "model")

	_ = s.Events().Append(ctx, th.ID, ports.AgentEvent{ThreadID: th.ID, Seq: 1, Type: "X", Payload: []byte(`{"a":1}`), CreatedAt: time.Now()}, sc)
	_ = s.Events().Append(ctx, th.ID, ports.AgentEvent{ThreadID: th.ID, Seq: 2, Type: "Y", CreatedAt: time.Now()}, sc)
	_ = s.Usage().Record(ctx, th.ID, ports.NewUsage{InputTokens: 1, OutputTokens: 2, TotalTokens: 3}, sc)
	latest, _ := s.Events().Latest(ctx, th.ID, "X", sc)
	mustEqual(t, string(latest.Payload), `{"a":1}`, "payload")
	since, _ := s.Events().ListSince(ctx, th.ID, 1, sc)
	mustEqual(t, len(since), 1, "since")
	mustEqual(t, string(since[0].Payload), "null", "empty payload reads as null")
	byType, _ := s.Events().ListByType(ctx, th.ID, "Y", sc)
	mustEqual(t, len(byType), 1, "by type")
	total, _ := s.Usage().Total(ctx, th.ID, sc)
	mustEqual(t, total.TotalTokens, 3, "usage")
	list, _ := s.Threads().List(ctx, sc)
	mustEqual(t, len(list), 1, "list")

	if err := s.Threads().Delete(ctx, th.ID, sc); err != nil {
		t.Fatal(err)
	}
	gone, _ := s.Threads().Get(ctx, th.ID, sc)
	if gone != nil {
		t.Fatal("thread still there")
	}
	rows, _ := s.Messages().List(ctx, th.ID, nil, sc)
	mustEqual(t, len(rows), 0, "messages gone")
	evs, _ := s.Events().ListSince(ctx, th.ID, -1, sc)
	mustEqual(t, len(evs), 0, "events gone")
	total, _ = s.Usage().Total(ctx, th.ID, sc)
	mustEqual(t, total.TotalTokens, 0, "usage gone")
	if err := s.Threads().Delete(ctx, th.ID, sc); err == nil {
		t.Fatal("deleting twice must fail")
	}
}

func TestSqliteAdminStore_RoundTripsARunAndItsSteps(t *testing.T) {
	db, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqliteadmin.New(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()
	adminStoreRoundTrip(t, ctx, store)
}

// adminStoreRoundTrip is shared with the Postgres test.
func adminStoreRoundTrip(t *testing.T, ctx context.Context, store ports.AdminStore) {
	t.Helper()
	run, err := store.Runs().Start(ctx, ports.NewRunRecord{
		ID: "r1", ThreadID: "t1", Agent: "chat", Model: "gpt-4o", Prompt: "hi",
		TokenBudget: ports.Ptr(100), RunState: ports.AgentRunState{"orgId": "acme"},
		ProviderOptions: ports.ProviderOptions{"openai": map[string]any{"tier": "flex"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, run.State, ports.StateRunning, "started")
	mustEqual(t, run.Prompt, "hi", "prompt")
	mustEqual(t, *run.TokenBudget, 100, "budget")
	mustEqual(t, run.RunState["orgId"], "acme", "state")
	mustEqual(t, run.ProviderOptions["openai"].(map[string]any)["tier"], "flex", "provider options")
	child, _ := store.Runs().Start(ctx, ports.NewRunRecord{ID: "r2", ThreadID: "t1", Agent: "researcher", Model: "gpt-4o", Depth: 1, ParentRunID: "r1"})
	mustEqual(t, child.ParentRunID, "r1", "parent")
	mustEqual(t, child.Depth, 1, "depth")

	ended := time.Now()
	completed := ports.StateCompleted
	if err := store.Runs().Patch(ctx, "r1", ports.RunPatch{
		State: &completed, StopReason: ports.Ptr("completed"), EndedAt: &ended, DurationMs: ports.Ptr(int64(1200)),
		QueuedMs: ports.Ptr(int64(30)), Steps: ports.Ptr(2), TotalTokens: ports.Ptr(30), InputTokens: ports.Ptr(20),
		Result: []byte(`{"text":"ok"}`),
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := store.Runs().Get(ctx, "r1")
	mustEqual(t, got.State, ports.StateCompleted, "patched state")
	mustEqual(t, got.StopReason, "completed", "stop reason")
	mustEqual(t, *got.DurationMs, int64(1200), "duration")
	mustEqual(t, *got.QueuedMs, int64(30), "queued")
	mustEqual(t, got.Steps, 2, "steps")
	mustEqual(t, got.TotalTokens, 30, "tokens")
	var result map[string]any
	_ = json.Unmarshal(got.Result, &result)
	mustEqual(t, result["text"], "ok", "result")
	if got.EndedAt == nil {
		t.Fatal("endedAt")
	}
	none, _ := store.Runs().Get(ctx, "nope")
	if none != nil {
		t.Fatal("unknown run must be nil")
	}

	byThread, _ := store.Runs().ListByThread(ctx, "t1")
	mustEqual(t, len(byThread), 2, "by thread")
	filtered, _ := store.Runs().List(ctx, ports.RunFilter{State: []ports.ExecutionState{ports.StateCompleted}, Agent: "chat"})
	mustEqual(t, len(filtered), 1, "filtered")
	since := time.Now().Add(time.Hour)
	later, _ := store.Runs().List(ctx, ports.RunFilter{Since: &since})
	mustEqual(t, len(later), 0, "since")
	counts, _ := store.Runs().CountByState(ctx)
	mustEqual(t, counts[ports.StateCompleted], 1, "count completed")
	mustEqual(t, counts[ports.StateRunning], 1, "count running")

	at := time.Now().Truncate(time.Millisecond)
	_ = store.Threads().Upsert(ctx, ports.NewAdminThread{ID: "t1", State: ports.StateRunning, Model: "gpt-4o",
		StartedWith: &ports.ThreadStart{RunID: "r1", Agent: "chat", Model: "gpt-4o", At: at, Prompt: "hi",
			ProviderOptions: ports.ProviderOptions{"openai": map[string]any{"tier": "flex"}}}})
	// The transition upserts that follow must not clear what started it
	_ = store.Threads().Upsert(ctx, ports.NewAdminThread{ID: "t1", State: ports.StateCompleted, Model: "gpt-4o"})
	threads, _ := store.Threads().List(ctx, ports.AdminThreadFilter{})
	mustEqual(t, len(threads), 1, "upsert, not duplicate")
	mustEqual(t, threads[0].State, ports.StateCompleted, "latest state")
	if threads[0].StartedWith == nil {
		t.Fatal("startedWith lost on upsert")
	}
	mustEqual(t, threads[0].StartedWith.RunID, "r1", "startedWith runId")
	mustEqual(t, threads[0].StartedWith.Prompt, "hi", "startedWith prompt")
	mustEqual(t, threads[0].StartedWith.At.UnixMilli(), at.UnixMilli(), "startedWith at")
	mustEqual(t, threads[0].StartedWith.ProviderOptions["openai"].(map[string]any)["tier"], "flex", "startedWith provider options")
	tc, _ := store.Threads().CountByState(ctx)
	mustEqual(t, tc[ports.StateCompleted], 1, "thread count")

	for i := 1; i <= 2; i++ {
		if err := store.Steps().Record(ctx, ports.NewStepRecord{
			RunID: "r1", ThreadID: "t1", Index: i, DurationMs: 10, FinishReason: "stop", TotalTokens: 15,
			Tools: []string{"probe"}, Text: "t", ToolCalls: []ports.StepToolCall{{ToolName: "probe", Args: []byte(`{}`), Result: []byte(`"ok"`)}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	steps, _ := store.Steps().ListByRun(ctx, "r1")
	mustEqual(t, len(steps), 2, "steps")
	mustEqual(t, steps[1].Index, 2, "ordered")
	mustStrings(t, steps[0].Tools, []string{"probe"}, "tools")
	mustEqual(t, steps[0].ToolCalls[0].ToolName, "probe", "tool calls")
	mustEqual(t, steps[0].Text, "t", "text")
	byThreadSteps, _ := store.Steps().ListByThread(ctx, "t1")
	mustEqual(t, len(byThreadSteps), 2, "thread timeline")
}

// A locally assembled runtime: SQLite for both stores, memory bus and kv,
// and the inline queue actually dispatching. Nothing to stand up first.
func TestLocalRuntime_RunsEndToEndAndHonoursDelayedDispatch(t *testing.T) {
	file := filepath.Join(t.TempDir(), "agentic-kit.sqlite")
	db, err := sqlite.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	adminStore, err := sqliteadmin.New(db)
	if err != nil {
		t.Fatal(err)
	}
	model := scripted(step{calls: []call{{"c1", "danger", `{}`}}}, step{text: "all done"})
	queue := inline.New(context.Background())
	t.Cleanup(queue.Clear)
	cfg := agentenkit.DefaultConfig()
	cfg.StopPoll = 5 * time.Millisecond
	cfg.HITLTTL = 20 * time.Millisecond
	cfg.ReclaimGrace = 0
	rt, err := agentenkit.SetupAgentCore(context.Background(), agentenkit.RuntimeOptions{
		Storage: storage, Admin: adminStore, Bus: memory.NewBus(), Kv: memory.NewKv(), Queue: queue,
		ResolveModel: func(string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		},
		Config: &cfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rt.Close() })
	queue.Bind(rt.Worker.Handler())
	ran := 0
	chat := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{agentenkit.MarkRequiresConfirmation(tool("danger", func(context.Context, map[string]any) (string, error) {
			ran++
			return "ok", nil
		}))},
	})
	res, err := chat.Run(context.Background(), agentenkit.RunInput{Prompt: "hi"})
	if err != nil || !res.Accepted {
		t.Fatalf("run: %v %+v", err, res)
	}
	// The dispatch happens on a later goroutine; the park's expiry job lands
	// after the TTL on its own, with nobody watching.
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := rt.GetThreadSnapshot(context.Background(), res.ThreadID, nil)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Thread.State == agentenkit.StateCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("thread never completed, state %s", snap.Thread.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
	mustEqual(t, ran, 0, "the expired approval never ran the tool")
	rec, _ := adminStore.Runs().Get(context.Background(), res.RunID)
	mustEqual(t, rec.State, agentenkit.StateCompleted, "run record")
	mustEqual(t, rec.Steps, 2, "steps over both segments")
	snap, _ := rt.GetThreadSnapshot(context.Background(), res.ThreadID, nil)
	mustEqual(t, len(snap.Messages), 4, "user, call, timeout denial, answer")
}

func TestSqliteKv_BehavesLikeRedis(t *testing.T) {
	db, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	kv, err := sqlite.NewKv(db)
	if err != nil {
		t.Fatal(err)
	}
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
	// SET NX: the first caller wins, the second loses
	if ok, _ := kv.Set(ctx, "lock", "r1", ports.SetOptions{OnlyIfNotExists: true, Expiry: time.Minute}); !ok {
		t.Fatal("first NX must win")
	}
	if ok, _ := kv.Set(ctx, "lock", "r2", ports.SetOptions{OnlyIfNotExists: true}); ok {
		t.Fatal("second NX must lose")
	}
	v, _, _ = kv.Get(ctx, "lock")
	mustEqual(t, v, "r1", "lock holder")
	// An expired key counts as absent, for Get and for NX alike
	_, _ = kv.Set(ctx, "short", "x", ports.SetOptions{Expiry: 10 * time.Millisecond})
	time.Sleep(20 * time.Millisecond)
	if _, ok, _ := kv.Get(ctx, "short"); ok {
		t.Fatal("expired key must be absent")
	}
	if ok, _ := kv.Set(ctx, "short", "y", ports.SetOptions{OnlyIfNotExists: true}); !ok {
		t.Fatal("NX over an expired key must win")
	}
	// Incr is monotonic and survives a reopen of the same database
	n1, _ := kv.Incr(ctx, "seq")
	n2, _ := kv.Incr(ctx, "seq")
	mustEqual(t, n1, int64(1), "incr 1")
	mustEqual(t, n2, int64(2), "incr 2")
	kv2, _ := sqlite.NewKv(db)
	n3, _ := kv2.Incr(ctx, "seq")
	mustEqual(t, n3, int64(3), "incr after reopen")
	_ = kv.Del(ctx, "seq")
	n4, _ := kv.Incr(ctx, "seq")
	mustEqual(t, n4, int64(1), "incr after del")
}

func TestSqliteKv_RunsThePlatform(t *testing.T) {
	db, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	kv, _ := sqlite.NewKv(db)
	storage, _ := sqlite.New(db)
	adminStore, _ := sqliteadmin.New(db)
	model := scripted(step{calls: []call{{"c1", "probe", `{}`}}}, step{text: "done"})
	queue := memory.NewQueue()
	cfg := agentenkit.DefaultConfig()
	cfg.StopPoll = 5 * time.Millisecond
	rt, err := agentenkit.SetupAgentCore(context.Background(), agentenkit.RuntimeOptions{
		Storage: storage, Admin: adminStore, Bus: memory.NewBus(), Kv: kv, Queue: queue,
		ResolveModel: func(string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		},
		Config: &cfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	chat := rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) { return "ok", nil })},
	})
	res, _ := chat.Run(context.Background(), agentenkit.RunInput{Prompt: "hi"})
	if _, err := queue.Drain(context.Background(), rt.Worker.Handler()); err != nil {
		t.Fatal(err)
	}
	snap, _ := rt.GetThreadSnapshot(context.Background(), res.ThreadID, nil)
	mustEqual(t, snap.Thread.State, agentenkit.StateCompleted, "completed")
	// Sequence numbers are unique and ascending across the whole log
	events, _ := rt.Events.Since(context.Background(), res.ThreadID, -1, nil)
	for i := 1; i < len(events); i++ {
		if events[i].Seq <= events[i-1].Seq {
			t.Fatalf("seq not ascending at %d: %d after %d", i, events[i].Seq, events[i-1].Seq)
		}
	}
}
