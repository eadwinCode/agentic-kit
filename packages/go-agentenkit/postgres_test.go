package agentenkit_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	pgstorage "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres"
	pgadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/postgres"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// openPostgres connects to TEST_ADMIN_PG, the same variable the TypeScript
// suite and CI use. Skipped when unset.
func openPostgres(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("TEST_ADMIN_PG")
	if url == "" {
		t.Skip("TEST_ADMIN_PG not set")
	}
	db, err := pgadmin.Open(url)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("postgres unreachable at TEST_ADMIN_PG: %v", err)
	}
	return db
}

func TestPostgresAdminStore_RoundTripsARun(t *testing.T) {
	db := openPostgres(t)
	ctx := context.Background()
	for _, tbl := range []string{"agentic_steps", "agentic_runs", "agentic_threads", "agentic_migrations"} {
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl)
	}
	store, err := pgadmin.Connect(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	// Safe to connect twice
	if _, err := pgadmin.Connect(ctx, db); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	adminStoreRoundTrip(t, ctx, store)
}

func TestPostgresStorage_BehavesLikeTheOthers(t *testing.T) {
	db := openPostgres(t)
	ctx := context.Background()
	for _, tbl := range []string{"t_usage", "t_events", "t_messages", "t_threads"} {
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl)
	}
	s, err := pgstorage.New(ctx, db, pgstorage.WithPrefix("t_"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	th, err := s.Threads().Create(ctx, ports.ThreadInit{Model: "m"}, sc)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i, txt := range []string{"a", "b", "c"} {
		agent := ""
		if i == 2 {
			agent = "sub_1"
		}
		m, err := s.Messages().Append(ctx, th.ID, ports.NewMessage{Role: ports.RoleUser, AgentID: agent, Content: agentenkit.TextContent(txt)}, sc)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, m.ID)
	}
	all, _ := s.Messages().List(ctx, th.ID, nil, sc)
	mustEqual(t, len(all), 3, "all")
	mustEqual(t, string(all[0].Content), `"a"`, "order")
	main, _ := s.Messages().List(ctx, th.ID, ports.MainAgent, sc)
	mustEqual(t, len(main), 2, "main")
	child, _ := s.Messages().List(ctx, th.ID, ports.AgentScope("sub_1"), sc)
	mustEqual(t, len(child), 1, "child")
	n, _ := s.Messages().DeleteFrom(ctx, th.ID, ids[1], sc)
	mustEqual(t, n, 2, "deleteFrom")
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
	_ = s.Threads().SetState(ctx, th.ID, ports.StateCompleted, sc)
	got, _ := s.Threads().Get(ctx, th.ID, sc)
	mustEqual(t, got.State, ports.StateCompleted, "state")

	_ = s.Events().Append(ctx, th.ID, ports.AgentEvent{ThreadID: th.ID, Seq: 1, Type: "X", Payload: []byte(`{"a":1}`), CreatedAt: got.UpdatedAt}, sc)
	_ = s.Events().Append(ctx, th.ID, ports.AgentEvent{ThreadID: th.ID, Seq: 2, Type: "X", CreatedAt: got.UpdatedAt}, sc)
	latest, _ := s.Events().Latest(ctx, th.ID, "X", sc)
	mustEqual(t, latest.Seq, int64(2), "latest")
	since, _ := s.Events().ListSince(ctx, th.ID, 1, sc)
	mustEqual(t, len(since), 1, "since")
	byType, _ := s.Events().ListByType(ctx, th.ID, "X", sc)
	mustEqual(t, len(byType), 2, "by type")
	_ = s.Usage().Record(ctx, th.ID, ports.NewUsage{AgentID: "chat", InputTokens: 1, CachedInputTokens: 1, OutputTokens: 1, TotalTokens: 3}, sc)
	total, _ := s.Usage().Total(ctx, th.ID, sc)
	mustEqual(t, total.TotalTokens, 3, "usage")
	list, _ := s.Threads().List(ctx, sc)
	mustEqual(t, len(list), 1, "list")

	if err := s.Threads().Delete(ctx, th.ID, sc); err != nil {
		t.Fatal(err)
	}
	rows, _ := s.Messages().List(ctx, th.ID, nil, sc)
	mustEqual(t, len(rows), 0, "cascade")
	total, _ = s.Usage().Total(ctx, th.ID, sc)
	mustEqual(t, total.TotalTokens, 0, "usage cascade")
	if err := s.Threads().Delete(ctx, th.ID, sc); err == nil {
		t.Fatal("deleting twice must fail")
	}
}
