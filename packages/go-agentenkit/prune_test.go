package agentenkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	pgstorage "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	memoryadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// PruneEvents: the stream-only rows older releases left in the event table
// go, in batches; an app's own types and the record stay. The TS package
// runs the same cases under the same names (test/prune.test.ts).
func TestPruneEvents(t *testing.T) {
	stores := []struct {
		name string
		open func(t *testing.T) ports.Storage
	}{
		{"memory", func(*testing.T) ports.Storage { return memory.NewStorage() }},
		{"sqlite", func(t *testing.T) ports.Storage {
			db, err := sqlite.Open(filepath.Join(t.TempDir(), "prune.sqlite"))
			must(t, err)
			t.Cleanup(func() { db.Close() })
			s, err := sqlite.New(db)
			must(t, err)
			return s
		}},
	}
	if os.Getenv("TEST_ADMIN_PG") != "" {
		stores = append(stores, struct {
			name string
			open func(t *testing.T) ports.Storage
		}{"postgres", func(t *testing.T) ports.Storage {
			db := openPostgres(t)
			t.Cleanup(func() { db.Close() })
			_, _ = db.Exec("DROP TABLE IF EXISTS pr_events, pr_messages, pr_usage, pr_threads, pr_migrations CASCADE")
			s, err := pgstorage.New(context.Background(), db, pgstorage.WithPrefix("pr_"))
			must(t, err)
			return s
		}})
	}
	for _, st := range stores {
		t.Run("old stream-only rows go in batches; the record and an app's types stay ("+st.name+")", func(t *testing.T) {
			storage := st.open(t)
			ctx, sc := context.Background(), ports.StorageContext{}
			rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
				Storage: storage, Admin: memoryadmin.New(), Bus: memory.NewBus(), Queue: memory.NewQueue(), Kv: memory.NewKv(),
				Streams:      memory.NewRunStreams(),
				ResolveModel: func(string) (agentenkit.ResolvedModel, error) { return agentenkit.ResolvedModel{}, nil },
			})
			must(t, err)
			var threads []string
			for range 2 {
				th, err := storage.Threads().Create(ctx, ports.ThreadInit{}, sc)
				must(t, err)
				threads = append(threads, th.ID)
				// What an older release wrote: chunks and state changes beside
				// the record and an app's own event.
				for _, typ := range []string{"CHUNK", "CHUNK", "STATE_CHANGE", "INPUT_REQUIRED", "INVOICE_CREATED", "STEP_COMMITTED"} {
					_, err := storage.Events().Append(ctx, th.ID, ports.NewThreadEvent{Type: typ, Payload: json.RawMessage(`{}`)}, sc)
					must(t, err)
				}
			}

			dry, err := rt.PruneEvents(ctx, agentenkit.PruneOptions{DryRun: true})
			must(t, err)
			mustEqual(t, dry.ByType["CHUNK"], int64(4), "chunks found")
			mustEqual(t, dry.ByType["STATE_CHANGE"]+dry.ByType["STEP_COMMITTED"], int64(4), "the rest found")
			mustEqual(t, dry.Total, int64(8), "total")
			left, _ := storage.Events().ListSince(ctx, threads[0], -1, sc)
			mustEqual(t, len(left), 6, "nothing went on a dry run")

			done, err := rt.PruneEvents(ctx, agentenkit.PruneOptions{BatchSize: 3})
			must(t, err)
			mustEqual(t, done.Total, int64(8), "all of them went")
			mustEqual(t, done.Batches > 2, true, "a batch at a time")
			for _, th := range threads {
				left, _ := storage.Events().ListSince(ctx, th, -1, sc)
				var types []string
				for _, e := range left {
					types = append(types, e.Type)
				}
				mustStrings(t, types, []string{"INPUT_REQUIRED", "INVOICE_CREATED"}, "what stays")
			}
			again, err := rt.PruneEvents(ctx, agentenkit.PruneOptions{})
			must(t, err)
			mustEqual(t, again.Total, int64(0), "safe to run again")
		})
	}
}

// A storage that keeps each tenant's events in their own database can only
// prune when it is told whose. PruneOptions.State reaches it like a run's
// state; without it the prune fails and nothing goes.
func TestPruneEvents_ATenantsEventsArePrunedWithTheCallersState(t *testing.T) {
	ctx, sc := context.Background(), ports.StorageContext{}
	byTenant := map[string]*memory.Storage{"a": memory.NewStorage(), "b": memory.NewStorage()}
	threads := map[string]string{}
	for tenant, s := range byTenant {
		th, err := s.Threads().Create(ctx, ports.ThreadInit{}, sc)
		must(t, err)
		threads[tenant] = th.ID
		for _, typ := range []string{"CHUNK", "STATE_CHANGE", "INPUT_REQUIRED"} {
			_, err := s.Events().Append(ctx, th.ID, ports.NewThreadEvent{Type: typ, Payload: json.RawMessage(`{}`)}, sc)
			must(t, err)
		}
	}
	storage := perTenantPruning{Storage: byTenant["a"], byTenant: byTenant}
	rt, err := agentenkit.SetupAgentCore(ctx, agentenkit.RuntimeOptions{
		Storage: storage, Admin: memoryadmin.New(), Bus: memory.NewBus(), Queue: memory.NewQueue(), Kv: memory.NewKv(),
		Streams:      memory.NewRunStreams(),
		ResolveModel: func(string) (agentenkit.ResolvedModel, error) { return agentenkit.ResolvedModel{}, nil },
	})
	must(t, err)

	if _, err := rt.PruneEvents(ctx, agentenkit.PruneOptions{}); err == nil {
		t.Fatal("a prune with no tenant must fail, not guess")
	}
	done, err := rt.PruneEvents(ctx, agentenkit.PruneOptions{State: agentenkit.AgentRunState{"tenant": "a"}})
	must(t, err)
	mustEqual(t, done.Total, int64(2), "tenant a's old rows went")
	left, _ := byTenant["a"].Events().ListSince(ctx, threads["a"], -1, sc)
	mustEqual(t, len(left), 1, "tenant a keeps its record")
	other, _ := byTenant["b"].Events().ListSince(ctx, threads["b"], -1, sc)
	mustEqual(t, len(other), 3, "tenant b is untouched")
}

// perTenantPruning prunes the events of the tenant sc names.
type perTenantPruning struct {
	*memory.Storage
	byTenant map[string]*memory.Storage
}

func (s perTenantPruning) Events() ports.EventStore {
	return tenantPruner{EventStore: s.Storage.Events(), byTenant: s.byTenant}
}

type tenantPruner struct {
	ports.EventStore
	byTenant map[string]*memory.Storage
}

func (p tenantPruner) Prune(ctx context.Context, types []string, limit int, dryRun bool, sc ports.StorageContext) (map[string]int64, error) {
	tenant, _ := sc.State["tenant"].(string)
	s, ok := p.byTenant[tenant]
	if !ok {
		return nil, errors.New("no tenant")
	}
	return s.Events().(ports.EventPruner).Prune(ctx, types, limit, dryRun, sc)
}
