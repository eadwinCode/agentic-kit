package agentenkit_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	memoryadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/memory"
	pgadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/postgres"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream H: a run settles exactly once whatever way it ends, its caps
// count every segment, and a retry never asks the model twice. The same
// cases run in the TS package (test/settle.test.ts).

// Only one claim on a run's settle wins; a claim left behind by a settler
// that died is taken over once it is old enough; a settled run is never
// claimed again. Every admin store keeps the same rule.
func TestSettle_OnlyOneClaimWinsAndAStaleOneIsTakenOver(t *testing.T) {
	stores := map[string]func(t *testing.T) ports.AdminStore{
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
			// A clean slate, as TestPostgresAdminStore_RoundTripsARun does, so
			// the claim runs against a freshly migrated schema.
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
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			runs := open(t).Runs()
			id := "claim-" + agentenkit.NewID()
			if _, err := runs.Start(ctx, ports.NewRunRecord{ID: id, ThreadID: "t-" + id, Agent: "chat", Model: "m"}); err != nil {
				t.Fatal(err)
			}
			fresh := time.Now().Add(-agentenkit.SettleClaimTTL)
			first, err := runs.ClaimSettle(ctx, id, "a", fresh)
			if err != nil {
				t.Fatal(err)
			}
			second, _ := runs.ClaimSettle(ctx, id, "b", fresh)
			mustEqual(t, first, true, "the first claim wins")
			mustEqual(t, second, false, "a second one loses while the first is fresh")

			// The first settler died: a claim older than the cut-off is taken over.
			taken, _ := runs.ClaimSettle(ctx, id, "c", time.Now().Add(time.Second))
			mustEqual(t, taken, true, "a stale claim is taken over")
			// The dead settler's late mark is not believed: the claim is not its own.
			if err := runs.EndSettle(ctx, id, "a", true); err != nil {
				t.Fatal(err)
			}
			rec, _ := runs.Get(ctx, id)
			if rec.SettledAt != nil {
				t.Fatal("a claim that was taken over cannot mark the run settled")
			}
			if err := runs.EndSettle(ctx, id, "c", true); err != nil {
				t.Fatal(err)
			}
			rec, _ = runs.Get(ctx, id)
			if rec.SettledAt == nil || rec.SettlingAt != nil {
				t.Fatalf("settled and no longer settling: %+v", rec)
			}
			again, _ := runs.ClaimSettle(ctx, id, "d", time.Now().Add(time.Hour))
			mustEqual(t, again, false, "a settled run is never claimed again")
		})
	}
}

// Two settlers that arrive together bill once.
func TestSettle_TwoSettlersAtOnceBillOnce(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		OnSettle: func(context.Context, agentenkit.RunFinishInfo) error {
			calls.Add(1)
			<-release
			return errors.New("first settle fails, so the run stays unsettled")
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	go func() { time.Sleep(50 * time.Millisecond); close(release) }()
	h.handleNext(t)
	mustEqual(t, calls.Load(), int32(1), "the worker settled")

	// Two sweeps at once over the same unsettled run: one claim wins.
	calls.Store(0)
	release = make(chan struct{})
	done := make(chan agentenkit.ReclaimReport, 2)
	for range 2 {
		go func() {
			r, _ := h.rt.ReclaimStuckRuns(h.ctx, 0)
			done <- r
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	a, b := <-done, <-done
	mustEqual(t, calls.Load(), int32(1), "the hook ran once for both sweeps")
	mustEqual(t, a.Settled+b.Settled, 1, "one sweep settled it")
	_ = ran
}

// A run with no recorded state is still found and settled by the sweep.
func TestSettle_TheSweepSettlesARunWithNoRecordedState(t *testing.T) {
	calls := 0
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		OnSettle: func(context.Context, agentenkit.RunFinishInfo) error {
			calls++
			if calls == 1 {
				return errors.New("ledger down")
			}
			return nil
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"}) // no state
	h.handleNext(t)
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	if rec.RunState != nil || rec.SettledAt != nil {
		t.Fatalf("a run with no state, unsettled: %+v", rec)
	}
	report, err := h.rt.ReclaimStuckRuns(h.ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, report.Settled, 1, "settled by the sweep")
	mustEqual(t, calls, 2, "the hook ran again")
}

// The sweep reads every page of unsettled runs, not only the first.
func TestSettle_TheSweepReadsEveryPage(t *testing.T) {
	var calls atomic.Int32
	h := makeRuntime(t, scripted(step{text: "never"}))
	h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:     "chat",
		OnSettle: func(context.Context, agentenkit.RunFinishInfo) error { calls.Add(1); return nil },
	})
	const n = 501
	ended := time.Now().Add(-time.Minute)
	for i := range n {
		id := fmt.Sprintf("page-%03d", i)
		if _, err := h.admin.Runs().Start(h.ctx, ports.NewRunRecord{ID: id, ThreadID: "t-" + id, Agent: "chat", Model: "gpt-4o"}); err != nil {
			t.Fatal(err)
		}
		_ = h.admin.Runs().Patch(h.ctx, id, ports.RunPatch{State: ports.Ptr(ports.StateCompleted), EndedAt: &ended})
	}
	time.Sleep(5 * time.Millisecond)
	report, err := h.rt.ReclaimStuckRuns(h.ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, report.Settled, n, "every run settled")
	mustEqual(t, calls.Load(), int32(n), "once each")
}

// A token budget counts every segment of a run: one that parks and resumes
// three times does not get a fresh budget each time.
func TestBudget_ARunThatParksThreeTimesCountsEverySegment(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "wipe", `{}`}}},
		step{calls: []call{{"c2", "wipe", `{}`}}},
		step{calls: []call{{"c3", "wipe", `{}`}}},
		step{text: "done"},
	), func(c *agentenkit.AgentConfig) { c.HITLTTL = time.Hour })
	wipe := tool("wipe", func(context.Context, map[string]any) (string, error) { return "wiped", nil })
	wipe.RequiresConfirmation = true
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Tools: []agentenkit.Tool{wipe}})
	// Each step spends 15 (10 in, 5 out). The budget fits two steps, not four.
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go", TokenBudget: 35})
	h.handleNext(t)
	for _, id := range []string{"c1", "c2", "c3"} {
		mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "parked on "+id)
		if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: id, Approved: true}); err != nil {
			t.Fatal(err)
		}
		h.handleNext(t)
	}
	term := h.lastTerminal(ran.ThreadID)
	mustEqual(t, term["stopReason"], "token_budget", "the budget counted all four steps")
	mustEqual(t, term["tokensUsed"], float64(60), "the whole run's tokens")
}

// failStepCommitOnce fails the first STEP_COMMITTED write: the worker dies
// right after the step's messages and usage are saved.
type failStepCommitOnce struct {
	ports.EventBus
	failed atomic.Bool
}

// Publish fails the first STEP_COMMITTED: sending the step's commit is the
// first thing after its messages and usage are saved.
func (b *failStepCommitOnce) Publish(ctx context.Context, threadID string, e ports.AgentEvent) error {
	if e.Type == "STEP_COMMITTED" && b.failed.CompareAndSwap(false, true) {
		return errors.New("worker died")
	}
	return b.EventBus.Publish(ctx, threadID, e)
}

// A retry after the run's last step was saved finalizes from the saved
// answer: the model is not asked a second time, and the call is billed once.
func TestRetry_ARunWhoseLastStepWasSavedIsNotAskedAgain(t *testing.T) {
	var settles atomic.Int32
	h := makeRuntimeOpts(t, scripted(step{text: "the answer"}, step{text: "a second answer"}), func(o *agentenkit.RuntimeOptions) {
		o.Bus = &failStepCommitOnce{EventBus: o.Bus}
	}, func(c *agentenkit.AgentConfig) { c.RunRetryBackoff = 0 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:     "chat",
		OnSettle: func(context.Context, agentenkit.RunFinishInfo) error { settles.Add(1); return nil },
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t) // saves the answer, then dies
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateQueued, "waiting to retry")
	h.handleNext(t) // the retry
	mustEqual(t, h.model.Calls(), 1, "the model was asked once")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "the run completed")
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant"}, "one answer")
	mustEqual(t, len(h.storage.UsageRows()), 1, "the call billed once")
	mustEqual(t, settles.Load(), int32(1), "settled once")
}

// A run is priced in one currency: a call priced in another is stored
// unpriced rather than mixed into a bill that cannot add it up.
func TestCost_ACallPricedInASecondCurrencyIsStoredUnpriced(t *testing.T) {
	var n atomic.Int32
	pricer := ports.PricerFunc(func(context.Context, ports.NewUsage) (*ports.Cost, error) {
		if n.Add(1) == 1 {
			return &ports.Cost{Micros: 100, Currency: "USD", Source: "table"}, nil
		}
		return &ports.Cost{Micros: 900, Currency: "EUR", Source: "table"}, nil
	})
	h := makeRuntimeOpts(t, scripted(step{calls: []call{{"c1", "probe", `{}`}}}, step{text: "done"}), withPricer(pricer))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:  "chat",
		Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) { return "ok", nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	rows := h.storage.UsageRows()
	mustEqual(t, len(rows), 2, "two calls")
	if rows[1].Cost != nil {
		t.Fatalf("the euro call is stored unpriced: %+v", rows[1].Cost)
	}
	bill, _ := h.storage.Usage().Total(h.ctx, ran.ThreadID, ports.UsageFilter{RunID: ran.RunID}, agentenkit.StorageContext{})
	mustEqual(t, len(bill.Costs), 1, "one currency on the run's bill")
	mustEqual(t, bill.Unpriced, 1, "the refused call is a gap in it")
}

// The thread view in the admin reads its money from the usage rows.
func TestAdmin_AThreadDetailCarriesItsCost(t *testing.T) {
	h := makeRuntimeOpts(t, scripted(step{text: "done"}), withPricer(testTable))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	detail, err := h.rt.Admin.GetThread(h.ctx, ran.ThreadID)
	if err != nil || detail == nil {
		t.Fatalf("thread detail: %+v %v", detail, err)
	}
	mustEqual(t, detail.Thread.Tokens.CostMicros, int64(250), "the thread's cost")
	mustEqual(t, detail.Thread.Tokens.Currency, "USD", "in its currency")
}
