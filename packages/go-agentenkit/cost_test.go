package agentenkit_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/pricing"
)

// $10 per million input, $30 per million output. A step of 10 input + 5
// output is therefore 100 + 150 = 250 micros, a quarter of a cent.
var testTable = pricing.Table{
	"gpt-4o": {InputPerMillion: 10, OutputPerMillion: 30},
}

func withPricer(p agentenkit.Pricer) func(*agentenkit.RuntimeOptions) {
	return func(o *agentenkit.RuntimeOptions) { o.Pricer = p }
}

func TestCost_EveryCallIsPricedOnTheRowThatStoresIt(t *testing.T) {
	h := makeRuntimeOpts(t, scripted(
		step{calls: []call{{"c1", "probe", `{}`}}},
		step{text: "done"},
	), withPricer(testTable))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) {
			return `{"ok":true}`, nil
		})},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)

	rows := h.storage.UsageRows()
	mustEqual(t, len(rows), 2, "usage rows")
	for _, r := range rows {
		if r.Cost == nil {
			t.Fatal("row stored unpriced")
		}
		mustEqual(t, r.Cost.Micros, int64(250), "row cost")
		mustEqual(t, r.Cost.Currency, "USD", "currency")
		mustEqual(t, r.Cost.Source, "table", "source")
		mustEqual(t, r.Model, "gpt-4o", "registry key on the row")
	}

	// The read side sums money as well as tokens.
	u, err := h.rt.GetThreadUsage(h.ctx, ran.ThreadID, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, u.Tokens.CostMicros, int64(500), "thread cost")
	mustEqual(t, u.Tokens.Currency, "USD", "thread currency")
	mustEqual(t, u.Tokens.Unpriced, 0, "nothing unpriced")
	mustEqual(t, u.Tokens.TotalTokens, 30, "thread tokens")

	// And so does the admin run view, from the same rows.
	detail, err := h.rt.Admin.GetRun(h.ctx, ran.RunID)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, detail.Usage.CostMicros, int64(500), "run cost")
	mustEqual(t, len(detail.Usage.Lines), 1, "one agent on one model, one line")
	mustEqual(t, detail.Usage.Lines[0].Calls, 2, "calls on the line")
	mustEqual(t, detail.Usage.Lines[0].AgentName, "chat", "line agent")
}

func TestCost_WithoutAPricerRowsAreStoredUnpriced(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "done"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)

	if h.storage.UsageRows()[0].Cost != nil {
		t.Fatal("priced with no pricer configured")
	}
	u, _ := h.rt.GetThreadUsage(h.ctx, ran.ThreadID, nil)
	mustEqual(t, u.Tokens.CostMicros, int64(0), "no money")
	// Unpriced above zero is how a reader tells "spent nothing" apart from
	// "nobody priced it".
	mustEqual(t, u.Tokens.Unpriced, 1, "unpriced calls counted")
}

func TestCost_AModelTheTableDoesNotKnowIsLeftUnpriced(t *testing.T) {
	h := makeRuntimeOpts(t, scripted(step{text: "done"}), withPricer(testTable))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "some-other-model"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)

	u, _ := h.rt.GetThreadUsage(h.ctx, ran.ThreadID, nil)
	mustEqual(t, u.Tokens.Unpriced, 1, "missing price shows as a gap")
	mustEqual(t, u.Tokens.CostMicros, int64(0), "and never as a silent zero")
	// This resolver declares no wire id for the key, so the key is the id.
	mustEqual(t, h.storage.UsageRows()[0].ModelID, "some-other-model", "modelId falls back to the key")
}

func TestCost_OnFinishGetsTheWholeRunsBill(t *testing.T) {
	var got agentenkit.RunFinishInfo
	h := makeRuntimeOpts(t, scripted(
		step{calls: []call{{"c1", "probe", `{}`}}},
		step{text: "done"},
	), withPricer(testTable))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) {
			return `{}`, nil
		})},
		OnFinish: func(i agentenkit.RunFinishInfo) { got = i },
	})
	h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)

	mustEqual(t, got.Usage.CostMicros, int64(500), "billed cost")
	mustEqual(t, got.Usage.Currency, "USD", "billed currency")
	mustEqual(t, got.Usage.Unpriced, 0, "nothing unpriced")
	// The bill is the lines: one per agent and model, ready to charge.
	mustEqual(t, len(got.Usage.Lines), 1, "bill lines")
	line := got.Usage.Lines[0]
	mustEqual(t, line.AgentName, "chat", "line names the agent")
	mustEqual(t, line.ModelID, "gpt-4o-2024-11-20", "line carries the wire id from resolveModel")
	mustEqual(t, line.Model, "gpt-4o", "line carries the registry key")
	mustEqual(t, line.Calls, 2, "calls")
	mustEqual(t, line.Estimated, 0, "no estimates")
	mustEqual(t, line.CostMicros, int64(500), "line cost")
}

func TestCost_BudgetStopsTheRunBetweenSteps(t *testing.T) {
	h := makeRuntimeOpts(t, scripted(
		step{calls: []call{{"c1", "probe", `{}`}}},
		step{calls: []call{{"c2", "probe", `{}`}}},
		step{text: "never reached"},
	), withPricer(testTable))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) {
			return `{}`, nil
		})},
	})
	// 400 micros: the first step (250) stays under, the second crosses it.
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi", CostBudgetMicros: 400})
	mustEqual(t, h.queue.Items()[0].CostBudgetMicros, int64(400), "cap rides on the job")
	h.handleNext(t)

	// The step that crossed the line is kept in full; the next never starts.
	mustEqual(t, h.model.Calls(), 2, "model calls")
	ev := h.events(ran.ThreadID, "COST_BUDGET_EXHAUSTED")
	mustEqual(t, len(ev), 1, "COST_BUDGET_EXHAUSTED")
	p := payload(ev[0])
	mustEqual(t, p["costMicros"], float64(500), "spent")
	mustEqual(t, p["costBudgetMicros"], float64(400), "cap")
	mustEqual(t, p["currency"], "USD", "currency")
	// A money break is not a stop: the run completes, and says why.
	mustEqual(t, h.lastTerminal(ran.ThreadID)["stopReason"], "cost_budget", "stopReason")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "state")
}

func TestCost_AStoppedCallIsStillBilled(t *testing.T) {
	h := makeRuntimeOpts(t, scripted(
		step{calls: []call{{"c1", "probe", `{}`}}},
		step{text: "cut off", delay: 500 * time.Millisecond},
	), withPricer(testTable), func(c *agentenkit.AgentConfig) { c.StopPoll = 5 * time.Millisecond })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) {
			return `{}`, nil
		})},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	job, _ := h.queue.Shift()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _ = h.rt.Worker.HandleJob(h.ctx, job) }()
	// Stop once the second call is actually in flight, rather than after a
	// guessed sleep: a loaded machine must not turn this into a flake.
	waitFor(t, func() bool { return h.model.Calls() == 2 })
	if _, err := chat.Stop(h.ctx, ran.ThreadID, nil); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	rows := h.storage.UsageRows()
	mustEqual(t, len(rows), 2, "the cut-off call is recorded too")
	cut := rows[1]
	mustEqual(t, string(cut.Outcome), "aborted", "outcome")
	mustEqual(t, cut.Estimated, true, "marked as an estimate")
	// The prompt was certainly sent, so the previous call's input count is
	// the floor to bill.
	mustEqual(t, cut.InputTokens, 10, "input estimated from the last finish")
}

func TestCost_CompactionIsBilledUnderItsOwnKind(t *testing.T) {
	long := strings.Repeat("word ", 4_000)
	h := makeRuntimeOpts(t, scripted(step{text: "done"}), withPricer(testTable),
		func(c *agentenkit.AgentConfig) {
			c.ContextCeilingTokens = 2_000
			c.ContextOutputReserveTokens = 200
			c.CompactionModel = "gpt-4o"
		})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: long})
	h.handleNext(t)
	ran2 := h.run(t, chat, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: long})
	_ = ran2
	h.handleNext(t)

	var compactions int
	for _, r := range h.storage.UsageRows() {
		if r.Kind == agentenkit.KindCompaction {
			compactions++
			if r.Cost == nil {
				t.Fatal("the platform's own call went unpriced")
			}
		}
	}
	if compactions == 0 {
		t.Fatal("no compaction call recorded")
	}
}
