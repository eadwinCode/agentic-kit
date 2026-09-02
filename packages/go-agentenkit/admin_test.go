package agentenkit_test

import (
	"context"
	"strings"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

func TestAdmin_RunRecordsOpenOnRunAndCloseWithTimingTokensAndSteps(t *testing.T) {
	h := makeRuntime(t, scripted(step{calls: []call{{"c", "probe", `{"q":1}`}}}, step{text: "done"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) { return `{"ok":true}`, nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	open, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, open.State, agentenkit.StateRunning, "open")
	mustEqual(t, open.Agent, "chat", "agent")
	h.handleNext(t)
	closed, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, closed.State, agentenkit.StateCompleted, "closed")
	mustEqual(t, closed.StopReason, "completed", "stopReason")
	mustEqual(t, closed.Steps, 2, "steps")
	mustEqual(t, closed.TotalTokens, 30, "tokens")
	mustEqual(t, closed.InputTokens, 20, "input")
	if closed.EndedAt == nil || closed.DurationMs == nil || closed.QueuedMs == nil {
		t.Fatalf("timing missing: %+v", closed)
	}
	// A step marker per iteration, with what it did
	steps, _ := h.admin.Steps().ListByRun(h.ctx, ran.RunID)
	mustEqual(t, len(steps), 2, "step rows")
	mustEqual(t, steps[0].Index, 1, "index")
	mustEqual(t, steps[0].FinishReason, "tool-calls", "finish")
	mustStrings(t, steps[0].Tools, []string{"probe"}, "tools")
	mustEqual(t, steps[0].TotalTokens, 15, "step tokens")
	mustEqual(t, len(steps[0].ToolCalls), 1, "tool calls recorded")
	mustEqual(t, string(steps[0].ToolCalls[0].Args), `{"q":1}`, "args")
	mustEqual(t, string(steps[0].ToolCalls[0].Result), `{"ok":true}`, "result")
	mustEqual(t, steps[1].Text, "done", "text")
	mustEqual(t, len(h.events(ran.ThreadID, "STEP_FINISHED")), 2, "live notices")
	mustEqual(t, h.events(ran.ThreadID, "STEP_FINISHED")[0].Seq, int64(0), "notice is bus-only")
}

func TestAdmin_KeepsTheReasonARunFailed(t *testing.T) {
	h := makeRuntime(t, scripted(step{err: errBoom}), func(c *agentenkit.AgentConfig) { c.RunMaxAttempts = 1 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.State, agentenkit.StateFailed, "state")
	if !strings.Contains(rec.Error, "boom") {
		t.Fatalf("error: %q", rec.Error)
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateFailed, "durable")
	mustEqual(t, h.kvGet(agentenkit.StateKey(ran.ThreadID)), "FAILED", "hot")
	term := h.lastTerminal(ran.ThreadID)
	mustEqual(t, term["state"], "FAILED", "published")
	if !strings.Contains(term["error"].(string), "boom") {
		t.Fatalf("published error: %v", term["error"])
	}
}

func TestAdmin_KeepsOnlyTimingsWhenRecordPayloadsIsOff(t *testing.T) {
	h := makeRuntime(t, scripted(step{calls: []call{{"c", "probe", `{"q":1}`}}}, step{text: "secret"}),
		func(c *agentenkit.AgentConfig) { c.RecordPayloads = false })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) { return "x", nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "private prompt", State: agentenkit.AgentRunState{"k": "v"}})
	h.handleNext(t)
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.Prompt, "", "no prompt")
	if rec.RunState != nil {
		t.Fatal("no state")
	}
	steps, _ := h.admin.Steps().ListByRun(h.ctx, ran.RunID)
	mustEqual(t, steps[1].Text, "", "no text")
	mustEqual(t, len(steps[0].ToolCalls), 0, "no payloads")
	mustStrings(t, steps[0].Tools, []string{"probe"}, "tool names stay")
}

func TestAdmin_CapsALargeValue(t *testing.T) {
	big := strings.Repeat("x", 5000)
	h := makeRuntime(t, scripted(step{text: big}), func(c *agentenkit.AgentConfig) { c.PayloadCapChars = 100 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: big})
	h.handleNext(t)
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, len([]rune(rec.Prompt)), 101, "capped prompt")
	steps, _ := h.admin.Steps().ListByRun(h.ctx, ran.RunID)
	mustEqual(t, len([]rune(steps[0].Text)), 101, "capped text")
	// The durable message is never capped
	mustEqual(t, len(h.storage.MessageRows(ran.ThreadID)[0].Content), 5002, "full prompt persisted")
}

func TestAdmin_OverviewStatsAndThreadRollUps(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	a := h.run(t, chat, agentenkit.RunInput{Prompt: "first"})
	h.handleNext(t)
	h.run(t, chat, agentenkit.RunInput{ThreadID: a.ThreadID, Prompt: "again"})
	h.handleNext(t)
	b := h.run(t, chat, agentenkit.RunInput{Prompt: "other"})

	ov, err := h.rt.Admin.Overview(h.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, ov.Runs.Total, 3, "runs")
	mustEqual(t, ov.RunsByState[agentenkit.StateCompleted], 2, "completed")
	mustEqual(t, ov.RunsByState[agentenkit.StateRunning], 1, "running")
	mustEqual(t, ov.Threads[agentenkit.StateRunning], 1, "threads running")
	mustEqual(t, ov.Threads[agentenkit.StateCompleted], 1, "threads completed")
	mustEqual(t, len(ov.Active), 1, "active")
	mustEqual(t, ov.Active[0].ThreadID, b.ThreadID, "active thread")
	mustEqual(t, ov.Runs.Tokens.TotalTokens, 30, "tokens")
	if ov.Runs.Duration == nil || ov.Runs.Queued == nil {
		t.Fatal("percentiles missing")
	}

	stats, _ := h.rt.Admin.Stats(h.ctx, agentenkit.StatsRange{})
	mustEqual(t, stats.ByStopReason["completed"], 2, "by stop reason")

	threads, _ := h.rt.Admin.ListThreads(h.ctx, agentenkit.AdminThreadFilter{})
	mustEqual(t, len(threads), 2, "threads")
	var first agentenkit.ThreadSummary
	for _, th := range threads {
		if th.ID == a.ThreadID {
			first = th
		}
	}
	mustEqual(t, first.Runs, 2, "runs on thread")
	mustEqual(t, first.Steps, 2, "steps on thread")
	mustEqual(t, first.Tokens.TotalTokens, 30, "tokens on thread")
	mustEqual(t, first.Prompt, "first", "the prompt that started it")

	detail, _ := h.rt.Admin.GetThread(h.ctx, a.ThreadID)
	mustEqual(t, len(detail.Runs), 2, "detail runs")
	mustEqual(t, len(detail.Steps), 2, "detail steps")
	none, _ := h.rt.Admin.GetThread(h.ctx, "nope")
	if none != nil {
		t.Fatal("unknown thread must be nil")
	}

	run, _ := h.rt.Admin.GetRun(h.ctx, a.RunID)
	mustEqual(t, len(run.Steps), 1, "run steps")
	for _, e := range run.Events {
		if e.Type == "CHUNK" {
			t.Fatal("chunks must be stripped from a timeline")
		}
	}
	since := time.Now().Add(time.Hour)
	later, _ := h.rt.Admin.ListRuns(h.ctx, agentenkit.RunFilter{Since: &since})
	mustEqual(t, len(later), 0, "since filter")
	byState, _ := h.rt.Admin.ListRuns(h.ctx, agentenkit.RunFilter{State: []agentenkit.ExecutionState{agentenkit.StateRunning}})
	mustEqual(t, len(byState), 1, "state filter")
}
