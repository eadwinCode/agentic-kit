package agentenkit_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// rfc3339 reads a time off a published payload; empty when it is not there.
func rfc3339(v any) time.Time {
	s, _ := v.(string)
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// Every STATE_CHANGE names its run, and carries when that run started or
// ended, so a client keeps a timer per run without ever refetching history.
func TestRunTiming_EveryStateChangeNamesItsRunAndItsClock(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "wipe", `{}`}}},
		step{text: "done"},
	), func(c *agentenkit.AgentConfig) { c.HITLTTL = time.Hour })
	wipe := tool("wipe", func(context.Context, map[string]any) (string, error) { return "wiped", nil })
	wipe.RequiresConfirmation = true
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Tools: []agentenkit.Tool{wipe}})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)

	// Dispatch: RUNNING names the run and its start, the record's own.
	running := payload(h.events(ran.ThreadID, "STATE_CHANGE")[0])
	mustEqual(t, running["state"], "RUNNING", "first state")
	mustEqual(t, running["runId"], ran.RunID, "run on the dispatch boundary")
	if !rfc3339(running["startedAt"]).Equal(rec.StartedAt) {
		t.Fatalf("startedAt on the wire %v, record %v", running["startedAt"], rec.StartedAt)
	}

	// The park: WAITING_FOR_INPUT carries the same run and the same start.
	h.handleNext(t)
	states := h.events(ran.ThreadID, "STATE_CHANGE")
	waiting := payload(states[len(states)-1])
	mustEqual(t, waiting["state"], "WAITING_FOR_INPUT", "parked")
	mustEqual(t, waiting["runId"], ran.RunID, "run on the park")
	if !rfc3339(waiting["startedAt"]).Equal(rec.StartedAt) {
		t.Fatalf("park startedAt %v, record %v", waiting["startedAt"], rec.StartedAt)
	}

	// The resume: RUNNING again, same run, same start.
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true}); err != nil {
		t.Fatal(err)
	}
	h.drain(t)
	states = h.events(ran.ThreadID, "STATE_CHANGE")
	resumed := payload(states[len(states)-2])
	mustEqual(t, resumed["state"], "RUNNING", "resumed")
	mustEqual(t, resumed["runId"], ran.RunID, "run on the resume")
	if !rfc3339(resumed["startedAt"]).Equal(rec.StartedAt) {
		t.Fatalf("resume startedAt %v, record %v", resumed["startedAt"], rec.StartedAt)
	}

	// The end: the terminal event names the run and when it ended, exactly
	// as the record has it.
	closed, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	term := payload(states[len(states)-1])
	mustEqual(t, term["state"], "COMPLETED", "terminal")
	mustEqual(t, term["runId"], ran.RunID, "run on the terminal")
	if closed.EndedAt == nil || !rfc3339(term["endedAt"]).Equal(*closed.EndedAt) {
		t.Fatalf("endedAt on the wire %v, record %v", term["endedAt"], closed.EndedAt)
	}
	for _, e := range states {
		if payload(e)["runId"] != ran.RunID {
			t.Fatalf("a STATE_CHANGE without the run: %s", e.Payload)
		}
	}
}

func TestRunTiming_AFailedRunEndsOnTheWireToo(t *testing.T) {
	h := makeRuntime(t, scripted(step{err: errBoom}), func(c *agentenkit.AgentConfig) { c.RunMaxAttempts = 1 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	term := h.lastTerminal(ran.ThreadID)
	mustEqual(t, term["state"], "FAILED", "terminal")
	mustEqual(t, term["runId"], ran.RunID, "run on the failure")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	if rec.EndedAt == nil || !rfc3339(term["endedAt"]).Equal(*rec.EndedAt) {
		t.Fatalf("endedAt on the wire %v, record %v", term["endedAt"], rec.EndedAt)
	}
}

// The snapshot's runs carry their clocks, so a reload starts the timer
// where the wire left it.
func TestRunTiming_TheSnapshotRunsCarryTheirClocks(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	snap, err := h.rt.GetThreadSnapshot(h.ctx, ran.ThreadID, nil)
	if err != nil || snap == nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(snap)
	var view struct {
		Runs []map[string]any `json:"runs"`
	}
	_ = json.Unmarshal(raw, &view)
	mustEqual(t, len(view.Runs), 1, "one run")
	mustEqual(t, view.Runs[0]["id"], ran.RunID, "the run")
	if rfc3339(view.Runs[0]["startedAt"]).IsZero() || rfc3339(view.Runs[0]["endedAt"]).IsZero() {
		t.Fatalf("snapshot run without clocks: %v", view.Runs[0])
	}
}

// goai runs a step's tools without streaming a result chunk for them. The
// loop publishes one per result, before the step commits, so a tool card
// flips to done (or failed) live rather than on the next reload.
func TestLoop_PublishesAToolResultChunkPerLocalTool(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "lookup", `{"q":"x"}`}, {"c2", "broken", `{}`}, {"c3", "wipe", `{}`}}},
		step{text: "done"},
	), func(c *agentenkit.AgentConfig) { c.HITLTTL = time.Hour })
	wipe := tool("wipe", nil)
	wipe.RequiresConfirmation = true
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Tools: []agentenkit.Tool{
		tool("lookup", func(context.Context, map[string]any) (string, error) { return `{"found":true}`, nil }),
		tool("broken", func(context.Context, map[string]any) (string, error) { return "", errBoom }),
		wipe,
	}})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)

	results := map[string]map[string]any{}
	var lastResultSeq int64
	for _, e := range h.events(ran.ThreadID, "CHUNK") {
		p := payload(e)
		if p["type"] == "tool-result" {
			results[p["toolCallId"].(string)] = p
			lastResultSeq = e.Seq
		}
	}
	mustEqual(t, len(results), 2, "one result per executed tool; the parked call has none yet")
	mustEqual(t, results["c1"]["toolName"], "lookup", "tool name")
	mustEqual(t, results["c1"]["result"].(map[string]any)["found"], true, "the tool's own output")
	mustEqual(t, results["c2"]["result"].(map[string]any)["error"], "boom", "a failed tool names its error")
	committed := h.events(ran.ThreadID, "STEP_COMMITTED")
	if len(committed) == 0 || committed[0].Seq < lastResultSeq {
		t.Fatal("tool results must be published before the step commits")
	}
}

// A nested run's tool results travel on its own stream, like its chunks.
func TestLoop_ANestedRunsToolResultsRideItsOwnStream(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},
		step{calls: []call{{"c2", "childLookup", `{}`}}}, // the child
		step{text: "child done"},
		step{text: "parent done"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		Subagents: &agentenkit.SubagentsConfig{
			Tools: []agentenkit.Tool{tool("childLookup", func(context.Context, map[string]any) (string, error) { return "the answer", nil })},
		},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	var nested []map[string]any
	for _, e := range h.events(ran.ThreadID, "SUBAGENT_CHUNK") {
		if chunk, _ := payload(e)["chunk"].(map[string]any); chunk["type"] == "tool-result" {
			nested = append(nested, chunk)
		}
	}
	mustEqual(t, len(nested), 1, "the child's result on the child's stream")
	mustEqual(t, nested[0]["toolCallId"], "c2", "call")
	mustEqual(t, nested[0]["result"], "the answer", "output")

	for _, e := range h.events(ran.ThreadID, "CHUNK") {
		if p := payload(e); p["type"] == "tool-result" && p["toolCallId"] == "c2" {
			t.Fatal("a nested tool result leaked onto the main stream")
		}
	}
}
