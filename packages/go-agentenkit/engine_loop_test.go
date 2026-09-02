package agentenkit_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

func TestEngineLoop_FeedsToolResultsBackAndPersistsPerStep(t *testing.T) {
	var executed []string
	h := makeRuntime(t, scripted(
		step{calls: []call{{"call_1", "lookup", `{"q":"x"}`}}},
		step{text: "done"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("lookup", func(_ context.Context, args map[string]any) (string, error) {
			executed = append(executed, args["q"].(string))
			return `{"ok":true}`, nil
		})},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	mustEqual(t, h.queue.Len(), 1, "queued jobs")
	h.handleNext(t)

	mustStrings(t, executed, []string{"x"}, "executed tools")
	mustStrings(t, h.states(ran.ThreadID), []string{"RUNNING", "COMPLETED"}, "states")
	terminal := h.lastTerminal(ran.ThreadID)
	mustEqual(t, terminal["stopReason"], "completed", "stopReason")
	mustEqual(t, terminal["tokensUsed"], float64(30), "tokensUsed") // 15 per step × 2 steps

	// Per-step persistence: user, assistant(tool-call), tool(result), assistant(text)
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant", "tool", "assistant"}, "roles")
	toolMsg := h.storage.MessageRows(ran.ThreadID)[2]
	parts := agentenkit.ParseContent(toolMsg.Content)
	mustEqual(t, parts[0].Type, "tool-result", "part type")
	mustEqual(t, parts[0].ToolCallID, "call_1", "toolCallId")
	mustEqual(t, parts[0].ToolName, "lookup", "toolName")
	mustEqual(t, string(parts[0].Result), `{"ok":true}`, "result")

	// §4 attribution: both steps recorded as one segment row
	rows := h.storage.UsageRows()
	mustEqual(t, len(rows), 1, "usage rows")
	mustEqual(t, rows[0].AgentID, "chat", "usage agent")
	mustEqual(t, rows[0].InputTokens, 20, "input")
	mustEqual(t, rows[0].OutputTokens, 10, "output")
	mustEqual(t, rows[0].TotalTokens, 30, "total")
}

func TestEngineLoop_BudgetIsCheckedBetweenSteps(t *testing.T) {
	calls := 0
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "probe", `{}`}}, usage: &[2]int{50, 10}},
		step{calls: []call{{"c2", "probe", `{}`}}, usage: &[2]int{50, 10}},
		step{text: "never"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o", TokenBudget: 100,
		Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) {
			calls++
			return "ok", nil
		})},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	// Step 1 spends 60 (< 100), step 2 crosses to 120 and is kept in full; step 3 never starts.
	mustEqual(t, calls, 2, "tool calls")
	mustEqual(t, h.model.Calls(), 2, "model calls")
	terminal := h.lastTerminal(ran.ThreadID)
	mustEqual(t, terminal["state"], "COMPLETED", "state")
	mustEqual(t, terminal["stopReason"], "token_budget", "stopReason")
	mustEqual(t, terminal["tokensUsed"], float64(120), "tokensUsed")
	// The break was announced before the run ended
	exhausted := h.events(ran.ThreadID, "TOKEN_BUDGET_EXHAUSTED")
	mustEqual(t, len(exhausted), 1, "TOKEN_BUDGET_EXHAUSTED")
	mustEqual(t, payload(exhausted[0])["tokensUsed"], float64(120), "tokensUsed on the event")
	mustEqual(t, payload(exhausted[0])["tokenBudget"], float64(100), "tokenBudget on the event")
	if exhausted[0].Seq >= h.events(ran.ThreadID, "STATE_CHANGE")[1].Seq {
		t.Fatal("must be published before the terminal STATE_CHANGE")
	}
}

func TestEngineLoop_MaxStepsFinalizesWhenCeilingHitWithPendingCalls(t *testing.T) {
	h := makeRuntime(t, scripted(step{calls: []call{{"c", "probe", `{}`}}}), func(c *agentenkit.AgentConfig) {
		c.MaxSteps = 2
		c.SubagentMaxSteps = 1
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("probe", func(context.Context, map[string]any) (string, error) { return "ok", nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "loop"})
	h.handleNext(t)
	mustEqual(t, h.model.Calls(), 2, "model calls")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["stopReason"], "max_steps", "stopReason")
}

func TestEngineLoop_OneShotPublishesTextResult(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "final answer"}))
	one := h.rt.CreateGenerateTextAgent(agentenkit.GenerateTextAgentSpec{Name: "one", Model: "gpt-4o"})
	ran := h.run(t, one, agentenkit.RunInput{Prompt: "q"})
	h.handleNext(t)
	mustEqual(t, len(h.events(ran.ThreadID, "CHUNK")), 0, "chunks")
	results := h.events(ran.ThreadID, "TEXT_RESULT")
	mustEqual(t, len(results), 1, "TEXT_RESULT events")
	mustEqual(t, payload(results[0])["text"], "final answer", "text")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant"}, "roles")
}

func TestEngineLoop_StreamPublishesChunksInAISDKShape(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "hello"}))
	var seen []string
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		OnChunk: func(c provider.StreamChunk) { seen = append(seen, string(c.Type)) },
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	chunks := h.events(ran.ThreadID, "CHUNK")
	if len(chunks) == 0 {
		t.Fatal("no CHUNK events")
	}
	first := payload(chunks[0])
	mustEqual(t, first["type"], "text-delta", "first chunk type")
	mustEqual(t, first["textDelta"], "hello", "textDelta")
	if len(seen) == 0 {
		t.Fatal("user OnChunk never fired")
	}
}

// A client rebuilds from durable messages and THEN replays activeEvents. So
// a step whose messages are already committed must not have its chunks
// replayed as well, or its text lands twice.
func TestReconnect_ReplaysOnlyTheUncommittedStep(t *testing.T) {
	var snap *agentenkit.ThreadSnapshot
	var threadID string
	h := makeRuntime(t, scripted(
		step{text: "PART ONE. ", calls: []call{{"c1", "probe", `{"n":1}`}}},
		step{text: "PART TWO. ", calls: []call{{"c2", "probe", `{"n":2}`}}},
		step{text: "DONE."},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("probe", func(_ context.Context, args map[string]any) (string, error) {
			if args["n"].(float64) == 2 {
				// Runs during step 2, after step 1's messages are durable.
				snap, _ = h.rt.GetThreadSnapshot(h.ctx, threadID, nil)
			}
			return `{"ok":true}`, nil
		})},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	threadID = ran.ThreadID
	h.handleNext(t)
	if snap == nil {
		t.Fatal("snapshot not taken")
	}
	assistantText := ""
	for _, m := range snap.Messages {
		if m.Role == agentenkit.RoleAssistant {
			for _, p := range agentenkit.ParseContent(m.Content) {
				assistantText += p.Text
			}
		}
	}
	replayed := ""
	for _, e := range snap.ActiveEvents {
		if e.Type == "CHUNK" && payload(e)["type"] == "text-delta" {
			replayed += payload(e)["textDelta"].(string)
		}
	}
	mustEqual(t, assistantText, "PART ONE. ", "durable text")
	mustEqual(t, replayed, "PART TWO. ", "replayed text")
	mustEqual(t, snap.Thread.State, agentenkit.StateRunning, "state")
}

func TestReconnect_KeepsAPendingApprovalRaisedBeforeTheStepCommitted(t *testing.T) {
	h := makeRuntime(t, scripted(step{calls: []call{{"c1", "danger", `{}`}}}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{agentenkit.MarkRequiresConfirmation(tool("danger", func(context.Context, map[string]any) (string, error) { return "ran", nil }))},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	snap, err := h.rt.GetThreadSnapshot(h.ctx, ran.ThreadID, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, snap.Thread.State, agentenkit.StateWaitingForInput, "state")
	found := false
	for _, e := range snap.ActiveEvents {
		if e.Type == "INPUT_REQUIRED" {
			found = true
		}
	}
	if !found {
		t.Fatal("INPUT_REQUIRED missing from activeEvents")
	}
}

func TestProviderOptions_ThreeLevels(t *testing.T) {
	cases := []struct {
		name          string
		config, spec  agentenkit.ProviderOptions
		run           agentenkit.ProviderOptions
		wantOpenAI    any
		wantAnthropic any
	}{
		{"runtime default", agentenkit.ProviderOptions{"openai": "cfg"}, nil, nil, "cfg", nil},
		{"spec overrides runtime", agentenkit.ProviderOptions{"openai": "cfg"}, agentenkit.ProviderOptions{"openai": "spec"}, nil, "spec", nil},
		{"run overrides both", agentenkit.ProviderOptions{"openai": "cfg"}, agentenkit.ProviderOptions{"openai": "spec"}, agentenkit.ProviderOptions{"openai": "run"}, "run", nil},
		{"keeps an untouched namespace", agentenkit.ProviderOptions{"anthropic": "a"}, agentenkit.ProviderOptions{"openai": "spec"}, nil, "spec", "a"},
		{"nothing set", nil, nil, nil, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.ProviderOptions = tc.config })
			chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o", ProviderOptions: tc.spec})
			h.run(t, chat, agentenkit.RunInput{Prompt: "hi", ProviderOptions: tc.run})
			h.handleNext(t)
			got := h.model.Params()[0].ProviderOptions
			mustEqual(t, got["openai"], tc.wantOpenAI, "openai")
			mustEqual(t, got["anthropic"], tc.wantAnthropic, "anthropic")
		})
	}
}

// ---- HITL run-segment park (§2.5) ----

type hitlHarness struct {
	*harness
	chat     *agentenkit.AgentHandle
	executed *[]string
}

func hitlSetup(t *testing.T, tune ...func(*agentenkit.AgentConfig)) *hitlHarness {
	executed := &[]string{}
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "deleteAccount", `{"id":"acc_1"}`}}},
		step{text: "handled"},
	), tune...)
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{
			agentenkit.MarkRequiresConfirmation(tool("deleteAccount", func(_ context.Context, args map[string]any) (string, error) {
				*executed = append(*executed, args["id"].(string))
				return `{"deleted":true}`, nil
			})),
		},
	})
	return &hitlHarness{harness: h, chat: chat, executed: executed}
}

func TestHITL_ParksAsADurableStateTransition(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete my account"})
	h.handleNext(t)

	mustEqual(t, len(*h.executed), 0, "tool must not run")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "durable state")
	mustEqual(t, h.kvGet(agentenkit.StateKey(ran.ThreadID)), "WAITING_FOR_INPUT", "hot state")
	mustEqual(t, h.kvGet(agentenkit.RunLockKey(ran.ThreadID)), "", "no lock held while parked")
	req := h.events(ran.ThreadID, "INPUT_REQUIRED")
	mustEqual(t, len(req), 1, "INPUT_REQUIRED")
	p := payload(req[0])
	mustEqual(t, p["toolCallId"], "c1", "toolCallId")
	mustEqual(t, p["toolName"], "deleteAccount", "toolName")
	resume := p["resume"].(map[string]any)
	mustEqual(t, resume["agent"], "chat", "resume agent")
	mustEqual(t, resume["runId"], ran.RunID, "resume runId")
	// The park sentinel is never persisted; only the assistant's tool call is.
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant"}, "roles")
	// Usage up to the park is billed
	mustEqual(t, len(h.storage.UsageRows()), 1, "usage rows")
}

func TestHITL_RedeliveredJobWhileParkedIsANoOp(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	job := h.handleNext(t)
	if _, err := h.rt.Worker.HandleJob(h.ctx, job); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "still parked")
	mustEqual(t, h.model.Calls(), 1, "no extra model call")
}

func TestHITL_RespondApprovedResumesViaTheQueue(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	h.queue.Shift() // the park's own expiry job

	res, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true})
	if err != nil || !res.Delivered {
		t.Fatalf("respond: %v %+v", err, res)
	}
	mustEqual(t, h.queue.Len(), 1, "resume job queued")
	job := h.handleNext(t)
	mustEqual(t, job.RunID, ran.RunID, "resume reuses the run id")

	mustStrings(t, *h.executed, []string{"acc_1"}, "the real tool ran on resume")
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant", "tool", "assistant"}, "roles")
	toolMsg := h.storage.MessageRows(ran.ThreadID)[2]
	part := agentenkit.ParseContent(toolMsg.Content)[0]
	mustEqual(t, part.ToolCallID, "c1", "tool result id")
	mustEqual(t, string(part.Result), `{"deleted":true}`, "tool result")
	mustStrings(t, h.states(ran.ThreadID), []string{"RUNNING", "WAITING_FOR_INPUT", "RUNNING", "COMPLETED"}, "states")
	mustEqual(t, h.kvGet(agentenkit.HitlKey("c1")), "", "handoff key consumed")
}

func TestHITL_RespondDeniedAppendsTheDenial(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	h.queue.Shift()
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: false}); err != nil {
		t.Fatal(err)
	}
	h.handleNext(t)
	mustEqual(t, len(*h.executed), 0, "tool never runs")
	part := agentenkit.ParseContent(h.storage.MessageRows(ran.ThreadID)[2].Content)[0]
	mustEqual(t, string(part.Result), `{"denied":true}`, "denial")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
}

func TestHITL_TTLExpiryBecomesTheTimeoutDenial(t *testing.T) {
	h := hitlSetup(t, func(c *agentenkit.AgentConfig) { c.HITLTTL = 10 * time.Millisecond; c.ReclaimGrace = 0 })
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	time.Sleep(30 * time.Millisecond)
	// The park's own expiry job, delivered after the TTL
	h.handleNext(t)
	mustEqual(t, len(*h.executed), 0, "tool never runs")
	part := agentenkit.ParseContent(h.storage.MessageRows(ran.ThreadID)[2].Content)[0]
	if !strings.Contains(string(part.Result), `"reason":"timeout"`) {
		t.Fatalf("expected timeout denial, got %s", part.Result)
	}
	mustEqual(t, len(h.events(ran.ThreadID, "INPUT_EXPIRED")), 1, "INPUT_EXPIRED")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
}

func TestHITL_StopWhileParkedWins(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	stop, err := h.chat.Stop(h.ctx, ran.ThreadID, nil)
	if err != nil || !stop.Accepted {
		t.Fatalf("stop: %v %+v", err, stop)
	}
	res, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered {
		t.Fatal("respond must be rejected after a stop")
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCancelled, "state")
	// A late delivery of the expiry job is a no-op against a cancelled thread
	h.drain(t)
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCancelled, "state after redelivery")
}

func TestHITL_ParkSchedulesItsOwnExpiry(t *testing.T) {
	h := hitlSetup(t, func(c *agentenkit.AgentConfig) { c.HITLTTL = time.Minute; c.ReclaimGrace = 10 * time.Second })
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	mustEqual(t, h.queue.Len(), 1, "expiry job")
	mustEqual(t, h.queue.Delays()[0], 70*time.Second, "delay")
	job := h.queue.Items()[0]
	mustEqual(t, job.RunID, ran.RunID, "expiry job carries the parked run id")
	mustEqual(t, job.Agent, "chat", "expiry job agent")
}

func TestHITL_ExpiryJobDeliveredEarlyLeavesTheThreadParked(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	h.handleNext(t) // the expiry job, long before the TTL
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "still parked")
	mustEqual(t, len(*h.executed), 0, "tool never ran")
}

func TestHITL_ExpiryJobThatWinsTheRaceHonoursTheAnswer(t *testing.T) {
	h := hitlSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "delete"})
	h.handleNext(t)
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true}); err != nil {
		t.Fatal(err)
	}
	// Two jobs wait: the expiry and the answer. Whichever lands first resolves
	// the park with the answer; the other is a no-op.
	h.drain(t)
	mustStrings(t, *h.executed, []string{"acc_1"}, "ran exactly once")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
}

func TestHITL_RunStateReachesTheResumedTool(t *testing.T) {
	var seen agentenkit.AgentRunState
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "danger", `{}`}}},
		step{text: "ok"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{agentenkit.MarkRequiresConfirmation(agentenkit.AgentTool("danger", "d",
			func(_ context.Context, _ struct{}, tc agentenkit.ToolContext) (string, error) {
				seen = tc.State
				return "done", nil
			}))},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go", State: agentenkit.AgentRunState{"orgId": "acme"}})
	h.handleNext(t)
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "c1", Approved: true}); err != nil {
		t.Fatal(err)
	}
	h.drain(t)
	mustEqual(t, seen["orgId"], "acme", "state on the resumed tool")
	// The resumed job carried the state on its ticket
	var jobState string
	for _, sc := range h.storage.Contexts {
		if sc.State["orgId"] == "acme" {
			jobState = "acme"
		}
	}
	mustEqual(t, jobState, "acme", "storage saw the state")
	_ = json.Marshal
}
