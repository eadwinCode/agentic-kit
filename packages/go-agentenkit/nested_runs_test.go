package agentenkit_test

import (
	"context"
	"strings"
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

const spawnArgs = `{"name":"researcher","instructions":"Find the answer and report back."}`

func TestNested_DelegatesInAnIsolatedStreamAndReportsBack(t *testing.T) {
	// Model calls, in order: parent asks to spawn; the child answers; the parent finishes.
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},
		step{text: "child says 42", usage: &[2]int{20, 10}},
		step{text: "parent done"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o", Subagents: &agentenkit.SubagentsConfig{},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "research this"})
	h.handleNext(t)

	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
	// The run-wide ledger counts the child's spend: 15 + 30 + 15
	mustEqual(t, h.lastTerminal(ran.ThreadID)["tokensUsed"], float64(60), "tokensUsed")

	started := h.events(ran.ThreadID, "SUBAGENT_STARTED")
	mustEqual(t, len(started), 1, "SUBAGENT_STARTED")
	childID := payload(started[0])["agentId"].(string)
	mustEqual(t, payload(started[0])["name"], "researcher", "name")
	mustEqual(t, len(h.events(ran.ThreadID, "SUBAGENT_COMPLETED")), 1, "SUBAGENT_COMPLETED")

	// The child's turns live in its own stream; the parent's history never sees them
	parent, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	child, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.AgentScope(childID), agentenkit.StorageContext{})
	var parentRoles, childRoles []string
	for _, m := range parent {
		parentRoles = append(parentRoles, string(m.Role))
	}
	for _, m := range child {
		childRoles = append(childRoles, string(m.Role))
	}
	mustStrings(t, parentRoles, []string{"user", "assistant", "tool", "assistant"}, "parent roles")
	mustStrings(t, childRoles, []string{"user", "assistant"}, "child roles")
	mustEqual(t, string(child[0].Content), `"Find the answer and report back."`, "the brief is the child's only input")
	// The parent received the capped result
	part := agentenkit.ParseContent(parent[2].Content)[0]
	if !strings.Contains(string(part.Result), `"result":"child says 42"`) {
		t.Fatalf("parent tool result: %s", part.Result)
	}
	// The child's chunks are namespaced into the same event log
	if len(h.events(ran.ThreadID, "SUBAGENT_CHUNK")) == 0 {
		t.Fatal("no SUBAGENT_CHUNK")
	}
	// A nested run is a run (§2.9)
	rec, _ := h.admin.Runs().Get(h.ctx, childID)
	mustEqual(t, rec.Depth, 1, "depth")
	mustEqual(t, rec.ParentRunID, ran.RunID, "parent")
	mustEqual(t, rec.State, agentenkit.StateCompleted, "child state")
	mustEqual(t, rec.Steps, 1, "child steps")
	mustEqual(t, rec.TotalTokens, 30, "child tokens")
	mustEqual(t, rec.Prompt, "Find the answer and report back.", "brief recorded")
	// Its steps belong to its own record, not its parent's
	childSteps, _ := h.admin.Steps().ListByRun(h.ctx, childID)
	parentSteps, _ := h.admin.Steps().ListByRun(h.ctx, ran.RunID)
	mustEqual(t, len(childSteps), 1, "child step rows")
	mustEqual(t, len(parentSteps), 2, "parent step rows")
	// Billing attribution per subagent (§4)
	rows := h.storage.UsageRows()
	mustEqual(t, len(rows), 2, "usage rows")
	mustEqual(t, rows[0].AgentID, childID, "child usage first")
	mustEqual(t, rows[0].TotalTokens, 30, "child usage")
	// The snapshot carries nested runs
	snap, _ := h.rt.GetThreadSnapshot(h.ctx, ran.ThreadID, nil)
	mustEqual(t, len(snap.Runs), 2, "runs in snapshot")
}

type nestedParkHarness struct {
	*harness
	chat     *agentenkit.AgentHandle
	executed *[]string
}

func nestedParkSetup(t *testing.T, extra ...step) *nestedParkHarness {
	executed := &[]string{}
	steps := []step{
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},  // parent spawns
		step{calls: []call{{"d1", "wipe", `{"target":"prod"}`}}}, // child asks for a destructive tool
	}
	steps = append(steps, extra...)
	h := makeRuntime(t, scripted(steps...))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Subagents: &agentenkit.SubagentsConfig{
			Tools: []agentenkit.Tool{agentenkit.MarkRequiresConfirmation(tool("wipe", func(_ context.Context, args map[string]any) (string, error) {
				*executed = append(*executed, args["target"].(string))
				return `{"wiped":true}`, nil
			}))},
		},
	})
	return &nestedParkHarness{harness: h, chat: chat, executed: executed}
}

func TestNested_AParkSuspendsTheWholeThreadAndRecordsTheChain(t *testing.T) {
	h := nestedParkSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)

	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "state")
	mustEqual(t, len(*h.executed), 0, "tool never ran")
	req := payload(h.events(ran.ThreadID, "INPUT_REQUIRED")[0])
	childID := payload(h.events(ran.ThreadID, "SUBAGENT_STARTED")[0])["agentId"].(string)
	mustEqual(t, req["agentId"], childID, "the child asked")
	mustEqual(t, req["toolName"], "wipe", "toolName")
	frames := req["frames"].([]any)
	mustEqual(t, len(frames), 1, "one waiting call")
	frame := frames[0].(map[string]any)
	mustEqual(t, frame["agentId"], nil, "the parent's stream is waiting")
	mustEqual(t, frame["toolCallId"], "s1", "the spawn call is waiting")
	mustEqual(t, req["nested"].(map[string]any)["agentId"], childID, "descriptor")
	// The child's record stays RUNNING: suspended, not finished
	rec, _ := h.admin.Runs().Get(h.ctx, childID)
	mustEqual(t, rec.State, agentenkit.StateRunning, "child state")
	// Neither stream carries the sentinel
	childMsgs, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.AgentScope(childID), agentenkit.StorageContext{})
	mustEqual(t, len(childMsgs), 2, "child: brief + tool call")
	parentMsgs, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	mustEqual(t, len(parentMsgs), 2, "parent: prompt + spawn call")
}

func TestNested_ApprovingReEntersTheChildAndUnwindsToTheParent(t *testing.T) {
	h := nestedParkSetup(t,
		step{text: "child: wiped, done"}, // the child, re-entered after the verdict
		step{text: "parent: all done"},   // the parent, handed the child's result
	)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	h.queue.Shift() // expiry job
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "d1", Approved: true}); err != nil {
		t.Fatal(err)
	}
	h.handleNext(t)

	mustStrings(t, *h.executed, []string{"prod"}, "the approved tool ran, in the child's stream")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
	childID := payload(h.events(ran.ThreadID, "SUBAGENT_STARTED")[0])["agentId"].(string)
	child, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.AgentScope(childID), agentenkit.StorageContext{})
	var childRoles []string
	for _, m := range child {
		childRoles = append(childRoles, string(m.Role))
	}
	mustStrings(t, childRoles, []string{"user", "assistant", "tool", "assistant"}, "child re-entered where it stopped")
	parent, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	var parentRoles []string
	for _, m := range parent {
		parentRoles = append(parentRoles, string(m.Role))
	}
	mustStrings(t, parentRoles, []string{"user", "assistant", "tool", "assistant"}, "parent got the spawn result")
	spawnResult := agentenkit.ParseContent(parent[2].Content)[0]
	mustEqual(t, spawnResult.ToolCallID, "s1", "answered the waiting spawn call")
	if !strings.Contains(string(spawnResult.Result), "child: wiped, done") {
		t.Fatalf("spawn result: %s", spawnResult.Result)
	}
	rec, _ := h.admin.Runs().Get(h.ctx, childID)
	mustEqual(t, rec.State, agentenkit.StateCompleted, "child closed")
	mustEqual(t, h.model.Calls(), 4, "model calls")
}

func TestNested_ADenialUnwindsWithoutRunningTheTool(t *testing.T) {
	h := nestedParkSetup(t, step{text: "child: could not"}, step{text: "parent: ok"})
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	h.queue.Shift()
	if _, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "d1", Approved: false}); err != nil {
		t.Fatal(err)
	}
	h.handleNext(t)
	mustEqual(t, len(*h.executed), 0, "tool never ran")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
}

func TestNested_ARedeliveryWhileUnansweredLeavesTheThreadParked(t *testing.T) {
	h := nestedParkSetup(t)
	ran := h.run(t, h.chat, agentenkit.RunInput{Prompt: "go"})
	job := h.handleNext(t)
	if _, err := h.rt.Worker.HandleJob(h.ctx, job); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateWaitingForInput, "state")
	mustEqual(t, h.model.Calls(), 2, "no extra model calls")
}

func TestNested_TheDepthCapTellsTheModelWhy(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},
		step{calls: []call{{"s2", "spawnSubagent", `{"name":"deeper","instructions":"go deeper"}`}}}, // the child tries
		step{text: "child gave up"},
		step{text: "parent done"},
	), func(c *agentenkit.AgentConfig) { c.SubagentMaxDepth = 1 })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Subagents: &agentenkit.SubagentsConfig{}})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
	mustEqual(t, len(h.events(ran.ThreadID, "SUBAGENT_STARTED")), 1, "only one child started")
	childID := payload(h.events(ran.ThreadID, "SUBAGENT_STARTED")[0])["agentId"].(string)
	child, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.AgentScope(childID), agentenkit.StorageContext{})
	refusal := agentenkit.ParseContent(child[2].Content)[0]
	if !strings.Contains(string(refusal.Result), "Max subagent depth (1) reached") {
		t.Fatalf("refusal: %s", refusal.Result)
	}
}

func TestNested_AFailingChildIsReportedToTheParentNotThrown(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},
		step{err: errBoom},
		step{text: "parent recovered"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Subagents: &agentenkit.SubagentsConfig{}})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "the run survived")
	failed := h.events(ran.ThreadID, "SUBAGENT_FAILED")
	mustEqual(t, len(failed), 1, "SUBAGENT_FAILED")
	mustEqual(t, payload(failed[0])["state"], "FAILED", "state")
	if !strings.Contains(payload(failed[0])["error"].(string), "boom") {
		t.Fatalf("error: %v", payload(failed[0])["error"])
	}
	childID := payload(failed[0])["agentId"].(string)
	rec, _ := h.admin.Runs().Get(h.ctx, childID)
	mustEqual(t, rec.State, agentenkit.StateFailed, "child record")
	parent, _ := h.storage.Messages().List(h.ctx, ran.ThreadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	result := agentenkit.ParseContent(parent[2].Content)[0]
	if !strings.Contains(string(result.Result), `"error"`) {
		t.Fatalf("parent tool result: %s", result.Result)
	}
}

func TestNested_AnInventedModelNameFallsBackToTheParents(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", `{"name":"x","instructions":"y","model":"unknown-model"}`}}},
		step{text: "child ok"},
		step{text: "parent ok"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o", Subagents: &agentenkit.SubagentsConfig{}})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
	mustEqual(t, len(h.events(ran.ThreadID, "SUBAGENT_COMPLETED")), 1, "child completed")
}

func TestNested_TheChildsSpendCanStopTheRun(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}, usage: &[2]int{10, 5}},
		step{text: "expensive child", usage: &[2]int{80, 20}},
		step{text: "never"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Subagents: &agentenkit.SubagentsConfig{}, TokenBudget: 100})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	mustEqual(t, h.model.Calls(), 2, "the parent never got a third step")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["stopReason"], "token_budget", "stopReason")
	mustEqual(t, h.lastTerminal(ran.ThreadID)["tokensUsed"], float64(115), "tokensUsed")
}
