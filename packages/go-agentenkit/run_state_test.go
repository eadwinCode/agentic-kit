package agentenkit_test

import (
	"context"
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

func TestRunState_ReachesStorageToolsNestedRunsAndTheTicket(t *testing.T) {
	var toolState, childToolState agentenkit.AgentRunState
	h := makeRuntime(t, scripted(
		step{calls: []call{{"c1", "lookup", `{}`}}},
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},
		step{calls: []call{{"c2", "childLookup", `{}`}}}, // the child
		step{text: "child done"},
		step{text: "parent done"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		Tools: []agentenkit.Tool{agentenkit.AgentTool("lookup", "l", func(_ context.Context, _ struct{}, tc agentenkit.ToolContext) (string, error) {
			toolState = tc.State
			return "ok", nil
		})},
		Subagents: &agentenkit.SubagentsConfig{
			Tools: []agentenkit.Tool{agentenkit.AgentTool("childLookup", "c", func(_ context.Context, _ struct{}, tc agentenkit.ToolContext) (string, error) {
				childToolState = tc.State
				return "ok", nil
			})},
		},
	})
	state := agentenkit.AgentRunState{"orgId": "acme", "userId": "u1"}
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go", State: state})

	// On the ticket
	mustEqual(t, h.queue.Items()[0].State["orgId"], "acme", "job state")
	// On the run record
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.RunState["orgId"], "acme", "recorded state")
	// On every storage call so far
	for _, sc := range h.storage.Contexts {
		if sc.State["orgId"] != "acme" {
			t.Fatalf("a storage call missed the state: %+v", sc)
		}
	}
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["state"], "COMPLETED", "state")
	mustEqual(t, toolState["orgId"], "acme", "tool state")
	mustEqual(t, toolState["userId"], "u1", "tool state")
	mustEqual(t, childToolState["orgId"], "acme", "nested tool state")
	// The worker's storage calls carry the run id too
	var withRun int
	for _, sc := range h.storage.Contexts {
		if sc.RunID == ran.RunID {
			withRun++
		}
	}
	if withRun == 0 {
		t.Fatal("no storage call carried the run id")
	}
	// Reads scope too
	_, _ = h.rt.ListThreads(h.ctx, agentenkit.AgentRunState{"orgId": "other"})
	mustEqual(t, h.storage.LastContext.State["orgId"], "other", "read scope")
}

func TestRunState_IsNeverNilInsideATool(t *testing.T) {
	var got agentenkit.AgentRunState
	h := makeRuntime(t, scripted(step{calls: []call{{"c1", "probe", `{}`}}}, step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat",
		Tools: []agentenkit.Tool{agentenkit.WrapTool(tool("probe", func(ctx context.Context, _ map[string]any) (string, error) {
			got = agentenkit.RunStateFromContext(ctx)
			return "ok", nil
		}).Tool)},
	})
	h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	if got == nil {
		t.Fatal("state must be an empty map, never nil")
	}
}
