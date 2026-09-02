package agentenkit_test

import (
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

func TestThreadStart_RecordedOnFirstSightWithEveryDispatchParameter(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) {
		c.ProviderOptions = agentenkit.ProviderOptions{"openai": map[string]any{"tier": "flex"}}
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o", ProviderOptions: agentenkit.ProviderOptions{"anthropic": map[string]any{"effort": "high"}},
	})
	ran := h.run(t, chat, agentenkit.RunInput{
		Prompt: "send the quarterly report", TokenBudget: 5000,
		State:           agentenkit.AgentRunState{"orgId": "acme"},
		ProviderOptions: agentenkit.ProviderOptions{"openai": map[string]any{"tier": "priority"}},
	})
	threads, _ := h.rt.Admin.ListThreads(h.ctx, agentenkit.AdminThreadFilter{})
	sw := threads[0].StartedWith
	if sw == nil {
		t.Fatal("startedWith missing")
	}
	mustEqual(t, sw.RunID, ran.RunID, "runId")
	mustEqual(t, sw.Agent, "chat", "agent")
	mustEqual(t, sw.Model, "gpt-4o", "model")
	mustEqual(t, sw.Prompt, "send the quarterly report", "prompt")
	mustEqual(t, *sw.TokenBudget, 5000, "budget")
	mustEqual(t, sw.State["orgId"], "acme", "state")
	// The three levels merged, the input winning per namespace (§3.1)
	mustEqual(t, sw.ProviderOptions["openai"].(map[string]any)["tier"], "priority", "input wins")
	mustEqual(t, sw.ProviderOptions["anthropic"].(map[string]any)["effort"], "high", "spec namespace kept")
	if sw.At.IsZero() {
		t.Fatal("at")
	}
	mustEqual(t, threads[0].Prompt, "send the quarterly report", "prompt on the summary")
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	mustEqual(t, rec.ProviderOptions["openai"].(map[string]any)["tier"], "priority", "run record carries them too")

	// A second run on the thread never overwrites what started it
	h.handleNext(t)
	again := h.run(t, chat, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "and again"})
	threads, _ = h.rt.Admin.ListThreads(h.ctx, agentenkit.AdminThreadFilter{})
	mustEqual(t, threads[0].StartedWith.RunID, ran.RunID, "first run stays")
	mustEqual(t, threads[0].StartedWith.Prompt, "send the quarterly report", "first prompt stays")
	if again.RunID == ran.RunID {
		t.Fatal("second run must have its own id")
	}
	detail, _ := h.rt.Admin.GetThread(h.ctx, ran.ThreadID)
	mustEqual(t, detail.Thread.StartedWith.RunID, ran.RunID, "on the detail too")
}

func TestThreadStart_KeepsOnlyTheIdentityWhenPayloadsAreOff(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.RecordPayloads = false })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "private", State: agentenkit.AgentRunState{"orgId": "acme"},
		ProviderOptions: agentenkit.ProviderOptions{"openai": map[string]any{"x": 1}}})
	threads, _ := h.rt.Admin.ListThreads(h.ctx, agentenkit.AdminThreadFilter{})
	sw := threads[0].StartedWith
	mustEqual(t, sw.RunID, ran.RunID, "runId")
	mustEqual(t, sw.Prompt, "", "no prompt")
	if sw.State != nil || sw.ProviderOptions != nil || sw.TokenBudget != nil {
		t.Fatalf("payloads must be absent: %+v", sw)
	}
	rec, _ := h.admin.Runs().Get(h.ctx, ran.RunID)
	if rec.ProviderOptions != nil {
		t.Fatal("run record must not carry provider options")
	}
}
