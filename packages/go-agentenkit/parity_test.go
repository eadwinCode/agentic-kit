package agentenkit_test

import (
	"context"
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream K: the two runtimes read config, budgets and ports the same
// way. The same cases run in the TS package (test/parity.test.ts).

func TestConfig_APartialConfigKeepsTheDefaults(t *testing.T) {
	config, err := ports.ResolveConfig(&agentenkit.AgentConfig{MaxSteps: 50})
	if err != nil {
		t.Fatalf("a partial config is merged, not zeroed: %v", err)
	}
	def := agentenkit.DefaultConfig()
	mustEqual(t, config.MaxSteps, 50, "the given field")
	mustEqual(t, config.StopPoll, def.StopPoll, "a poll left out keeps the default")
	mustEqual(t, config.RunLockLease, def.RunLockLease, "so does the lease")
	mustEqual(t, config.CompactionModel, def.CompactionModel, "and the compaction model")
}

func TestBudget_ABudgetOfZeroMeansNoCapFromThisLevel(t *testing.T) {
	looping := step{calls: []call{{"c1", "ping", `{}`}}}
	h := makeRuntime(t, scripted(looping, looping, step{text: "done"}))
	// The spec's budget stands under a run that sends 0.
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", TokenBudget: 20,
		Tools: []agentenkit.Tool{tool("ping", func(context.Context, map[string]any) (string, error) { return "pong", nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go", TokenBudget: 0})
	h.handleNext(t)
	mustEqual(t, h.lastTerminal(ran.ThreadID)["stopReason"], "token_budget", "the spec's cap applied")
	if err := core.ValidateTokenBudget(-1, "tokenBudget"); err == nil {
		t.Fatal("a negative budget is refused rather than read as no cap")
	}
}

func TestRuntime_ThePortsAreScopedToARunsState(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	scoped := h.rt.Ports(agentenkit.AgentRunState{"tenant": "acme"})
	mustEqual(t, scoped.Config.MaxSteps, agentenkit.DefaultConfig().MaxSteps, "the runtime's config")
	thread, err := scoped.Storage.Threads.Create(h.ctx, ports.ThreadInit{})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := h.rt.Ports(nil).Storage.Threads.Get(h.ctx, thread.ID)
	if got == nil {
		t.Fatal("the same storage behind it")
	}
}
