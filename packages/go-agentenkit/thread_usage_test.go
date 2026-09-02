package agentenkit_test

import (
	"context"
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

func TestThreadUsage_SumsSpendAndMeasuresTheContext(t *testing.T) {
	h := makeRuntime(t, scripted(step{calls: []call{{"c", "probe", `{}`}}}, step{text: "done"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name: "chat", Model: "gpt-4o",
		Tools: []agentenkit.Tool{tool("probe", func(_ context.Context, _ map[string]any) (string, error) { return "ok", nil })},
	})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	u, err := h.rt.GetThreadUsage(h.ctx, ran.ThreadID, nil)
	if err != nil || u == nil {
		t.Fatalf("usage: %v %v", u, err)
	}
	mustEqual(t, u.Tokens.TotalTokens, 30, "total")
	mustEqual(t, u.Tokens.InputTokens, 20, "input")
	mustEqual(t, u.Model, "gpt-4o", "model")
	mustEqual(t, u.Context.Messages, 4, "messages in context")
	mustEqual(t, u.Context.BudgetTokens, 128_000-16_000, "budget")
	mustEqual(t, u.Context.CompactAtTokens, int(float64(112_000)*0.8), "compact at")
	if u.Context.UsedTokens <= 0 {
		t.Fatal("used tokens must be estimated")
	}
	missing, err := h.rt.GetThreadUsage(h.ctx, "nope", nil)
	if err != nil || missing != nil {
		t.Fatal("unknown thread must be nil")
	}
}
