package agentenkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
)

// A thread well under the trigger still compacts when someone asks.
func TestCompaction_CompactThreadForcesASummary(t *testing.T) {
	h, chat, _ := compactingRuntime(t)
	first := runPrompt(t, h, chat, "", 1)
	runPrompt(t, h, chat, first.ThreadID, 2)
	runPrompt(t, h, chat, first.ThreadID, 3)
	mustEqual(t, summaries(t, h, first.ThreadID), 0, "under the trigger")
	result, err := h.rt.CompactThread(h.ctx, first.ThreadID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Compacted {
		t.Fatalf("result = %+v", result)
	}
	mustEqual(t, summaries(t, h, first.ThreadID), 1, "one summary written")
	again, err := h.rt.CompactThread(h.ctx, "missing", nil)
	if err != nil || again.Compacted || again.Reason == "" {
		t.Fatalf("missing thread = %+v err=%v", again, err)
	}
}

// overflowingModel refuses its first streamed call as too long, then answers.
type overflowingModel struct {
	summarizingModel
	mu      sync.Mutex
	refused bool
}

func (m *overflowingModel) DoStream(ctx context.Context, p provider.GenerateParams) (*provider.StreamResult, error) {
	m.mu.Lock()
	refuse := !m.refused && strings.Contains(string(promptJSON(p.Messages)), "run-3 ")
	if refuse {
		m.refused = true
	}
	m.mu.Unlock()
	if refuse {
		return nil, errors.New("prompt is too long: 224705 tokens > 200000 maximum")
	}
	return m.summarizingModel.DoStream(ctx, p)
}

func promptJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// The provider refusing a prompt as too long compacts the thread and the
// run goes on, instead of failing every retry the same way.
func TestCompaction_APromptTooLongCompactsAndRetries(t *testing.T) {
	model := &overflowingModel{}
	h := makeRuntimeOpts(t, scripted(step{text: "unused"}), func(o *agentenkit.RuntimeOptions) {
		o.ResolveModel = func(string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		}
	}, func(c *agentenkit.AgentConfig) {
		c.ContextCeilingTokens = 100_000 // far from the trigger: only the refusal compacts
		c.CompactionModel = "gpt-4o"
		c.PromptCaching = false
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	first := runPrompt(t, h, chat, "", 1)
	runPrompt(t, h, chat, first.ThreadID, 2)
	third := runPrompt(t, h, chat, first.ThreadID, 3)
	if !model.refused {
		t.Fatal("the model never refused")
	}
	mustEqual(t, summaries(t, h, first.ThreadID), 1, "the refusal compacted the thread")
	if !strings.Contains(model.lastPrompt(), "run-3 ") || strings.Contains(model.lastPrompt(), "run-1 ") {
		t.Fatal("the retry sends the summary and the new turn")
	}
	_ = third
}

func TestIsContextOverflow(t *testing.T) {
	for _, message := range []string{
		"prompt is too long: 224705 tokens > 200000 maximum",
		"This model's maximum context length is 128000 tokens",
		"context_length_exceeded",
	} {
		if !core.IsContextOverflow(errors.New(message)) {
			t.Fatalf("%q not matched", message)
		}
	}
	if core.IsContextOverflow(errors.New("rate limited")) || core.IsContextOverflow(nil) {
		t.Fatal("false positive")
	}
}
