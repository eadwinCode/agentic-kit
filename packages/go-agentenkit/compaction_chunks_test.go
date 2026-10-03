package agentenkit_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// tinySummarizer records every summary prompt it is sent.
type tinySummarizer struct {
	summarizingModel
	mu      sync.Mutex
	prompts []string
}

func (m *tinySummarizer) DoGenerate(_ context.Context, p provider.GenerateParams) (*provider.GenerateResult, error) {
	m.mu.Lock()
	m.prompts = append(m.prompts, string(promptJSON(p.Messages)))
	m.mu.Unlock()
	return &provider.GenerateResult{Text: "part summary", FinishReason: provider.FinishStop,
		Usage: provider.Usage{InputTokens: 100, OutputTokens: 10}}, nil
}

// A history larger than the summarizer's window is summarized in parts that
// fit it, then merged, instead of one call the summarizer would refuse.
func TestCompaction_HistoryLargerThanTheSummarizerIsChunked(t *testing.T) {
	chatModel := &summarizingModel{}
	summarizer := &tinySummarizer{}
	h := makeRuntimeOpts(t, scripted(step{text: "unused"}), func(o *agentenkit.RuntimeOptions) {
		o.ResolveModel = func(name string) (agentenkit.ResolvedModel, error) {
			if name == "tiny" {
				return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return summarizer }, ContextWindow: 2_000}, nil
			}
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return chatModel }, ContextWindow: 128_000}, nil
		}
	}, func(c *agentenkit.AgentConfig) {
		c.ContextCeilingTokens = 100_000 // no automatic compaction
		c.CompactionModel = "tiny"
		c.PromptCaching = false
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	first := runPrompt(t, h, chat, "", 1)
	for i := 2; i <= 8; i++ {
		runPrompt(t, h, chat, first.ThreadID, i)
	}
	result, err := h.rt.CompactThread(h.ctx, first.ThreadID, nil)
	if err != nil || !result.Compacted {
		t.Fatalf("result = %+v err=%v", result, err)
	}
	if len(summarizer.prompts) < 3 {
		t.Fatalf("summary calls = %d, want parts plus a merge", len(summarizer.prompts))
	}
	for i, p := range summarizer.prompts {
		if len(p) > 1_000*4+1_000 { // a chunk is half the 2,000 window, plus the instruction
			t.Fatalf("summary call %d is %d chars", i, len(p))
		}
	}
	if last := summarizer.prompts[len(summarizer.prompts)-1]; !strings.Contains(last, "Merge them") {
		t.Fatal("the last call merges the parts")
	}
	mustEqual(t, summaries(t, h, first.ThreadID), 1, "one summary written")
}
