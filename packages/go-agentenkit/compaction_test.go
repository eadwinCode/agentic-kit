package agentenkit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream E: a summary covers what it summarized, so the prompt stops
// carrying it, a thread compacts once per time it outgrows the trigger, and
// the kept tail starts at a user turn. The same cases run in the TS package
// (test/compaction.test.ts).

// summarizingModel answers every streamed call "ok" and every summary
// request "summary", and keeps the prompts it streamed.
type summarizingModel struct {
	mu      sync.Mutex
	prompts [][]provider.Message
}

func (m *summarizingModel) ModelID() string { return "mock-summarizing" }

func (m *summarizingModel) DoGenerate(context.Context, provider.GenerateParams) (*provider.GenerateResult, error) {
	return &provider.GenerateResult{Text: "summary", FinishReason: provider.FinishStop,
		Usage: provider.Usage{InputTokens: 100, OutputTokens: 10}}, nil
}

func (m *summarizingModel) DoStream(_ context.Context, p provider.GenerateParams) (*provider.StreamResult, error) {
	m.mu.Lock()
	m.prompts = append(m.prompts, p.Messages)
	m.mu.Unlock()
	ch := make(chan provider.StreamChunk, 2)
	ch <- provider.StreamChunk{Type: provider.ChunkText, Text: "ok"}
	ch <- provider.StreamChunk{Type: provider.ChunkFinish, FinishReason: provider.FinishStop, Usage: provider.Usage{InputTokens: 10, OutputTokens: 5}}
	close(ch)
	return &provider.StreamResult{Stream: ch}, nil
}

func (m *summarizingModel) lastPrompt() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, _ := json.Marshal(m.prompts[len(m.prompts)-1])
	return string(b)
}

// compactingRuntime: a 1,800-token budget, compacting past 1,440, keeping a
// 450-token tail. Each prompt below is about 300 tokens.
func compactingRuntime(t *testing.T) (*harness, *agentenkit.AgentHandle, *summarizingModel) {
	model := &summarizingModel{}
	h := makeRuntimeOpts(t, scripted(step{text: "unused"}), func(o *agentenkit.RuntimeOptions) {
		o.ResolveModel = func(string) (agentenkit.ResolvedModel, error) {
			return agentenkit.ResolvedModel{Instance: func() provider.LanguageModel { return model }, ContextWindow: 128_000}, nil
		}
	}, func(c *agentenkit.AgentConfig) {
		c.ContextCeilingTokens = 2_000
		c.ContextOutputReserveTokens = 200
		c.CompactionModel = "gpt-4o"
		c.PromptCaching = false
	})
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
	return h, chat, model
}

func runPrompt(t *testing.T, h *harness, chat *agentenkit.AgentHandle, threadID string, i int) agentenkit.RunResult {
	t.Helper()
	ran := h.run(t, chat, agentenkit.RunInput{ThreadID: threadID, Prompt: fmt.Sprintf("run-%d %s", i, strings.Repeat("x", 1_200))})
	h.handleNext(t)
	return ran
}

func summaries(t *testing.T, h *harness, threadID string) int {
	t.Helper()
	msgs, _ := h.storage.Messages().List(h.ctx, threadID, agentenkit.MainAgent, agentenkit.StorageContext{})
	n := 0
	for _, m := range msgs {
		if strings.Contains(string(m.Content), core.ContextSummaryType) {
			n++
		}
	}
	return n
}

func TestCompaction_AThreadCompactsOnceEachTimeItGrowsPastTheTrigger(t *testing.T) {
	h, chat, model := compactingRuntime(t)
	first := runPrompt(t, h, chat, "", 1)
	for i := 2; i <= 6; i++ {
		runPrompt(t, h, chat, first.ThreadID, i)
	}
	mustEqual(t, summaries(t, h, first.ThreadID), 1, "compacted once, not on every run after")
	if strings.Contains(model.lastPrompt(), "run-1 ") {
		t.Fatal("a summarized turn is not sent again")
	}
	if !strings.Contains(model.lastPrompt(), "run-6 ") {
		t.Fatal("the new turn is sent")
	}
}

func TestCompaction_TheSummaryIsBilledToTheRunItServed(t *testing.T) {
	h, chat, _ := compactingRuntime(t)
	first := runPrompt(t, h, chat, "", 1)
	var compacting agentenkit.RunResult
	for i := 2; i <= 5; i++ {
		compacting = runPrompt(t, h, chat, first.ThreadID, i)
	}
	for _, r := range h.storage.UsageRows() {
		if r.Kind == agentenkit.KindCompaction {
			mustEqual(t, r.RunID, compacting.RunID, "billed to the run it served")
			return
		}
	}
	t.Fatal("no compaction call recorded")
}

func TestCompaction_TheKeptTailStartsAtAUserTurn(t *testing.T) {
	h, _, _ := compactingRuntime(t)
	deps := h.rt.Ports(nil)
	th, _ := h.storage.Threads().Create(h.ctx, ports.ThreadInit{}, agentenkit.StorageContext{})
	add := func(role ports.MessageRole, content json.RawMessage) {
		if _, err := h.storage.Messages().Append(h.ctx, th.ID, ports.NewMessage{Role: role, Content: content}, agentenkit.StorageContext{}); err != nil {
			t.Fatal(err)
		}
	}
	big := strings.Repeat("y", 2_400)
	add(ports.RoleUser, core.TextContent("first "+big))
	add(ports.RoleUser, core.TextContent("second "+big))
	add(ports.RoleAssistant, core.PartsContent([]core.ContentPart{{Type: "tool-call", ToolCallID: "c1", ToolName: "lookup", Args: json.RawMessage(`{"q":"` + strings.Repeat("q", 300) + `"}`)}}))
	// A result that fits the tail on its own, with its call just outside it.
	add(ports.RoleTool, core.ToolResultContent("c1", "lookup", strings.Repeat("z", 1_300)))
	add(ports.RoleAssistant, core.TextContent("looked it up"))
	add(ports.RoleUser, core.TextContent("third"))
	out, err := agentenkit.CompactContext(h.ctx, deps, th.ID, "gpt-4o", core.CompactOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, out[0].Role, ports.RoleSystem, "the summary leads")
	mustEqual(t, out[1].Role, ports.RoleUser, "the tail starts at a user turn")
	mustEqual(t, len(out), 2, "summary + the last user turn")
}

func TestCompaction_ASummaryFromBeforeTheCoverMarkIsLeftOut(t *testing.T) {
	msg := func(id string, role ports.MessageRole, content json.RawMessage) ports.MessageDTO {
		return ports.MessageDTO{ID: id, Role: role, Content: content}
	}
	u1, a1, u2 := msg("u1", ports.RoleUser, core.TextContent("one")), msg("a1", ports.RoleAssistant, core.TextContent("two")), msg("u2", ports.RoleUser, core.TextContent("three"))
	legacy := msg("s0", ports.RoleSystem, json.RawMessage(`{"type":"CONTEXT_SUMMARY","text":"old"}`))
	marked := msg("s1", ports.RoleSystem, core.ContextSummaryContent("new", "a1"))

	ids := func(ms []ports.MessageDTO) []string {
		var out []string
		for _, m := range ms {
			out = append(out, m.ID)
		}
		return out
	}
	mustStrings(t, ids(core.PromptHistory([]ports.MessageDTO{u1, a1, legacy, u2})), []string{"u1", "a1", "u2"}, "a legacy summary covers nothing known")
	mustStrings(t, ids(core.PromptHistory([]ports.MessageDTO{u1, a1, u2, marked})), []string{"s1", "u2"}, "the summary, then what came after what it covers")
}
