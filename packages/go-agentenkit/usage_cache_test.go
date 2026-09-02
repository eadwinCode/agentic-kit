package agentenkit_test

import (
	"testing"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

func TestAttributeTokens(t *testing.T) {
	a := agentenkit.AttributeTokens(provider.Usage{InputTokens: 100, CacheReadTokens: 40, OutputTokens: 10, TotalTokens: 150})
	mustEqual(t, a.InputTokens, 100, "input")
	mustEqual(t, a.CachedInputTokens, 40, "cached")
	mustEqual(t, a.OutputTokens, 10, "output")
	mustEqual(t, a.TotalTokens, 150, "total is input + cached + output")
	mustEqual(t, agentenkit.CountTokens(provider.Usage{InputTokens: -5, OutputTokens: 3}), 3, "negatives are guarded")
}

func TestMarkPromptCaching_StampsTheSystemAndTheTail(t *testing.T) {
	msgs := []provider.Message{
		{Role: provider.RoleSystem, Content: []provider.Part{{Type: provider.PartText, Text: "persona"}}},
		{Role: provider.RoleUser, Content: []provider.Part{{Type: provider.PartText, Text: "a"}}},
		{Role: provider.RoleAssistant, Content: []provider.Part{{Type: provider.PartText, Text: "b"}, {Type: provider.PartText, Text: "c"}}},
	}
	out := agentenkit.MarkPromptCaching(msgs)
	mustEqual(t, out[0].Content[0].CacheControl, "ephemeral", "system stamped")
	mustEqual(t, out[1].Content[0].CacheControl, "", "middle untouched")
	mustEqual(t, out[2].Content[0].CacheControl, "", "only the last part")
	mustEqual(t, out[2].Content[1].CacheControl, "ephemeral", "tail stamped")
	mustEqual(t, msgs[0].Content[0].CacheControl, "", "input not mutated")
}

func TestPromptCaching_HoistsTheSystemPromptIntoAStampedMessage(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", System: "You are terse."})
	h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	p := h.model.Params()[0]
	mustEqual(t, p.System, "", "not sent as the bare system string")
	mustEqual(t, p.Messages[0].Role, provider.RoleSystem, "hoisted first")
	mustEqual(t, p.Messages[0].Content[0].Text, "You are terse.", "text")
	mustEqual(t, p.Messages[0].Content[0].CacheControl, "ephemeral", "carries the breakpoint")
	last := p.Messages[len(p.Messages)-1]
	mustEqual(t, last.Content[len(last.Content)-1].CacheControl, "ephemeral", "tail stamped")
}

func TestPromptCaching_OffSendsThePlainSystemString(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}), func(c *agentenkit.AgentConfig) { c.PromptCaching = false })
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", System: "You are terse."})
	h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
	h.handleNext(t)
	p := h.model.Params()[0]
	mustEqual(t, p.System, "You are terse.", "system string")
	for _, m := range p.Messages {
		for _, part := range m.Content {
			mustEqual(t, part.CacheControl, "", "no stamps")
		}
	}
}

func TestNested_CachesTheChildsPromptToo(t *testing.T) {
	h := makeRuntime(t, scripted(
		step{calls: []call{{"s1", "spawnSubagent", spawnArgs}}},
		step{text: "child"},
		step{text: "parent"},
	))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Subagents: &agentenkit.SubagentsConfig{}})
	h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	child := h.model.Params()[1]
	mustEqual(t, child.Messages[0].Role, provider.RoleSystem, "child persona hoisted")
	mustEqual(t, child.Messages[0].Content[0].CacheControl, "ephemeral", "persona stamped")
	mustEqual(t, child.Messages[0].Content[0].Text, `You are the "researcher" subagent. Complete the task, then stop.`, "persona")
	tail := child.Messages[len(child.Messages)-1]
	mustEqual(t, tail.Content[len(tail.Content)-1].CacheControl, "ephemeral", "child tail stamped")
}
