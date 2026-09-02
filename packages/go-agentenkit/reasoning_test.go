package agentenkit_test

import (
	"testing"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// Anthropic extended thinking must be replayed WITH its signature inside a
// tool loop, so the stored shape keeps it; the React hook reads the same
// `reasoning` parts the TypeScript package stores.
func TestReasoning_RoundTripsWithItsSignature(t *testing.T) {
	msg := provider.Message{Role: provider.RoleAssistant, Content: []provider.Part{
		{Type: provider.PartReasoning, Text: "let me think", ProviderOptions: map[string]any{"signature": "sig_abc"}},
		{Type: provider.PartReasoning, ProviderOptions: map[string]any{"redactedData": "enc_xyz"}},
		{Type: provider.PartText, Text: "answer"},
	}}
	content := agentenkit.ContentFromMessage(msg)
	parts := agentenkit.ParseContent(content)
	mustEqual(t, len(parts), 3, "parts")
	mustEqual(t, parts[0].Type, "reasoning", "reasoning part")
	mustEqual(t, parts[0].Text, "let me think", "reasoning text")
	mustEqual(t, parts[0].Signature, "sig_abc", "signature kept")
	mustEqual(t, parts[1].Type, "redacted-reasoning", "redacted part")
	mustEqual(t, parts[1].Data, "enc_xyz", "redacted data kept")

	back := agentenkit.MessageFromDTO(agentenkit.MessageDTO{Role: agentenkit.RoleAssistant, Content: content})
	mustEqual(t, len(back.Content), 3, "replayed parts")
	mustEqual(t, back.Content[0].Type, provider.PartReasoning, "reasoning replayed")
	mustEqual(t, back.Content[0].ProviderOptions["signature"], "sig_abc", "signature replayed")
	mustEqual(t, back.Content[1].ProviderOptions["redactedData"], "enc_xyz", "redacted replayed")
	mustEqual(t, back.Content[2].Text, "answer", "text replayed")
}

// A streamed reasoning chunk reaches the client as a `reasoning` CHUNK, the
// same shape the TypeScript package publishes, and the step persists it.
func TestReasoning_StreamsAndPersists(t *testing.T) {
	h := makeRuntime(t, scripted(step{reasoning: "thinking hard", text: "42"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "q"})
	h.handleNext(t)
	var kinds []string
	for _, e := range h.events(ran.ThreadID, "CHUNK") {
		kinds = append(kinds, payload(e)["type"].(string))
	}
	mustStrings(t, kinds[:2], []string{"reasoning", "text-delta"}, "chunk order")
	mustEqual(t, payload(h.events(ran.ThreadID, "CHUNK")[0])["textDelta"], "thinking hard", "reasoning delta")
	parts := agentenkit.ParseContent(h.storage.MessageRows(ran.ThreadID)[1].Content)
	mustEqual(t, parts[0].Type, "reasoning", "persisted reasoning")
	mustEqual(t, parts[0].Text, "thinking hard", "persisted text")
	mustEqual(t, parts[1].Text, "42", "persisted answer")
}
