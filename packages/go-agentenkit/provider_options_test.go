package agentenkit_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// A provider's own data on a part (Gemini's thought signature above all)
// must survive a save and a load: every run, resume and redrive reads the
// thread back from storage before the next model call, and Gemini rejects a
// tool call from the current turn that comes back without its signature.
// The same cases run in the TS package (test/ai-sdk-v7.test.ts).

func TestStoredProviderOptions_AToolCallKeepsItsThoughtSignature(t *testing.T) {
	sig := map[string]any{"google": map[string]any{"thoughtSignature": "sig"}}
	msg := provider.Message{Role: provider.RoleAssistant, Content: []provider.Part{
		{Type: provider.PartText, Text: "let me look"},
		{Type: provider.PartToolCall, ToolCallID: "c1", ToolName: "lookup", ToolInput: json.RawMessage(`{"q":"x"}`), ProviderOptions: sig},
	}}
	content := agentenkit.ContentFromMessage(msg)
	parts := agentenkit.ParseContent(content)
	mustEqual(t, parts[0].ProviderOptions == nil, true, "nothing stored on a part that had none")
	mustEqual(t, jsonOf(t, parts[1].ProviderOptions), `{"google":{"thoughtSignature":"sig"}}`, "stored on the call")

	back := agentenkit.MessageFromDTO(agentenkit.MessageDTO{Role: agentenkit.RoleAssistant, Content: content})
	mustEqual(t, len(back.Content), 2, "parts")
	mustEqual(t, back.Content[0].ProviderOptions == nil, true, "none back on the text")
	google, _ := back.Content[1].ProviderOptions["google"].(map[string]any)
	mustEqual(t, google["thoughtSignature"], "sig", "signature back on the call")
	mustEqual(t, back.Content[1].ToolCallID, "c1", "the call itself")
}

func TestStoredProviderOptions_OnlyProviderKeyedEntriesAreStored(t *testing.T) {
	// goai keeps Anthropic's signature as a plain top-level value; that has
	// its own field. Only provider-keyed objects go under providerOptions,
	// the shape the AI SDK reads.
	msg := provider.Message{Role: provider.RoleAssistant, Content: []provider.Part{
		{Type: provider.PartReasoning, Text: "hm", ProviderOptions: map[string]any{
			"signature":  "sig_abc",
			"openrouter": map[string]any{"reasoning_details": []any{map[string]any{"type": "reasoning.text", "text": "hm"}}},
			"broken":     map[string]any{"ch": make(chan int)},
		}},
		{Type: provider.PartToolCall, ToolCallID: "ws1", ToolName: "web_search", ProviderOptions: map[string]any{
			"resultBlock": map[string]any{"type": "web_search_tool_result"},
		}},
	}}
	parts := agentenkit.ParseContent(agentenkit.ContentFromMessage(msg))
	mustEqual(t, len(parts), 2, "the row is still written")
	mustEqual(t, parts[1].ProviderOptions == nil, true, "goai's own resultBlock is not stored")
	mustEqual(t, parts[0].Signature, "sig_abc", "signature in its own field")
	mustEqual(t, jsonOf(t, parts[0].ProviderOptions),
		`{"openrouter":{"reasoning_details":[{"text":"hm","type":"reasoning.text"}]}}`, "only the provider-keyed entry")
}

// parity/stored-parts.json: each stored part, written by either runtime,
// reaches the model call with the same provider options.
func TestStoredProviderOptions_StoredPartsReadTheSameInBothRuntimes(t *testing.T) {
	raw, err := os.ReadFile("../parity/stored-parts.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name            string          `json:"name"`
		Part            json.RawMessage `json:"part"`
		ProviderOptions map[string]any  `json:"providerOptions"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			content := json.RawMessage("[" + string(c.Part) + "]")
			back := agentenkit.MessageFromDTO(agentenkit.MessageDTO{Role: agentenkit.RoleAssistant, Content: content})
			mustEqual(t, len(back.Content), 1, "one part")
			mustEqual(t, jsonOf(t, sdkOptions(back.Content[0].ProviderOptions)), jsonOf(t, c.ProviderOptions), "provider options")
		})
	}
}

// sdkOptions reads goai part options in the AI SDK shape: the top-level
// Anthropic values goai uses go under "anthropic".
func sdkOptions(opts map[string]any) map[string]any {
	if len(opts) == 0 {
		return nil
	}
	out := map[string]any{}
	anthropic := map[string]any{}
	for k, v := range opts {
		switch k {
		case "signature", "redactedData":
			anthropic[k] = v
		case "anthropic":
			// Merged, not replaced, so a key lost on load still shows.
			stored, _ := v.(map[string]any)
			for sk, sv := range stored {
				if _, set := anthropic[sk]; !set {
					anthropic[sk] = sv
				}
			}
		default:
			out[k] = v
		}
	}
	if len(anthropic) > 0 {
		out["anthropic"] = anthropic
	}
	return out
}

func jsonOf(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The case from the bug: a Gemini tool call parks for approval, and the
// resumed run reads the thread back from storage. The next model call must
// still carry the call's thought signature.
func TestStoredProviderOptions_ASignatureSurvivesAParkAndResume(t *testing.T) {
	sig := map[string]any{"google": map[string]any{"thoughtSignature": "sig_a1"}}
	h, chat, sent := approvalSetup(t,
		step{calls: []call{{"a1", "send", `{}`}}, callMeta: sig},
		step{text: "done"},
	)
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "go"})
	h.handleNext(t)
	h.queue.Shift() // the park's expiry job
	if res, err := h.rt.HITL.Respond(h.ctx, agentenkit.RespondInput{ThreadID: ran.ThreadID, ToolCallID: "a1", Approved: true}); err != nil || !res.Delivered {
		t.Fatalf("respond: %+v %v", res, err)
	}
	h.handleNext(t)
	mustEqual(t, *sent, 1, "the approved tool ran")
	mustEqual(t, h.thread(t, ran.ThreadID).State, agentenkit.StateCompleted, "completed")

	params := h.model.Params()
	mustEqual(t, len(params), 2, "two model calls")
	var found bool
	for _, m := range params[1].Messages {
		for _, p := range m.Content {
			if p.Type == provider.PartToolCall && p.ToolCallID == "a1" {
				found = true
				google, _ := p.ProviderOptions["google"].(map[string]any)
				mustEqual(t, google["thoughtSignature"], "sig_a1", "signature sent back after the resume")
			}
		}
	}
	if !found {
		t.Fatal("the resumed call's prompt has the tool call")
	}
}
