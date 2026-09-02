package core

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// This file has no TypeScript twin. The TypeScript package stores AI SDK
// messages as they are; Go stores JSON and converts to and from goai's
// provider.Message here. The stored shapes are the AI SDK ones, so both
// runtimes read the same rows:
//
//	"hi"                                                – plain text
//	{"type":"CONTEXT_SUMMARY","text":"..."}             – compaction summary (§2.6)
//	[{"type":"text","text":"..."},
//	 {"type":"reasoning","text":"..."},
//	 {"type":"tool-call","toolCallId":"c1","toolName":"lookup","args":{...}},
//	 {"type":"tool-result","toolCallId":"c1","toolName":"lookup","result":{...}}]

// ContextSummaryType marks a compaction summary envelope (§2.6).
const ContextSummaryType = "CONTEXT_SUMMARY"

// ContentPart is one element of a stored parts array.
type ContentPart struct {
	Type       string          `json:"type"`
	Text       string          `json:"text,omitempty"`
	ToolCallID string          `json:"toolCallId,omitempty"`
	ToolName   string          `json:"toolName,omitempty"`
	Args       json.RawMessage `json:"args,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
}

// TextContent encodes plain text as stored content.
func TextContent(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

// PartsContent encodes a parts array as stored content.
func PartsContent(parts []ContentPart) json.RawMessage {
	b, _ := json.Marshal(parts)
	return b
}

// ContextSummaryContent encodes a compaction summary envelope.
func ContextSummaryContent(text string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"type": ContextSummaryType, "text": text})
	return b
}

// ToolResultContent is a tool message carrying one result, the shape the
// resumed segment appends after an approval (§2.5).
func ToolResultContent(toolCallID, toolName string, result any) json.RawMessage {
	return PartsContent([]ContentPart{{
		Type: "tool-result", ToolCallID: toolCallID, ToolName: toolName,
		Result: MarshalPayload(result),
	}})
}

// jsonOrString stores a tool's output as JSON when it is JSON, else as text.
func jsonOrString(output string) json.RawMessage {
	trimmed := strings.TrimSpace(output)
	if trimmed != "" && json.Valid([]byte(trimmed)) {
		return json.RawMessage(trimmed)
	}
	return TextContent(output)
}

// ContentFromMessage converts a goai message into stored content.
func ContentFromMessage(m provider.Message) json.RawMessage {
	if (m.Role == provider.RoleUser || m.Role == provider.RoleSystem) &&
		len(m.Content) == 1 && m.Content[0].Type == provider.PartText {
		return TextContent(m.Content[0].Text)
	}
	parts := make([]ContentPart, 0, len(m.Content))
	for _, p := range m.Content {
		switch p.Type {
		case provider.PartText:
			parts = append(parts, ContentPart{Type: "text", Text: p.Text})
		case provider.PartReasoning:
			parts = append(parts, ContentPart{Type: "reasoning", Text: p.Text})
		case provider.PartToolCall:
			args := p.ToolInput
			if len(bytes.TrimSpace(args)) == 0 || !json.Valid(args) {
				args = json.RawMessage("{}")
			}
			parts = append(parts, ContentPart{
				Type: "tool-call", ToolCallID: p.ToolCallID, ToolName: p.ToolName, Args: args,
			})
		case provider.PartToolResult:
			parts = append(parts, ContentPart{
				Type: "tool-result", ToolCallID: p.ToolCallID, ToolName: p.ToolName,
				Result: jsonOrString(p.ToolOutput),
			})
		default:
			// Images and files are not something a run's text loop produces;
			// keep the text so the row is never empty.
			if p.Text != "" {
				parts = append(parts, ContentPart{Type: "text", Text: p.Text})
			}
		}
	}
	return PartsContent(parts)
}

// ParseContent decodes stored content into parts. Plain text and the summary
// envelope come back as one text part.
func ParseContent(content json.RawMessage) []ContentPart {
	trimmed := bytes.TrimSpace(content)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	switch trimmed[0] {
	case '"':
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return nil
		}
		return []ContentPart{{Type: "text", Text: s}}
	case '[':
		var parts []ContentPart
		if err := json.Unmarshal(trimmed, &parts); err != nil {
			return nil
		}
		return parts
	case '{':
		var env struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(trimmed, &env); err != nil {
			return nil
		}
		return []ContentPart{{Type: "text", Text: env.Text}}
	}
	return nil
}

// toolOutputString renders a stored tool result as the string goai sends
// back to the model.
func toolOutputString(result json.RawMessage) string {
	trimmed := bytes.TrimSpace(result)
	if len(trimmed) == 0 {
		return ""
	}
	if trimmed[0] == '"' {
		var s string
		if json.Unmarshal(trimmed, &s) == nil {
			return s
		}
	}
	return string(trimmed)
}

// MessageFromDTO converts a stored turn into a goai message.
func MessageFromDTO(m ports.MessageDTO) provider.Message {
	out := provider.Message{Role: provider.Role(m.Role)}
	for _, p := range ParseContent(m.Content) {
		switch p.Type {
		case "text":
			out.Content = append(out.Content, provider.Part{Type: provider.PartText, Text: p.Text})
		case "reasoning":
			out.Content = append(out.Content, provider.Part{Type: provider.PartReasoning, Text: p.Text})
		case "tool-call":
			args := p.Args
			if len(bytes.TrimSpace(args)) == 0 {
				args = json.RawMessage("{}")
			}
			out.Content = append(out.Content, provider.Part{
				Type: provider.PartToolCall, ToolCallID: p.ToolCallID, ToolName: p.ToolName, ToolInput: args,
			})
		case "tool-result":
			out.Content = append(out.Content, provider.Part{
				Type: provider.PartToolResult, ToolCallID: p.ToolCallID, ToolName: p.ToolName,
				ToolOutput: toolOutputString(p.Result),
			})
		}
	}
	return out
}

// MessagesFromDTOs converts a stored history into goai messages.
func MessagesFromDTOs(rows []ports.MessageDTO) []provider.Message {
	out := make([]provider.Message, 0, len(rows))
	for _, r := range rows {
		out = append(out, MessageFromDTO(r))
	}
	return out
}

// RepairDanglingToolCalls closes every assistant tool call that has no tool
// result before the next turn, with a synthetic result saying so.
//
// History can carry such a call legitimately: a run that was stopped while
// parked for approval persisted the call and never its result, or a worker
// died between the two. Strict providers (OpenAI, Anthropic) reject a prompt
// with a dangling call outright, which would wedge the thread on every later
// run. The repair is prompt-side only; nothing is written back.
func RepairDanglingToolCalls(msgs []provider.Message) []provider.Message {
	out := make([]provider.Message, 0, len(msgs)+2)
	for i := 0; i < len(msgs); i++ {
		m := msgs[i]
		out = append(out, m)
		if m.Role != provider.RoleAssistant {
			continue
		}
		var calls []provider.Part
		for _, p := range m.Content {
			if p.Type == provider.PartToolCall {
				calls = append(calls, p)
			}
		}
		if len(calls) == 0 {
			continue
		}
		answered := map[string]bool{}
		j := i + 1
		for ; j < len(msgs) && msgs[j].Role == provider.RoleTool; j++ {
			for _, p := range msgs[j].Content {
				if p.Type == provider.PartToolResult {
					answered[p.ToolCallID] = true
				}
			}
		}
		var missing []provider.Part
		for _, c := range calls {
			if !answered[c.ToolCallID] {
				missing = append(missing, provider.Part{
					Type: provider.PartToolResult, ToolCallID: c.ToolCallID, ToolName: c.ToolName,
					ToolOutput: `{"cancelled":true,"reason":"no result was recorded for this call"}`,
				})
			}
		}
		if len(missing) == 0 {
			continue
		}
		// Keep the results together, right after the call, before any later turn.
		out = append(out, msgs[i+1:j]...)
		out = append(out, provider.Message{Role: provider.RoleTool, Content: missing})
		i = j - 1
	}
	return out
}

// ToolCallIDsIn lists the tool-call ids a stored tool message answers.
func ToolCallIDsIn(content json.RawMessage) []string {
	var ids []string
	for _, p := range ParseContent(content) {
		if p.ToolCallID != "" {
			ids = append(ids, p.ToolCallID)
		}
	}
	return ids
}
