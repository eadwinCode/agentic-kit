package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// mockModel stands in for a real provider when no API key is set. It reads
// keywords off the last user turn and calls the matching tool, then
// summarises the tool's result on the next round-trip, so every path through
// the platform (tools, approvals, questions, subagents, custom events) can be
// exercised offline. It streams word by word to look like the real thing.
type mockModel struct{ id string }

func (m *mockModel) ModelID() string { return "mock:" + m.id }

type plan struct {
	text      string
	reasoning string
	calls     []provider.ToolCall
}

// numberExpr finds an arithmetic expression: digits, operators, parentheses.
var numberExpr = regexp.MustCompile(`[\d(][\d\s()+\-*/.]*[\d)]`)

func expressionIn(prompt string) string {
	for _, m := range numberExpr.FindAllString(prompt, -1) {
		if strings.ContainsAny(m, "+-*/") {
			return strings.TrimSpace(m)
		}
	}
	return ""
}

func hasTool(p provider.GenerateParams, name string) bool {
	for _, t := range p.Tools {
		if t.Name == name {
			return true
		}
	}
	return false
}

func lastText(msgs []provider.Message, role provider.Role) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role != role {
			continue
		}
		var sb strings.Builder
		for _, part := range msgs[i].Content {
			if part.Type == provider.PartText {
				sb.WriteString(part.Text)
			}
		}
		return sb.String()
	}
	return ""
}

// childPersona is the system prompt the platform gives every nested run.
const childPersona = "subagent. Complete the task, then stop."

func isSubagent(p provider.GenerateParams) bool {
	if strings.Contains(p.System, childPersona) {
		return true
	}
	for _, m := range p.Messages {
		if m.Role == provider.RoleSystem && len(m.Content) > 0 && strings.Contains(m.Content[0].Text, childPersona) {
			return true
		}
	}
	return false
}

func (m *mockModel) plan(p provider.GenerateParams) plan {
	call := func(name string, args map[string]any) provider.ToolCall {
		b, _ := json.Marshal(args)
		return provider.ToolCall{ID: "call_" + agentenkit.NewID()[:8], Name: name, Input: b}
	}
	if len(p.Messages) == 0 {
		return plan{text: "Hello! Ask me something."}
	}
	last := p.Messages[len(p.Messages)-1]

	// After a tool ran: report what it said.
	if last.Role == provider.RoleTool {
		var lines []string
		for _, part := range last.Content {
			if part.Type != provider.PartToolResult {
				continue
			}
			out := part.ToolOutput
			var obj map[string]any
			if json.Unmarshal([]byte(out), &obj) == nil {
				switch part.ToolName {
				case "getWeather":
					lines = append(lines, fmt.Sprintf("It is %v°C and %v in %v.", obj["tempC"], obj["condition"], obj["city"]))
					continue
				case "renderDesign":
					lines = append(lines, "Your design is ready. The preview is on the right.")
					continue
				case "lookupOrders":
					if orders, ok := obj["orders"].([]any); ok {
						var items []string
						for _, o := range orders {
							if m, ok := o.(map[string]any); ok {
								items = append(items, fmt.Sprintf("%v (%v, %v)", m["id"], m["status"], m["total"]))
							}
						}
						lines = append(lines, fmt.Sprintf("Orders for %v: %s.", obj["org"], strings.Join(items, "; ")))
						continue
					}
				case "calculator":
					lines = append(lines, fmt.Sprintf("%v = %v", obj["expression"], obj["result"]))
					continue
				case "askDesignQuestions":
					// Turn the answers into a brief: "Bean & Co, terracotta and cream, playful"
					var parts []string
					if answers, ok := obj["answers"].(map[string]any); ok {
						for _, q := range []string{"What is the brand name?", "Which colours do you like?", "Playful or serious?"} {
							if v, ok := answers[q].(string); ok && v != "" {
								parts = append(parts, v)
							}
						}
					}
					brief := "Logo for " + strings.Join(parts, ", ")
					return plan{text: "Thanks, rendering that now. ", calls: []provider.ToolCall{call("renderDesign", map[string]any{"brief": brief})}}
				case "spawnSubagent":
					if r, ok := obj["result"]; ok {
						lines = append(lines, fmt.Sprintf("The subagent reported: %v", r))
					} else {
						lines = append(lines, fmt.Sprintf("The subagent could not finish: %v", obj["error"]))
					}
					continue
				case "sendEmail":
					if _, denied := obj["denied"]; denied {
						lines = append(lines, "Understood, I will not send the email.")
					} else if obj["reason"] == "timeout" {
						lines = append(lines, "The approval expired, so nothing was sent.")
					} else {
						lines = append(lines, fmt.Sprintf("Email sent to %v.", obj["to"]))
					}
					continue
				}
			}
			lines = append(lines, fmt.Sprintf("%s returned: %s", part.ToolName, truncate(out, 200)))
		}
		return plan{text: strings.Join(lines, " ")}
	}

	original := lastText(p.Messages, provider.RoleUser)
	prompt := strings.ToLower(original)
	if isSubagent(p) {
		// A child works from its brief alone. Do one useful thing and stop.
		if strings.Contains(prompt, "weather") && hasTool(p, "getWeather") {
			return plan{calls: []provider.ToolCall{call("getWeather", map[string]any{"city": cityIn(original)})}}
		}
		if strings.Contains(prompt, "email") && hasTool(p, "sendEmail") {
			return plan{calls: []provider.ToolCall{call("sendEmail", map[string]any{"to": "team@example.com", "subject": "From the subagent", "body": "Research summary attached."})}}
		}
		return plan{text: "Research summary: Go's scheduler multiplexes goroutines onto OS threads; channels and sync primitives coordinate them. Prefer contexts for cancellation."}
	}

	// "think" streams a reasoning block first, the way a provider with
	// extended thinking would.
	var reasoning string
	if strings.Contains(prompt, "think") {
		reasoning = "The user wants me to show my reasoning. I will consider the request, pick the right tool if one applies, and keep the answer short."
	}
	pl := m.route(p, prompt, original)
	pl.reasoning = reasoning
	return pl
}

// route picks the tool or text for a main-agent turn.
func (m *mockModel) route(p provider.GenerateParams, prompt, original string) plan {
	call := func(name string, args map[string]any) provider.ToolCall {
		b, _ := json.Marshal(args)
		return provider.ToolCall{ID: "call_" + agentenkit.NewID()[:8], Name: name, Input: b}
	}
	switch {
	case strings.Contains(prompt, "weather") && hasTool(p, "getWeather"):
		var calls []provider.ToolCall
		for _, city := range strings.Split(cityIn(original), " and ") {
			calls = append(calls, call("getWeather", map[string]any{"city": strings.TrimSpace(city)}))
		}
		return plan{text: "Let me check. ", calls: calls}
	case (strings.Contains(prompt, "question") || strings.Contains(prompt, "ask me")) && hasTool(p, "askDesignQuestions"):
		return plan{text: "A few questions first. ", calls: []provider.ToolCall{call("askDesignQuestions", map[string]any{
			"questions": []string{"What is the brand name?", "Which colours do you like?", "Playful or serious?"},
		})}}
	case (strings.Contains(prompt, "design") || strings.Contains(prompt, "render") || strings.Contains(prompt, "logo")) && hasTool(p, "renderDesign"):
		return plan{text: "Rendering that now. ", calls: []provider.ToolCall{call("renderDesign", map[string]any{"brief": lastText(p.Messages, provider.RoleUser)})}}
	case strings.Contains(prompt, "email") && hasTool(p, "sendEmail"):
		return plan{text: "I need your approval to send this. ", calls: []provider.ToolCall{call("sendEmail", map[string]any{
			"to": "client@example.com", "subject": "Design update", "body": "Here is the latest design preview.",
		})}}
	case (strings.Contains(prompt, "order") || strings.Contains(prompt, "invoice")) && hasTool(p, "lookupOrders"):
		return plan{calls: []provider.ToolCall{call("lookupOrders", map[string]any{"limit": 3})}}
	case (strings.Contains(prompt, "research") || strings.Contains(prompt, "delegate") || strings.Contains(prompt, "subagent")) && hasTool(p, "spawnSubagent"):
		return plan{text: "Delegating that. ", calls: []provider.ToolCall{call("spawnSubagent", map[string]any{
			"name": "researcher", "instructions": "Research this and report back in three sentences: " + lastText(p.Messages, provider.RoleUser),
		})}}
	case hasTool(p, "calculator") && expressionIn(prompt) != "":
		return plan{calls: []provider.ToolCall{call("calculator", map[string]any{"expression": expressionIn(prompt)})}}
	}
	return plan{text: "I am the built-in mock model (set OPENAI_API_KEY for a real one). Try: " +
		"\"weather in Paris and Rome\", \"render a logo for a coffee brand\", \"ask me questions about my design\", " +
		"\"send an email to the client\", \"show my orders\", \"research goroutines\", or \"what is (12*7)+3\"."}
}

func cityIn(prompt string) string {
	if i := strings.LastIndex(strings.ToLower(prompt), " in "); i >= 0 {
		return strings.Trim(strings.TrimSpace(prompt[i+4:]), "?.!")
	}
	return "London"
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func usageFor(p provider.GenerateParams, text string) provider.Usage {
	in := 0
	for _, m := range p.Messages {
		for _, part := range m.Content {
			in += (len(part.Text) + len(part.ToolOutput) + len(part.ToolInput)) / 4
		}
	}
	out := len(strings.Fields(text)) + 4
	return provider.Usage{InputTokens: in + 20, OutputTokens: out, TotalTokens: in + 20 + out}
}

func finishOf(pl plan) provider.FinishReason {
	if len(pl.calls) > 0 {
		return provider.FinishToolCalls
	}
	return provider.FinishStop
}

func (m *mockModel) DoGenerate(ctx context.Context, p provider.GenerateParams) (*provider.GenerateResult, error) {
	pl := m.plan(p)
	return &provider.GenerateResult{Text: pl.text, Reasoning: pl.reasoning, ToolCalls: pl.calls, FinishReason: finishOf(pl), Usage: usageFor(p, pl.text)}, nil
}

func (m *mockModel) DoStream(ctx context.Context, p provider.GenerateParams) (*provider.StreamResult, error) {
	pl := m.plan(p)
	ch := make(chan provider.StreamChunk, 8)
	go func() {
		defer close(ch)
		for _, word := range strings.SplitAfter(pl.reasoning, " ") {
			if word == "" {
				continue
			}
			select {
			case <-time.After(25 * time.Millisecond):
			case <-ctx.Done():
				ch <- provider.StreamChunk{Type: provider.ChunkError, Error: ctx.Err()}
				return
			}
			if !provider.TrySend(ctx, ch, provider.StreamChunk{Type: provider.ChunkReasoning, Text: word}) {
				return
			}
		}
		for _, word := range strings.SplitAfter(pl.text, " ") {
			if word == "" {
				continue
			}
			select {
			case <-time.After(35 * time.Millisecond):
			case <-ctx.Done():
				ch <- provider.StreamChunk{Type: provider.ChunkError, Error: ctx.Err()}
				return
			}
			if !provider.TrySend(ctx, ch, provider.StreamChunk{Type: provider.ChunkText, Text: word}) {
				return
			}
		}
		for _, c := range pl.calls {
			provider.TrySend(ctx, ch, provider.StreamChunk{Type: provider.ChunkToolCall, ToolCallID: c.ID, ToolName: c.Name, ToolInput: string(c.Input)})
		}
		provider.TrySend(ctx, ch, provider.StreamChunk{Type: provider.ChunkFinish, FinishReason: finishOf(pl), Usage: usageFor(p, pl.text)})
	}()
	return &provider.StreamResult{Stream: ch}, nil
}
