package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"html"
	"strings"
	"sync"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// app holds the runtime, the registered agent and the demo's own state.
type app struct {
	rt    *agentenkit.AgentCore
	chat  *agentenkit.AgentHandle
	model string

	// Rendered previews, served at /api/previews/{id}.svg.
	previews sync.Map
}

// billing is the demo's stand-in for a billing system: a token allowance
// per thread. A real one would key this by organisation and read a ledger;
// the shape of the integration is the same.
//
// Three enforcement points, because a run is not a request:
//   - creditCheck is the platform's BillingPreCheck: it refuses a NEW run
//     before anything is persisted or dispatched, and publishes CREDIT_LIMIT
//     on the thread so the chat shows why.
//   - the run handler passes the remaining allowance as the run's
//     TokenBudget: the platform checks it between steps and publishes
//     TOKEN_BUDGET_EXHAUSTED when a run spends it.
//   - what a run spent is taken off the allowance when it finishes.
type billing struct {
	mu        sync.Mutex
	remaining map[string]int
}

// creditAllowance is what every thread starts with, in tokens.
const creditAllowance = 6_000

var credit = &billing{remaining: map[string]int{}}

func (b *billing) balance(threadID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if v, ok := b.remaining[threadID]; ok {
		return v
	}
	return creditAllowance
}

func (b *billing) set(threadID string, tokens int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.remaining[threadID] = tokens
}

func (b *billing) spend(threadID string, tokens int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.remaining[threadID]; !ok {
		b.remaining[threadID] = creditAllowance
	}
	b.remaining[threadID] -= tokens
	if b.remaining[threadID] < 0 {
		b.remaining[threadID] = 0
	}
}

// resetAt is when the demo's billing period rolls over.
func resetAt() time.Time { return time.Now().Add(24 * time.Hour).Truncate(time.Hour) }

// creditCheck is wired as the platform's BillingPreCheck. It runs when the
// user sends a message; the composer stays open, the refusal is what the
// user sees.
func creditCheck(ctx context.Context, check agentenkit.BillingCheck) error {
	remaining := credit.balance(check.ThreadID)
	if remaining > 0 {
		return nil
	}
	// Durable, so the chat shows it now and after a reload.
	_, _ = check.PublishEvent(ctx, "CREDIT_LIMIT", map[string]any{
		"kind": "monthly", "remaining": 0, "resetAt": resetAt(),
	}, false)
	return fmt.Errorf("credit limit reached. resets %s - clear it to continue", resetAt().Format("2 Jan"))
}

func newApp(rt *agentenkit.AgentCore, model string) *app {
	a := &app{rt: rt, model: model}
	// Every tool gets a ToolContext: the run state, the tool call id, and
	// PublishEvent bound to the thread. Custom events reach the SPA through
	// the same stream as the platform's own.
	tools := []agentenkit.Tool{
		a.getWeather(),
		a.calculator(),
		a.lookupOrders(),
		a.renderDesign(),
		// Parked behind a human approval instead of executing (§2.5).
		agentenkit.MarkRequiresConfirmation(a.sendEmail()),
		// Parked too, but the approval carries answers back into the tool.
		agentenkit.MarkRequiresConfirmation(a.askDesignQuestions()),
	}
	a.chat = rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
		Name:  "chat",
		Model: model,
		// What a run spent comes off the thread's allowance.
		OnFinish: func(info agentenkit.RunFinishInfo) { credit.spend(info.ThreadID, info.TokensUsed) },
		System: "You are a concise assistant for a small design studio. Use the tools when they help. " +
			"Before rendering a design, ask the user questions with askDesignQuestions if the brief is vague. " +
			"Delegate research to a subagent when a task is self-contained.",
		Tools: tools,
		// Opt-in delegation (§2.7): the platform injects spawnSubagent, and
		// these tools are merged into every child and HITL-wrapped like the
		// parent's, so a subagent parks for approval too.
		Subagents: &agentenkit.SubagentsConfig{
			Tools: []agentenkit.Tool{a.getWeather(), agentenkit.MarkRequiresConfirmation(a.sendEmail())},
		},
	})
	return a
}

// ---- tools ----

func (a *app) getWeather() agentenkit.Tool {
	return agentenkit.AgentTool("getWeather", "Current weather for a city.",
		func(ctx context.Context, in struct {
			City string `json:"city" jsonschema:"description=City name"`
		}, tc agentenkit.ToolContext) (string, error) {
			// A notice: live only, never replayed. Right for a progress tick.
			_, _ = tc.PublishEvent(ctx, "PROGRESS", map[string]any{"label": "Checking the weather in " + in.City}, agentenkit.PublishOptions{Notice: true})
			time.Sleep(300 * time.Millisecond)
			h := fnv.New32a()
			h.Write([]byte(strings.ToLower(in.City)))
			n := h.Sum32()
			conditions := []string{"sunny", "cloudy", "light rain", "windy", "clear"}
			return jsonString(map[string]any{
				"city": in.City, "tempC": 8 + int(n%20), "condition": conditions[n%uint32(len(conditions))],
			}), nil
		})
}

func (a *app) calculator() agentenkit.Tool {
	return agentenkit.AgentTool("calculator", "Evaluate an arithmetic expression with + - * / and parentheses.",
		func(_ context.Context, in struct {
			Expression string `json:"expression" jsonschema:"description=The expression, e.g. (12*7)+3"`
		}, _ agentenkit.ToolContext) (string, error) {
			v, err := evalExpr(in.Expression)
			if err != nil {
				return "", err
			}
			return jsonString(map[string]any{"expression": in.Expression, "result": v}), nil
		})
}

func (a *app) lookupOrders() agentenkit.Tool {
	return agentenkit.AgentTool("lookupOrders", "Recent orders for the current organisation.",
		func(ctx context.Context, in struct {
			Limit int `json:"limit,omitempty" jsonschema:"description=How many to return (default 3)"`
		}, tc agentenkit.ToolContext) (string, error) {
			// The run state (§2.10): whatever the client attached to run(),
			// here the organisation. The model never chooses it.
			org, _ := tc.State["orgId"].(string)
			if org == "" {
				return "", errors.New("no orgId on the run state")
			}
			limit := in.Limit
			if limit <= 0 {
				limit = 3
			}
			var orders []map[string]any
			for i := 1; i <= limit; i++ {
				orders = append(orders, map[string]any{
					"id": fmt.Sprintf("%s-%03d", strings.ToUpper(org), i), "total": 120 * i, "status": []string{"paid", "shipped", "open"}[i%3],
				})
			}
			return jsonString(map[string]any{"org": org, "orders": orders}), nil
		})
}

func (a *app) renderDesign() agentenkit.Tool {
	return agentenkit.AgentTool("renderDesign", "Render a design preview from a brief.",
		func(ctx context.Context, in struct {
			Brief string `json:"brief" jsonschema:"description=What to design, with colours and mood"`
		}, tc agentenkit.ToolContext) (string, error) {
			for _, stage := range []string{"Sketching layout", "Choosing a palette", "Rendering preview"} {
				_, _ = tc.PublishEvent(ctx, "PROGRESS", map[string]any{"label": stage + "…"}, agentenkit.PublishOptions{Notice: true})
				select {
				case <-time.After(500 * time.Millisecond):
				case <-ctx.Done():
					return "", ctx.Err()
				}
			}
			id := agentenkit.NewID()
			a.previews.Store(id, renderSVG(in.Brief))
			url := "/api/previews/" + id + ".svg"
			// Durable: in the log, replayed to a client that reconnects mid-run.
			if _, err := tc.PublishEvent(ctx, "DESIGN_PREVIEW", map[string]any{"url": url, "brief": in.Brief}, agentenkit.PublishOptions{}); err != nil {
				return "", err
			}
			return jsonString(map[string]any{"url": url}), nil
		})
}

func (a *app) sendEmail() agentenkit.Tool {
	return agentenkit.AgentTool("sendEmail", "Send an email (destructive: needs the user's approval).",
		func(ctx context.Context, in struct {
			To      string `json:"to"`
			Subject string `json:"subject"`
			Body    string `json:"body"`
		}, tc agentenkit.ToolContext) (string, error) {
			// Only reached after a human approved the park (§2.5).
			_, _ = tc.PublishEvent(ctx, "EMAIL_SENT", map[string]any{"to": in.To, "subject": in.Subject}, agentenkit.PublishOptions{})
			return jsonString(map[string]any{"status": "SENT", "to": in.To, "subject": in.Subject}), nil
		})
}

func (a *app) askDesignQuestions() agentenkit.Tool {
	return agentenkit.AgentTool("askDesignQuestions", "Ask the user a few short questions before designing. The run waits for the answers.",
		func(ctx context.Context, in struct {
			Questions []string `json:"questions" jsonschema:"description=Two to four short questions"`
		}, tc agentenkit.ToolContext) (string, error) {
			// The park carried the questions to the client; the approval brings
			// the answers back as tc.Approval.Payload.
			if tc.Approval == nil {
				return "", errors.New("askDesignQuestions must be approved before it runs")
			}
			var answers map[string]string
			_ = json.Unmarshal(tc.Approval.Payload, &answers)
			_, _ = tc.PublishEvent(ctx, "QUESTIONS_ANSWERED", map[string]any{"answers": answers}, agentenkit.PublishOptions{})
			return jsonString(map[string]any{"answers": answers}), nil
		})
}

// ---- helpers ----

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// renderSVG turns a brief into a preview: a palette picked from the brief's
// hash, and the brief itself as the headline.
func renderSVG(brief string) string {
	h := fnv.New32a()
	h.Write([]byte(brief))
	n := h.Sum32()
	palettes := [][3]string{
		{"#0f172a", "#38bdf8", "#f8fafc"}, {"#7c2d12", "#fb923c", "#fff7ed"}, {"#14532d", "#4ade80", "#f0fdf4"},
		{"#4c1d95", "#c084fc", "#faf5ff"}, {"#1e3a8a", "#facc15", "#eff6ff"},
	}
	p := palettes[n%uint32(len(palettes))]
	title := brief
	if len(title) > 48 {
		title = title[:48] + "…"
	}
	return fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 480 270" width="480" height="270">
  <rect width="480" height="270" fill="%s"/>
  <circle cx="400" cy="70" r="50" fill="%s"/>
  <rect x="32" y="180" width="%d" height="14" rx="7" fill="%s"/>
  <text x="32" y="80" font-family="system-ui, sans-serif" font-size="22" font-weight="700" fill="%s">%s</text>
  <text x="32" y="112" font-family="system-ui, sans-serif" font-size="13" fill="%s" opacity="0.8">rendered by the go-agentenkit example</text>
</svg>`, p[0], p[1], 120+int(n%240), p[1], p[2], html.EscapeString(title), p[2])
}

// evalExpr is a tiny recursive-descent evaluator: + - * / and parentheses.
func evalExpr(s string) (float64, error) {
	p := &parser{s: strings.ReplaceAll(s, " ", "")}
	v, err := p.expr()
	if err != nil {
		return 0, err
	}
	if p.i != len(p.s) {
		return 0, fmt.Errorf("unexpected %q at %d", p.s[p.i:], p.i)
	}
	return v, nil
}

type parser struct {
	s string
	i int
}

func (p *parser) peek() byte {
	if p.i < len(p.s) {
		return p.s[p.i]
	}
	return 0
}

func (p *parser) expr() (float64, error) {
	v, err := p.term()
	if err != nil {
		return 0, err
	}
	for p.peek() == '+' || p.peek() == '-' {
		op := p.s[p.i]
		p.i++
		r, err := p.term()
		if err != nil {
			return 0, err
		}
		if op == '+' {
			v += r
		} else {
			v -= r
		}
	}
	return v, nil
}

func (p *parser) term() (float64, error) {
	v, err := p.factor()
	if err != nil {
		return 0, err
	}
	for p.peek() == '*' || p.peek() == '/' {
		op := p.s[p.i]
		p.i++
		r, err := p.factor()
		if err != nil {
			return 0, err
		}
		if op == '*' {
			v *= r
		} else {
			if r == 0 {
				return 0, errors.New("division by zero")
			}
			v /= r
		}
	}
	return v, nil
}

func (p *parser) factor() (float64, error) {
	if p.peek() == '(' {
		p.i++
		v, err := p.expr()
		if err != nil {
			return 0, err
		}
		if p.peek() != ')' {
			return 0, errors.New("missing )")
		}
		p.i++
		return v, nil
	}
	if p.peek() == '-' {
		p.i++
		v, err := p.factor()
		return -v, err
	}
	start := p.i
	for p.i < len(p.s) && (p.s[p.i] >= '0' && p.s[p.i] <= '9' || p.s[p.i] == '.') {
		p.i++
	}
	if start == p.i {
		return 0, fmt.Errorf("expected a number at %d", start)
	}
	var v float64
	if _, err := fmt.Sscanf(p.s[start:p.i], "%g", &v); err != nil {
		return 0, err
	}
	return v, nil
}
