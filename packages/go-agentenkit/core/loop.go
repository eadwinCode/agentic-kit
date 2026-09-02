package core

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// IsParked reports whether a tool output is the sentinel a parked
// RequiresConfirmation tool returns (§2.5), and which call it parks.
func IsParked(output string) (toolCallID string, ok bool) {
	if len(output) == 0 || output[0] != '{' {
		return "", false
	}
	var m map[string]string
	if json.Unmarshal([]byte(output), &m) != nil {
		return "", false
	}
	id, ok := m[HITLParked]
	return id, ok && id != ""
}

// StepResult is one platform-owned step (§2.1, §5.6): a single round-trip.
// goai executes the step's tool calls and reports a structured result;
// whether to continue is the loop's decision, never goai's.
type StepResult struct {
	Text         string
	FinishReason provider.FinishReason
	Usage        provider.Usage
	// ResponseMessages are the assistant + tool messages this step produced,
	// appended to the conversation (in memory AND storage) before the next step.
	ResponseMessages []provider.Message
	ToolCalls        []provider.ToolCall
	ToolResults      []provider.ToolResult
}

// StepCall is what ExecuteStep needs.
type StepCall struct {
	Kind            ports.AgentKind
	Model           provider.LanguageModel
	Messages        []provider.Message
	Tools           []ports.Tool
	ProviderOptions ports.ProviderOptions
	OnChunk         func(provider.StreamChunk)
	// System overrides the spec's persona: a nested run brings its own and
	// must not inherit the parent's (§2.7). Empty keeps the spec's.
	System string
	// CacheSystemPrompt moves the system prompt into Messages as a stamped
	// message so it can carry a cache breakpoint (§2.6).
	CacheSystemPrompt bool
}

// ExecuteStep runs ONE round-trip. goai only runs a step's tools inside its
// own loop, and that loop needs MaxSteps > 1, so the step asks for two and
// stops before the second: the tools of step one run, and the loop here
// decides what happens next.
//
// Ownership rule (§3.1): the user's options are applied FIRST, the
// platform's LAST, so goai can never be handed a different model, prompt or
// step ceiling than the platform decided on.
func ExecuteStep(ctx context.Context, agent *RegisteredAgent, call StepCall) (*StepResult, error) {
	opts := slices.Clone(agent.Args.Options)

	system := call.System
	if system == "" {
		system = agent.Args.System
	}
	hoist := call.CacheSystemPrompt && system != ""
	messages := call.Messages
	if hoist {
		// Hoisted, the system prompt leads the messages and carries the
		// breakpoint; a fresh slice leaves the loop's own alone.
		messages = append([]provider.Message{SystemCacheMessage(system)}, call.Messages...)
	} else if system != "" {
		opts = append(opts, goai.WithSystem(system))
	}

	tools := make([]goai.Tool, 0, len(call.Tools))
	for _, t := range call.Tools {
		tools = append(tools, t.Tool)
	}
	opts = append(opts,
		goai.WithMessages(messages...),
		goai.WithTools(tools...),
		goai.WithMaxSteps(2), // the loop owns continuation
		goai.WithStopWhen(func([]goai.StepResult) bool { return true }),
	)
	if call.ProviderOptions != nil {
		opts = append(opts, goai.WithProviderOptions(map[string]any(call.ProviderOptions)))
	}

	var result *goai.TextResult
	if call.Kind == ports.KindStreamText {
		// goai runs a step's tools on its own goroutine as soon as it has sent
		// the step's chunks, without waiting for anyone to read them. The tools
		// are gated until this loop has drained (and published) every chunk of
		// the step, so the event log always shows a tool call before anything
		// the tool did: a nested run's chunks, a park, a snapshot taken from
		// inside the tool. goai marks that point with a step_finish chunk
		// before executing tools; the stream ending releases the gate too.
		drained := make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(drained) }) }
		gated := make([]goai.Tool, 0, len(tools))
		for _, t := range tools {
			gated = append(gated, gateTool(t, drained))
		}
		opts = append(opts, goai.WithTools(gated...))

		stream, err := goai.StreamText(ctx, call.Model, opts...)
		if err != nil {
			return nil, err
		}
		// Drain the full stream so OnChunk fires per part, and let a provider
		// failure surface here rather than hang on a result that never comes.
		drainErr := drainStream(stream, func(chunk provider.StreamChunk) {
			if call.OnChunk != nil {
				call.OnChunk(chunk)
			}
			if chunk.Type == provider.ChunkStepFinish {
				release()
			}
		})
		release()
		result = stream.Result()
		if drainErr != nil {
			return nil, drainErr
		}
	} else {
		var err error
		result, err = goai.GenerateText(ctx, call.Model, opts...)
		if err != nil {
			return nil, err
		}
	}

	step := &StepResult{
		Text:             result.Text,
		FinishReason:     result.FinishReason,
		Usage:            result.TotalUsage,
		ResponseMessages: result.ResponseMessages,
	}
	if n := len(result.Steps); n > 0 {
		last := result.Steps[n-1]
		step.Text = last.Text
		step.FinishReason = last.FinishReason
		step.Usage = last.Usage
		step.ToolCalls = last.ToolCalls
		step.ToolResults = last.ToolResults
	}
	if step.FinishReason == "" {
		step.FinishReason = provider.FinishStop
	}
	return step, nil
}

// gateTool delays a tool's Execute until the step's chunks are drained.
func gateTool(t goai.Tool, drained <-chan struct{}) goai.Tool {
	if t.Execute == nil {
		return t
	}
	inner := t.Execute
	t.Execute = func(ctx context.Context, input json.RawMessage) (string, error) {
		select {
		case <-drained:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return inner(ctx, input)
	}
	return t
}

// capValue keeps a recorded value small (§2.9): one oversized tool result
// should not be able to bloat the operational store. JSON is truncated by
// its serialised form so the shape stays readable.
func capValue(raw json.RawMessage, limit int) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	if !json.Valid(raw) {
		return TextContent(capText(string(raw), limit))
	}
	if limit <= 0 || len([]rune(string(raw))) <= limit {
		return raw
	}
	return TextContent(capText(string(raw), limit))
}

// RunLedger is the tokens a run has spent, main agent and nested runs
// together (§2.7). Shared so a child's spend counts against the run's safety
// cap the moment it happens.
type RunLedger struct {
	mu         sync.Mutex
	tokensUsed int
}

// Add books tokens onto the ledger.
func (l *RunLedger) Add(n int) {
	l.mu.Lock()
	l.tokensUsed += n
	l.mu.Unlock()
}

// TokensUsed reads the ledger.
func (l *RunLedger) TokensUsed() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tokensUsed
}

// LoopInput seeds a loop.
type LoopInput struct {
	// AgentID is whose stream this loop persists to (§2.7). Empty is the
	// main agent.
	AgentID string
	// RunID is the run these steps belong to (§2.9).
	RunID string
	Kind  ports.AgentKind
	Model provider.LanguageModel
	// Messages is the seeded context: compacted history for the main agent,
	// the brief plus its own persisted turns for a nested run.
	Messages []provider.Message
	Tools    []ports.Tool
	MaxSteps int
	// GenCtx is the context the model call runs under. It is cancelled by a
	// user stop (§2.1). Storage calls use the loop's own ctx, so a stopped run
	// can still be finalized.
	GenCtx context.Context
	// Aborted reports whether the platform cancelled GenCtx on purpose.
	Aborted         func() bool
	ProviderOptions ports.ProviderOptions
	// TokenBudget is the cumulative cap for the whole run, checked against
	// the shared ledger.
	TokenBudget int
	OnChunk     func(provider.StreamChunk)
	// System is the persona for a nested run; empty keeps the spec's.
	System            string
	CacheSystemPrompt bool
}

// LoopOutcome is how a loop ended.
type LoopOutcome struct {
	Text         string
	FinishReason provider.FinishReason
	// Attribution is what THIS loop spent; the caller records it against its
	// own AgentID.
	Attribution TokenAttribution
	TokensUsed  int
	// Parked: a RequiresConfirmation tool parked and the segment ends (§2.5).
	Parked           bool
	ParkedToolCallID string
	// Aborted: the platform cancelled the run mid-loop, a user stop (§2.1).
	Aborted bool
	// Steps is the iterations this loop completed (§2.9).
	Steps int
}

// stripParked drops the park sentinels from a tool message. Unlike the
// TypeScript package, which drops the whole message, the other results in
// the same message are kept: a step that parks one tool and runs another
// must not lose the second result, or its tool call would dangle.
func stripParked(m provider.Message) (provider.Message, bool) {
	if m.Role != provider.RoleTool {
		return m, true
	}
	kept := make([]provider.Part, 0, len(m.Content))
	for _, p := range m.Content {
		if p.Type == provider.PartToolResult {
			if _, parked := IsParked(p.ToolOutput); parked {
				continue
			}
		}
		kept = append(kept, p)
	}
	if len(kept) == 0 {
		return m, false
	}
	m.Content = kept
	return m, true
}

// RunLoop is the platform-owned loop (§2.1, §5.6), run by the main agent and
// by every nested run alike (§2.7).
//
// One single-round-trip step per iteration. After EVERY step the produced
// messages are persisted under this loop's AgentID, so a worker that dies
// mid-run resumes from the last step, and so a parked nested run can be
// re-entered later instead of restarted. Every continuation decision (tool
// results ready, budget spent, step ceiling, HITL park, user stop) is made
// here between steps, never inside goai.
func RunLoop(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, threadID string, input LoopInput, ledger *RunLedger) (*LoopOutcome, error) {
	out := &LoopOutcome{}
	genCtx := input.GenCtx
	if genCtx == nil {
		genCtx = ctx
	}
	aborted := input.Aborted
	if aborted == nil {
		aborted = func() bool { return genCtx.Err() != nil }
	}
	messages := input.Messages
	stepsLeft := input.MaxSteps

	for stepsLeft > 0 && !aborted() {
		stepsLeft--
		stepStartedAt := time.Now()
		var onChunk func(provider.StreamChunk)
		if input.Kind == ports.KindStreamText {
			onChunk = input.OnChunk
		}
		step, err := ExecuteStep(genCtx, agent, StepCall{
			Kind: input.Kind, Model: input.Model, Messages: messages, Tools: input.Tools,
			ProviderOptions: input.ProviderOptions, OnChunk: onChunk,
			System: input.System, CacheSystemPrompt: input.CacheSystemPrompt,
		})
		if err != nil {
			if aborted() {
				break // user stop mid-step
			}
			return out, err // real failure → §2.8 redrive policy
		}

		// Per-step durability (§5.6): append this step's turns BEFORE the next
		// step. A parked HITL tool result (the sentinel) is NOT a real result;
		// the resumed segment appends the user's verdict.
		for _, m := range step.ResponseMessages {
			persisted, keep := stripParked(m)
			if !keep {
				continue
			}
			if _, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
				Role: ports.MessageRole(m.Role), Content: ContentFromMessage(persisted), AgentID: input.AgentID,
			}); err != nil {
				return out, err
			}
		}
		messages = append(messages, step.ResponseMessages...)

		// A replay boundary (§2.2): everything this step produced is durable
		// history now, so a reconnecting client must NOT also replay its
		// chunks. Persisted (not a notice) because the snapshot needs its seq.
		if _, err := Publish(ctx, deps, threadID, "STEP_COMMITTED", map[string]any{
			"index": out.Steps, "agentId": nullable(input.AgentID),
		}); err != nil {
			return out, err
		}

		// Token attribution (§4), accumulated across the segment's steps and
		// into the run-wide ledger the safety cap is checked against (§2.7).
		a := AttributeTokens(step.Usage)
		out.Attribution.Add(a)
		out.TokensUsed += a.TotalTokens
		ledger.Add(a.TotalTokens)
		out.Text = step.Text
		out.FinishReason = step.FinishReason
		out.Steps++

		// §2.9: one row per step in the platform's OWN store, plus a bus-only
		// notice so live dashboards see it.
		marker := ports.StepRecord{
			RunID: input.RunID, ThreadID: threadID, AgentID: input.AgentID, Index: out.Steps,
			DurationMs: time.Since(stepStartedAt).Milliseconds(), FinishReason: string(step.FinishReason),
			InputTokens: a.InputTokens, CachedInputTokens: a.CachedInputTokens,
			OutputTokens: a.OutputTokens, TotalTokens: a.TotalTokens,
			Tools: []string{}, At: time.Now(),
		}
		argsOf := map[string]json.RawMessage{}
		for _, tc := range step.ToolCalls {
			argsOf[tc.ID] = tc.Input
		}
		for _, r := range step.ToolResults {
			if r.ToolName != "" {
				marker.Tools = append(marker.Tools, r.ToolName)
			}
			if deps.Config.RecordPayloads {
				marker.ToolCalls = append(marker.ToolCalls, ports.StepToolCall{
					ToolName: r.ToolName,
					Args:     capValue(argsOf[r.ToolCallID], deps.Config.PayloadCapChars),
					Result:   capValue(jsonOrString(r.Output), deps.Config.PayloadCapChars),
				})
			}
		}
		if deps.Config.RecordPayloads {
			marker.Text = capText(step.Text, deps.Config.PayloadCapChars)
		}
		if input.RunID != "" {
			_ = deps.Admin.Steps().Record(ctx, marker)
		}
		_ = PublishNotice(ctx, deps, threadID, "STEP_FINISHED", marker)

		// §2.5 park: a RequiresConfirmation tool returned the sentinel; the
		// segment ends here on WAITING_FOR_INPUT (set by ParkForApproval).
		for _, r := range step.ToolResults {
			if id, parked := IsParked(r.Output); parked {
				out.Parked = true
				out.ParkedToolCallID = id
				break
			}
		}
		if out.Parked {
			break
		}

		// Budget check BETWEEN steps (§2.1): the step that crossed the line is
		// kept in full; the next one never starts. Published before the break,
		// so a client learns why the run ended the moment it does.
		if input.TokenBudget > 0 && ledger.TokensUsed() >= input.TokenBudget {
			_, _ = Publish(ctx, deps, threadID, "TOKEN_BUDGET_EXHAUSTED", map[string]any{
				"agentId": nullable(input.AgentID), "tokensUsed": ledger.TokensUsed(), "tokenBudget": input.TokenBudget,
			})
			break
		}

		// tool-calls → goai executed the step's tools; the loop feeds the
		// results back. Anything else (stop, length, …) ends the run.
		if step.FinishReason != provider.FinishToolCalls {
			break
		}
	}

	out.Aborted = aborted()
	return out, nil
}
