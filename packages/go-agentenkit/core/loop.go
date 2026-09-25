package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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
	// ProviderMetadata is what the provider attached to the finish, one entry
	// per provider namespace, plus two reserved keys: "responseId" and
	// "responseHeaders". Gateways put a call's real cost in a header, so a
	// receipt pricer reads it from there (§4).
	ProviderMetadata map[string]any
	// StreamedText is what actually reached the client before the call ended.
	// Set even when the call failed or was stopped mid-stream, so the tokens
	// of a cut-off call can still be estimated and billed.
	StreamedText string
	// Finished is false when no finish ever arrived: the stream was cut off
	// by a stop or a provider failure, and the counters are not the
	// provider's own.
	Finished bool
}

// providerMeta flattens what goai reports into the single map a Pricer
// reads. Provider namespaces keep their names; the response id and headers
// get reserved keys, because an AI gateway bills through a header and a
// receipt pricer has to be able to find it.
func providerMeta(pm map[string]map[string]any, resp provider.ResponseMetadata) map[string]any {
	out := make(map[string]any, len(pm)+len(resp.ProviderMetadata)+2)
	for ns, v := range pm {
		out[ns] = v
	}
	for k, v := range resp.ProviderMetadata {
		out[k] = v
	}
	if resp.ID != "" {
		out["responseId"] = resp.ID
	}
	if len(resp.Headers) > 0 {
		out["responseHeaders"] = resp.Headers
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
	var streamedText string
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
			// Nothing streamed and nothing was spent: no usage row to keep.
			return &StepResult{}, err
		}
		// Drain the full stream so OnChunk fires per part, and let a provider
		// failure surface here rather than hang on a result that never comes.
		// The text is accumulated as it goes, so a call cut off half way still
		// knows how much it produced and can be billed for it (§4).
		var streamed strings.Builder
		var sawFinish, sawReason, sawToolDelta bool
		drainErr := drainStream(stream, func(chunk provider.StreamChunk) {
			// Everything the model produced counts as output when a call is
			// cut off: the answer, its thinking, and the tool arguments,
			// which for a code-writing agent are most of the tokens. Tool
			// input arrives as deltas when the provider streams it, or as
			// one complete call when it does not; never both for one call.
			switch chunk.Type {
			case provider.ChunkText, provider.ChunkReasoning:
				streamed.WriteString(chunk.Text)
			case provider.ChunkToolCallDelta:
				sawToolDelta = true
				streamed.WriteString(chunk.ToolInput)
			case provider.ChunkToolCall:
				if !sawToolDelta {
					streamed.WriteString(chunk.ToolInput)
				}
			}
			// Whether the call ran to its end is decided by what this loop
			// SAW, not by whether the stream reported an error: a stop that
			// tears the provider down mid-call does not always surface as one
			// (§4). It completed when the finish chunk arrived AND the model
			// gave a finish reason. goai's OpenAI stream puts that reason on
			// the step_finish chunk and ends with a finish chunk that carries
			// only the usage; its tool loop puts it on the finish chunk. A
			// stream that closed with no reason anywhere was cut short
			// without saying so, which is not a finish (the TS runtime reads
			// the SDK's 'unknown' the same way).
			if (chunk.Type == provider.ChunkFinish || chunk.Type == provider.ChunkStepFinish) && chunk.FinishReason != "" {
				sawReason = true
			}
			if chunk.Type == provider.ChunkFinish {
				sawFinish = true
			}
			if call.OnChunk != nil {
				call.OnChunk(chunk)
			}
			if chunk.Type == provider.ChunkStepFinish {
				release()
			}
		})
		release()
		streamedText = streamed.String()
		if drainErr != nil || !sawFinish || !sawReason {
			// The call ended without finishing: a provider failure, or a stop
			// that tore it down mid-stream. goai ends every other path with a
			// finish chunk, and drops that chunk exactly when its own context
			// is already cancelled, so its absence is the reliable signal.
			// The platform's own context is NOT: goai cancels its side first,
			// and the run's cancellation can still be a poll behind.
			//
			// Result() must not be read here. goai writes the result up to the
			// moment it sends that chunk, so a stream that never sent one may
			// still have a goroutine writing, and reading it races.
			return &StepResult{StreamedText: streamedText}, drainErr
		}
		// Safe now: the finish chunk arrived, so goai has stopped writing.
		result = stream.Result()
	} else {
		var err error
		result, err = goai.GenerateText(ctx, call.Model, opts...)
		if err != nil {
			return &StepResult{}, err
		}
	}

	step := stepFrom(result)
	step.StreamedText, step.Finished = streamedText, true
	if step.StreamedText == "" {
		step.StreamedText = step.Text
	}
	return step, nil
}

// stepFrom reduces a goai result to the platform's one-round-trip step. The
// last goai step is the authority when there is one; otherwise the totals
// are all there is, which is the shape a cut-off stream comes back in.
func stepFrom(result *goai.TextResult) *StepResult {
	if result == nil {
		return &StepResult{}
	}
	step := &StepResult{
		Text:             result.Text,
		FinishReason:     result.FinishReason,
		Usage:            result.TotalUsage,
		ResponseMessages: result.ResponseMessages,
		ProviderMetadata: providerMeta(result.ProviderMetadata, result.Response),
	}
	if n := len(result.Steps); n > 0 {
		last := result.Steps[n-1]
		step.Text = last.Text
		step.FinishReason = last.FinishReason
		step.Usage = last.Usage
		step.ToolCalls = last.ToolCalls
		step.ToolResults = last.ToolResults
		if meta := providerMeta(last.ProviderMetadata, last.Response); meta != nil {
			step.ProviderMetadata = meta
		}
	}
	if step.FinishReason == "" {
		step.FinishReason = provider.FinishStop
	}
	return step
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

// RunLedger is what a run has spent, main agent and nested runs together
// (§2.7): its tokens, its money and the one currency that money is in.
// Shared so a child's spend counts against the run's caps the moment it
// happens.
//
// A run's ledger starts from what the run already spent (SeedRunLedger): its
// earlier segments, before a park or a retry, count against the same caps.
// After that it is kept in memory, so the caps are checked without reading
// every usage row back after every step.
type RunLedger struct {
	mu         sync.Mutex
	tokensUsed int
	costMicros int64
	currency   string
}

// SeedRunLedger starts a ledger from the run's usage rows (§4). A failed
// read is logged and the ledger starts from zero: a run is not failed over
// its caps' bookkeeping.
func SeedRunLedger(ctx context.Context, deps ports.RuntimePorts, threadID, runID string) *RunLedger {
	l := &RunLedger{}
	if runID == "" {
		return l
	}
	spent, err := deps.Storage.Usage.Total(context.WithoutCancel(ctx), threadID, ports.UsageFilter{RunID: runID})
	if err != nil {
		Logger(deps).Error("run spend not read; the caps count from zero this segment", "run", runID, "err", err)
		return l
	}
	l.tokensUsed, l.costMicros, l.currency = spent.TotalTokens, spent.CostMicros, spent.Currency
	return l
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

// Spent is the money the run has spent, and the currency it is in.
func (l *RunLedger) Spent() (int64, string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.costMicros, l.currency
}

// Record prices one call, stores its row and books it (§4). A run is priced
// in one currency: a call priced in another is stored unpriced, with an
// error logged, rather than mixed into a bill that cannot add it up.
func (l *RunLedger) Record(ctx context.Context, deps ports.RuntimePorts, threadID string, u ports.NewUsage) ports.NewUsage {
	u = price(ctx, deps, u)
	if l != nil && u.Cost != nil {
		l.mu.Lock()
		if l.currency == "" {
			l.currency = u.Cost.Currency
		}
		if u.Cost.Currency != l.currency {
			Logger(deps).Error("usage priced in a second currency; stored unpriced",
				"run", u.RunID, "model", u.Model, "currency", u.Cost.Currency, "runCurrency", l.currency)
			u.Cost = nil
		}
		l.mu.Unlock()
	}
	store(ctx, deps, threadID, u)
	if l != nil {
		l.mu.Lock()
		l.tokensUsed += u.TotalTokens()
		if u.Cost != nil {
			l.costMicros += u.Cost.Micros
		}
		l.mu.Unlock()
	}
	return u
}

// LoopInput seeds a loop.
// ErrRunLockLost ends a loop whose run lock was lost between steps (§3.4).
var ErrRunLockLost = errors.New("agentenkit: run lock lost")

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
	Aborted func() bool
	// CommitParks writes the parks raised during a step, once that step is
	// saved (see ParkBox). Only the main loop sets it: a nested run's parks
	// wait for the main agent's step. Nil commits nothing.
	CommitParks func(ctx context.Context) error
	// Fenced reports that the run lock is gone (§3.4): another worker may
	// own the thread, so this loop must not write another step to it. Nil
	// means never.
	Fenced          func() bool
	ProviderOptions ports.ProviderOptions
	// TokenBudget is the cumulative cap for the whole run, checked against
	// the shared ledger.
	TokenBudget int
	// CostBudgetMicros is the money cap for the whole run (§4), checked
	// against the same shared ledger and between the same steps.
	CostBudgetMicros int64
	// BillingRunID is the DISPATCHED run every call here is billed to (§4).
	// A nested loop's RunID is its own; this stays the parent's, so one
	// run's whole bill, delegated work included, is a single query.
	BillingRunID string
	// ModelKey is the registry key the model was resolved from — what a
	// price list is usually keyed by.
	ModelKey string
	// ModelID is the wire id that key resolved to (ResolvedModel.WireID),
	// for a price list keyed by wire ids instead.
	ModelID string
	// AgentName is the name that goes on the bill line: the registered
	// handle for the main run, the delegation's name for a nested one.
	AgentName string
	// OnChunk sees every raw chunk: the host's own callback.
	OnChunk func(provider.StreamChunk)
	// PublishChunk publishes a chunk as an event. Deltas reach it merged
	// (see chunkBatcher), and everything a step streamed is out before the
	// step is committed.
	PublishChunk func(payload map[string]any)
	// System is the persona for a nested run; empty keeps the spec's.
	System string
	// SystemFn builds the persona per step (§3.1); it wins over System and
	// over the spec's.
	SystemFn ports.SystemFunc
	// PrepareStep edits the prompt per step; what it adds is never persisted.
	PrepareStep ports.PrepareStepFunc
	// State is the run's state, handed to SystemFn (§2.10).
	State             ports.AgentRunState
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
	// Interrupted: the last model call ended without a finish and without a
	// user stop, and no error said why. The caller works out the cause (a
	// lost lock, a segment deadline, a stream the provider cut short); the
	// run must not be taken as finished.
	Interrupted bool
	// Steps is the iterations this loop completed (§2.9).
	Steps int
	// CostExhausted: the run hit its money cap and stopped between steps (§4).
	CostExhausted bool
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
	// The input count of the last finished call. A call cut off before its
	// finish never reports one, and its prompt was the same size as the
	// previous step's plus a little, so this is the honest floor to bill.
	lastInput := 0

	for stepsLeft > 0 && !aborted() {
		stepsLeft--
		stepStartedAt := time.Now()
		var onChunk func(provider.StreamChunk)
		var batcher *chunkBatcher
		if input.Kind == ports.KindStreamText {
			if input.PublishChunk != nil {
				batcher = newChunkBatcher(input.PublishChunk)
			}
			onChunk = func(c provider.StreamChunk) {
				if batcher != nil {
					batcher.push(ChunkPayload(c))
				}
				if input.OnChunk != nil {
					input.OnChunk(c)
				}
			}
		}
		system := input.System
		if input.SystemFn != nil {
			// Built per step with the run's state (§3.1): what the agent is
			// acting on may have changed since the last step.
			built, err := input.SystemFn(ctx, threadID, input.State)
			if err != nil {
				return out, err
			}
			system = built
		}
		prompt := messages
		if input.PrepareStep != nil {
			prepared, err := input.PrepareStep(ctx, threadID, input.State, messages)
			if err != nil {
				return out, err
			}
			prompt = prepared
		}
		// One round trip is bounded in wall time when StepTimeout is set: a
		// model that accepts the call and never answers ends this step like
		// any failed step, and the run takes the retry policy (§2.8).
		stepCtx, cancelStep := genCtx, context.CancelFunc(func() {})
		if deps.Config.StepTimeout > 0 {
			stepCtx, cancelStep = context.WithTimeout(genCtx, deps.Config.StepTimeout)
		}
		step, err := ExecuteStep(stepCtx, agent, StepCall{
			Kind: input.Kind, Model: input.Model, Messages: prompt, Tools: input.Tools,
			ProviderOptions: input.ProviderOptions, OnChunk: onChunk,
			System: system, CacheSystemPrompt: input.CacheSystemPrompt,
		})
		stepTimedOut := deps.Config.StepTimeout > 0 && errors.Is(stepCtx.Err(), context.DeadlineExceeded) && genCtx.Err() == nil
		cancelStep()
		if batcher != nil {
			batcher.flush() // the step's text is out before anything below
		}
		if stepTimedOut {
			if err == nil {
				err = context.DeadlineExceeded
			}
			err = fmt.Errorf("step %d ran longer than %s: %w", out.Steps+1, deps.Config.StepTimeout, err)
			Logger(deps).Warn("step timed out", "run", input.RunID, "step", out.Steps+1, "after", deps.Config.StepTimeout)
		}
		if err != nil || !step.Finished {
			// The call ended without a finish: a user stop, or the provider
			// failing part way. Either way the provider billed for what it had
			// already produced, so the call is recorded rather than dropped —
			// with estimated counters where it never reported real ones (§4).
			outcome := ports.UsageErrored
			if aborted() {
				outcome = ports.UsageAborted
			}
			if lastInput == 0 {
				// Nothing finished on this run yet, so estimate the prompt that
				// was certainly sent rather than billing the call as free.
				lastInput = EstimateMessages(messages)
			}
			if u := unfinishedUsage(input, out.Steps+1, step, outcome, lastInput); u.TotalTokens() > 0 {
				u = ledger.Record(ctx, deps, threadID, u)
				out.Attribution.Add(u.Totals())
				out.TokensUsed += u.TotalTokens()
			}
			if aborted() {
				break // user stop mid-step
			}
			if err == nil {
				out.Interrupted = true
				break
			}
			return out, err // real failure → §2.8 redrive policy
		}

		// The step is done, and its messages are the first thing it writes.
		// A worker that lost the lock meanwhile must not write them: the
		// next holder may already be writing its own. The call itself did
		// happen and the provider billed it, so its usage is still recorded.
		if input.Fenced != nil && input.Fenced() {
			ledger.Record(ctx, deps, threadID, usageOf(input, out.Steps+1, ports.KindStep, step, ports.UsageFinished))
			return out, ErrRunLockLost
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

		// One priced usage row per model call (§4), booked on the run-wide
		// ledger the caps are checked against (§2.7). Written as soon as the
		// step's messages are, before anything else: a crash after this
		// point resumes from the saved step and never runs the call again,
		// so its row must already be there.
		u := ledger.Record(ctx, deps, threadID, usageOf(input, out.Steps+1, ports.KindStep, step, ports.UsageFinished))

		// goai runs a step's tools after the step finishes and streams no
		// tool-result chunk for them, so a client would keep showing the call
		// as running until its next snapshot. One is published per result
		// here, before the step commits, in the same shape a provider-run
		// tool's result streams in. A parked call has no result yet: its
		// verdict arrives with the resume.
		if onChunk != nil {
			for _, r := range step.ToolResults {
				if _, parked := IsParked(r.Output); parked {
					continue
				}
				onChunk(provider.StreamChunk{
					Type: provider.ChunkToolResult, ToolCallID: r.ToolCallID, ToolName: r.ToolName,
					Text: r.Output, Error: r.Error,
				})
			}
		}
		// A replay boundary (§2.2): everything this step produced is durable
		// history now, so a reconnecting client must NOT also replay its
		// chunks. Persisted (not a notice) because the snapshot needs its seq.
		// It carries the step's finish and usage: a run stream turns it into
		// its STEP_FINISHED, at the moment the step is saved.
		a := u.Totals()
		if _, err := Publish(ctx, deps, threadID, "STEP_COMMITTED", map[string]any{
			"index": out.Steps, "agentId": nullable(input.AgentID),
			"step": out.Steps + 1, "finishReason": string(step.FinishReason),
			"inputTokens": a.InputTokens, "cachedInputTokens": a.CachedInputTokens,
			"outputTokens": a.OutputTokens, "totalTokens": a.TotalTokens,
		}); err != nil {
			return out, err
		}
		// The step is durable now, tool calls included, so the parks it
		// raised can be written: WAITING_FOR_INPUT and the approval requests.
		if input.CommitParks != nil {
			if err := input.CommitParks(ctx); err != nil {
				return out, err
			}
		}

		lastInput = u.InputTokens
		out.Attribution.Add(a)
		out.TokensUsed += a.TotalTokens
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
		_ = PublishNotice(ctx, deps, threadID, "STEP_FINISHED", stepFinishedPayload(marker))

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

		// The money cap (§4), checked in the same place and the same way,
		// against the same shared ledger: a nested run's calls count against
		// it, and a run that parked or retried starts from what it already
		// spent (see SeedRunLedger).
		//
		// It only ever sees priced calls: with no Pricer configured nothing is
		// ever spent and the cap never fires.
		if input.CostBudgetMicros > 0 {
			if spent, currency := ledger.Spent(); spent >= input.CostBudgetMicros {
				exhausted := map[string]any{
					"agentId": nullable(input.AgentID), "costMicros": spent,
					"costBudgetMicros": input.CostBudgetMicros,
				}
				if currency != "" { // left out when unset, as TS does
					exhausted["currency"] = currency
				}
				_, _ = Publish(ctx, deps, threadID, "COST_BUDGET_EXHAUSTED", exhausted)
				out.CostExhausted = true
				break
			}
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

// usageOf is one finished model call as a usage row, ready to be priced (§4).
func usageOf(input LoopInput, step int, kind ports.UsageKind, s *StepResult, outcome ports.UsageOutcome) ports.NewUsage {
	u := ports.NewUsage{
		RunID: input.BillingRunID, AgentID: input.AgentID, AgentName: input.AgentName,
		Kind: kind, Step: step,
		Model: input.ModelKey, ModelID: input.ModelID,
		Outcome: outcome, ProviderMetadata: s.ProviderMetadata,
	}
	FillTokens(&u, s.Usage)
	return u
}

// unfinishedUsage is the row for a call that never reported a finish: a user
// stop, or the provider failing mid-stream (§4).
//
// The provider still billed for it, so the counters are filled in from what
// IS known: whatever partial usage came back, the previous call's input
// count for the prompt that was certainly sent, and the configured estimator
// over the text that actually streamed. Estimated marks the row, so a bill
// built from these rows can say which lines are guesses.
func unfinishedUsage(input LoopInput, step int, s *StepResult, outcome ports.UsageOutcome, lastInput int) ports.NewUsage {
	u := usageOf(input, step, ports.KindStep, s, outcome)
	if u.InputTokens == 0 {
		u.InputTokens, u.Estimated = lastInput, true
	}
	if u.OutputTokens == 0 && s.StreamedText != "" {
		// The same estimator compaction measures context fill with, so the
		// context budget and the cost of a cut-off step follow one rule.
		u.OutputTokens, u.Estimated = estimateTokens([]byte(s.StreamedText)), true
	}
	return u
}

// stepFinishedPayload is the STEP_FINISHED notice: the step record, with the
// main agent's stream named null rather than "", as the TS runtime sends it.
func stepFinishedPayload(marker ports.StepRecord) map[string]any {
	raw, _ := json.Marshal(marker)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	out["agentId"] = nullable(marker.AgentID)
	return out
}
