package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// ValidateTokenBudget: the safety cap (§2.1) must be either zero (unbounded
// apart from MaxSteps) or positive.
func ValidateTokenBudget(value int, label string) error {
	if value < 0 {
		return fmt.Errorf("%s must be zero or a positive number", label)
	}
	return nil
}

// MarkRequiresConfirmation marks a tool the engine treats as destructive:
// parked behind ParkForApproval (§2.5) instead of executing directly.
func MarkRequiresConfirmation(t ports.Tool) ports.Tool {
	t.RequiresConfirmation = true
	return t
}

// RequireConfirmation is MarkRequiresConfirmation for a plain goai tool.
func RequireConfirmation(t goai.Tool) ports.Tool {
	return MarkRequiresConfirmation(ports.WrapTool(t))
}

// verdictReady asks whether an approval is settled yet (§2.7). Read-only on
// purpose: with several open at once, nothing may be executed until EVERY
// one is ready, or a redelivery would run half of them and then leave the
// thread parked with those verdicts already consumed.
func verdictReady(ctx context.Context, deps ports.RuntimePorts, pending PendingHitl) (string, error) {
	if _, found, err := deps.Kv.Get(ctx, HitlKey(pending.ToolCallID)); err != nil {
		return "", err
	} else if found {
		return "answered", nil
	}
	if !time.Now().Before(pending.Deadline(deps.Config)) {
		return "expired", nil
	}
	return "open", nil
}

// settleVerdict turns a settled approval into the tool result the
// conversation will carry (§2.5): run the approved tool, record the denial,
// or convert an expired request into the timeout denial.
//
// A tool failure is surfaced TO THE MODEL as the tool result, so the
// conversation always stays executable.
func settleVerdict(ctx, genCtx context.Context, deps ports.RuntimePorts, threadID string, pending PendingHitl, target *ports.Tool, state ports.AgentRunState) (json.RawMessage, bool, error) {
	raw, found, err := deps.Kv.Get(ctx, HitlKey(pending.ToolCallID))
	if err != nil {
		return nil, false, err
	}
	if err := deps.Kv.Del(ctx, HitlKey(pending.ToolCallID)); err != nil {
		return nil, false, err
	}
	if !found {
		if _, err := Publish(ctx, deps, threadID, "INPUT_EXPIRED", map[string]any{"toolCallId": pending.ToolCallID}); err != nil {
			return nil, false, err
		}
		return MarshalPayload(map[string]any{"responded": false, "cancelled": true, "reason": "timeout"}), true, nil
	}
	var answer struct {
		Approved bool            `json:"approved"`
		Payload  json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal([]byte(raw), &answer); err != nil {
		return nil, false, fmt.Errorf("hitl answer for %s: %w", pending.ToolCallID, err)
	}
	if !answer.Approved {
		return MarshalPayload(map[string]any{"denied": true}), false, nil
	}
	if target == nil || target.Execute == nil {
		return MarshalPayload(map[string]any{"error": "Unknown tool: " + pending.ToolName}), false, nil
	}
	args := pending.Arguments
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	// The resumed tool gets the same context a live one does (§2.10), plus
	// what the human sent back with the approval.
	toolCtx := ContextWithPublisher(ContextWithRunState(genCtx, state), ThreadPublisher(deps, threadID))
	toolCtx = ContextWithApproval(ContextWithToolCallID(toolCtx, pending.ToolCallID), Approval{Payload: answer.Payload})
	output, err := target.Execute(toolCtx, args)
	if err != nil {
		return MarshalPayload(map[string]any{"error": err.Error()}), false, nil
	}
	return jsonOrString(output), false, nil
}

// unwindVerdict lands a settled verdict and unwinds whatever was waiting on
// it (§2.7). The verdict belongs to the stream that asked: the main agent's,
// or a nested run's. When a nested run asked, its own loop is re-entered
// from its persisted turns and its result is handed to the call waiting one
// level up, repeating until the main agent's spawnSubagent call is answered.
//
// Returns false when the unwind parked again: the thread stays
// WAITING_FOR_INPUT and a later dispatch picks up from the new request.
func unwindVerdict(ctx, genCtx context.Context, deps ports.RuntimePorts, threadID string, pending PendingHitl, result json.RawMessage, subCtx *SubagentCtx) (bool, error) {
	if _, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
		Role: ports.RoleTool, AgentID: pending.AgentID,
		Content: ToolResultContent(pending.ToolCallID, pending.ToolName, result),
	}); err != nil {
		return false, err
	}
	// producer is whoever must now run to produce the next result. Nil means
	// the main agent, whose loop the caller re-enters itself.
	producer := pending.Nested
	for i, frame := range pending.Frames {
		if producer == nil || subCtx == nil {
			break
		}
		outcome, err := RunNestedAgent(genCtx, subCtx, *producer, nil, pending.Frames[i:])
		if err != nil {
			return false, err
		}
		if outcome.Parked || outcome.Aborted {
			return false, nil // parked again one level down, or a user stop mid-unwind (§2.1)
		}
		if run, err := deps.Admin.Runs().Get(ctx, producer.AgentID); err == nil && run != nil {
			completed := ports.StateCompleted
			closeNested(ctx, subCtx, run, outcome, ports.RunPatch{
				State: &completed, Result: MarshalPayload(map[string]any{"text": outcome.Text}),
			})
		}
		if _, err := Publish(ctx, deps, threadID, "SUBAGENT_COMPLETED", map[string]any{"agentId": producer.AgentID}); err != nil {
			return false, err
		}
		// Hand the capped result to the call one level up (§2.6)
		if _, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
			Role: ports.RoleTool, AgentID: frame.AgentID,
			Content: ToolResultContent(frame.ToolCallID, "spawnSubagent", map[string]any{
				"agentId": producer.AgentID,
				"result":  capRunes(outcome.Text, deps.Config.SubagentResultCapChars),
			}),
		}); err != nil {
			return false, err
		}
		producer = frame.Nested
	}
	return true, nil
}

// closeRunRecord sums this segment onto the run's record and stamps how it
// ended (§2.9). Observability must never fail a run that otherwise
// succeeded, so errors are swallowed.
func closeRunRecord(ctx context.Context, deps ports.RuntimePorts, runID string, f FinalizeInput) {
	prior, err := deps.Admin.Runs().Get(ctx, runID)
	if err != nil || prior == nil {
		return // a run started elsewhere, or a foreign dispatch
	}
	endedAt := time.Now()
	patch := ports.RunPatch{
		State: &f.State, StopReason: ports.Ptr(f.StopReason), EndedAt: &endedAt,
		DurationMs:        ports.Ptr(endedAt.Sub(prior.StartedAt).Milliseconds()),
		Steps:             ports.Ptr(prior.Steps + f.Steps),
		InputTokens:       ports.Ptr(prior.InputTokens + f.Attribution.InputTokens),
		CachedInputTokens: ports.Ptr(prior.CachedInputTokens + f.Attribution.CachedInputTokens),
		OutputTokens:      ports.Ptr(prior.OutputTokens + f.Attribution.OutputTokens),
		TotalTokens:       ports.Ptr(prior.TotalTokens + f.Attribution.TotalTokens),
	}
	if f.Error != "" {
		patch.Error = ports.Ptr(f.Error)
	}
	_ = deps.Admin.Runs().Patch(ctx, runID, patch)
}

// accrueRunRecord adds a parked segment's steps and tokens onto the run's
// record (§2.9) without closing it. Best effort, like every admin write.
func accrueRunRecord(ctx context.Context, deps ports.RuntimePorts, runID string, loop *LoopOutcome) {
	prior, err := deps.Admin.Runs().Get(ctx, runID)
	if err != nil || prior == nil {
		return
	}
	patch := ports.RunPatch{
		Steps:             ports.Ptr(prior.Steps + loop.Steps),
		InputTokens:       ports.Ptr(prior.InputTokens + loop.Attribution.InputTokens),
		CachedInputTokens: ports.Ptr(prior.CachedInputTokens + loop.Attribution.CachedInputTokens),
		OutputTokens:      ports.Ptr(prior.OutputTokens + loop.Attribution.OutputTokens),
		TotalTokens:       ports.Ptr(prior.TotalTokens + loop.Attribution.TotalTokens),
	}
	_ = deps.Admin.Runs().Patch(ctx, runID, patch)
}

// failRun finalises a run as FAILED on both homes AND keeps why (§2.9).
//
// The spec's OnSettle still runs (§5.6): a caller that opened records for
// this run must get to close them as failed. Its own error cannot change
// the outcome, which is already a failure.
func failRun(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, threadID, runID, reason string) error {
	if agent != nil && agent.Args.OnSettle != nil {
		// A failed run still spent money on the steps it did make (§4).
		bill, billErr := runBill(ctx, deps, threadID, runID)
		_ = agent.Args.OnSettle(ctx, ports.RunFinishInfo{
			ThreadID: threadID, RunID: runID, State: ports.StateFailed, StopReason: "failed", Error: reason,
			Usage: bill, UsageErr: billErr,
		})
	}
	if _, err := deps.Kv.Set(ctx, StateKey(threadID), string(ports.StateFailed), ports.SetOptions{}); err != nil {
		return err
	}
	if err := SetThreadState(ctx, deps, threadID, ports.StateFailed, ""); err != nil {
		return err
	}
	if runID != "" {
		closeRunRecord(ctx, deps, runID, FinalizeInput{
			State: ports.StateFailed, StopReason: "completed", Error: reason, RunID: runID,
		})
	}
	_, err := Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{"state": ports.StateFailed, "error": reason})
	return err
}

func findTool(tools []ports.Tool, name string) *ports.Tool {
	for i := range tools {
		if tools[i].Name == name {
			return &tools[i]
		}
	}
	return nil
}

// resumePendingHitl resolves every parked request at segment start (§2.5,
// §2.7) and flips the thread back to RUNNING. Returns false when at least
// one approval is still open within its TTL: the dispatch is an
// at-least-once redelivery and the thread stays parked. rawTools must be the
// UNWRAPPED main toolset.
func resumePendingHitl(ctx, genCtx context.Context, deps ports.RuntimePorts, threadID string, open []PendingHitl, rawTools []ports.Tool, subCtx *SubagentCtx, state ports.AgentRunState) (bool, error) {
	// Readiness first, side effects second: the thread resumes only when
	// EVERY open approval has been answered or has expired (§2.7).
	for _, p := range open {
		st, err := verdictReady(ctx, deps, p)
		if err != nil {
			return false, err
		}
		if st == "open" {
			return false, nil // redelivery no-op (§2.8)
		}
	}
	for _, pending := range open {
		// A nested run's tools come from the delegation config, not the main
		// agent's set: the approved tool has to be resolved where it lives.
		var target *ports.Tool
		if pending.AgentID == "" {
			target = findTool(rawTools, pending.ToolName)
		} else if subCtx != nil {
			target = findTool(nestedRawTools(subCtx, pending.Nested), pending.ToolName)
		}
		result, _, err := settleVerdict(ctx, genCtx, deps, threadID, pending, target, state)
		if err != nil {
			return false, err
		}
		ok, err := unwindVerdict(ctx, genCtx, deps, threadID, pending, result, subCtx)
		if err != nil || !ok {
			return false, err
		}
	}
	if _, err := deps.Kv.Set(ctx, StateKey(threadID), string(ports.StateRunning), ports.SetOptions{}); err != nil {
		return false, err
	}
	if err := SetThreadState(ctx, deps, threadID, ports.StateRunning, ""); err != nil {
		return false, err
	}
	if _, err := Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{"state": ports.StateRunning}); err != nil {
		return false, err
	}
	return true, nil
}

// ExecuteInput is what the worker hands the engine.
type ExecuteInput struct {
	ThreadID string
	Model    string
	// RunID is this dispatch's run id (§2.1). A job without one keeps the
	// old behavior: no staleness check, and no redrive on a lock conflict.
	RunID string
	// EnqueuedAt is epoch ms at enqueue, for the queue-wait measurement (§2.9).
	EnqueuedAt int64
	// State is the run's state (§2.10), carried so a redrive keeps it.
	State       ports.AgentRunState
	TokenBudget int
	// CostBudgetMicros is the run's money cap (§4), carried on the dispatch
	// so the worker enforces what the caller asked for.
	CostBudgetMicros int64
	ProviderOptions  ports.ProviderOptions
	// MaxSteps is the run's own step cap; zero keeps the config's (§2.1).
	MaxSteps int
}

// ExecuteOutcome says what Execute did.
type ExecuteOutcome string

const (
	// OutcomeExecuted: this worker ran the segment (or it was a legitimate no-op).
	OutcomeExecuted ExecuteOutcome = "executed"
	// OutcomeLockConflict: someone else holds the thread's run lock; nothing ran.
	OutcomeLockConflict ExecuteOutcome = "lock-conflict"
	// OutcomeStale: a NEWER run owns the thread; this job must do nothing.
	OutcomeStale ExecuteOutcome = "stale"
)

// Execute is the engine (§2.1, §5.6). Worker-side only: runs are dispatched
// via the queue (§2.8) and may outlive any HTTP response.
//
// Execution is a platform-owned loop of single-round-trip steps: after EVERY
// step the produced messages are persisted, so a worker that dies mid-run
// resumes from the last step, and every continuation decision is made
// between steps, never inside goai.
//
// Concurrency: acquires the per-thread run lock (SET NX + lease) before any
// work. Two workers can never run one thread, and a crashed worker's lock
// expires instead of blocking forever (§3.4).
func Execute(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, input ExecuteInput) (ExecuteOutcome, error) {
	threadID, runID := input.ThreadID, input.RunID
	if err := ValidateTokenBudget(input.TokenBudget, "tokenBudget"); err != nil {
		return "", err
	}
	if input.CostBudgetMicros < 0 {
		return "", fmt.Errorf("costBudgetMicros must be zero or a positive number")
	}
	// The run's identity and state ride the context from here, so the model
	// calls (and anything wrapping a model) can see whose run they serve.
	ctx = ContextWithRunState(ContextWithRunID(ctx, runID), input.State)

	// True once the thread has started a NEWER run than this one (§2.1).
	stale := func() (bool, error) {
		if runID == "" {
			return false, nil
		}
		current, _, err := deps.Kv.Get(ctx, RunIDKey(threadID))
		return current != runID, err
	}

	// The lock carries the run id, so a later conflict can tell a duplicate
	// delivery of THIS run apart from an older run that is still finishing.
	lockValue := runID
	if lockValue == "" {
		lockValue = NewID()
	}
	locked, err := deps.Kv.Set(ctx, RunLockKey(threadID), lockValue, ports.SetOptions{
		OnlyIfNotExists: true, Expiry: deps.Config.RunLockLease,
	})
	if err != nil {
		return "", err
	}
	if !locked {
		return OutcomeLockConflict, nil // another worker owns this thread (§2.8)
	}
	// Release on success, failure, or stop.
	defer func() { _ = deps.Kv.Del(context.WithoutCancel(ctx), RunLockKey(threadID)) }()

	// Token budget (§2.1 safety cap): execute input → spec → config. Checked
	// BETWEEN steps: the finished step is always kept in full.
	tokenBudget := input.TokenBudget
	if tokenBudget == 0 {
		tokenBudget = agent.Spec.TokenBudget
	}
	if tokenBudget == 0 {
		tokenBudget = deps.Config.TokenBudget
	}
	// The money cap (§4) resolves the same way, widest last.
	costBudget := input.CostBudgetMicros
	if costBudget == 0 {
		costBudget = agent.Spec.CostBudgetMicros
	}
	if costBudget == 0 {
		costBudget = deps.Config.CostBudgetMicros
	}
	// Provider-specific options (§3.1), widest first: runtime config → agent
	// spec → this run. Each wins over the one before it, per namespace.
	providerOptions := ports.MergeProviderOptions(
		ports.MergeProviderOptions(deps.Config.ProviderOptions, agent.Spec.ProviderOptions),
		input.ProviderOptions)

	// Two ways a run ends early, one behavior: everything tears down at once.
	//   1. the state key reads CANCELLED: the user pressed stop (§2.1);
	//   2. the run id has moved on: the user pressed stop and then sent another
	//      message, which put RUNNING back over CANCELLED before this poll
	//      could read it. The state key lies in that window; the run id never does.
	genCtx, cancel := context.WithCancel(ctx)
	var abortedFlag atomic.Bool
	aborted := func() bool { return abortedFlag.Load() }
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		ticker := time.NewTicker(deps.Config.StopPoll)
		defer ticker.Stop()
		for {
			select {
			case <-genCtx.Done():
				return
			case <-ticker.C:
				state, _, err := deps.Kv.Get(ctx, StateKey(threadID))
				if err != nil {
					continue // transient kv errors must never kill the poller
				}
				replaced, _ := stale()
				if state == string(ports.StateCancelled) || replaced {
					abortedFlag.Store(true)
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-pollDone
	}()

	// A newer run already owns this thread: this job has nothing to do, and
	// must not touch state on the live run's behalf (§2.1).
	if replaced, err := stale(); err != nil {
		return "", err
	} else if replaced {
		return OutcomeStale, nil
	}
	// At-least-once idempotency (§2.8): a job whose run already ended, or was
	// stopped, must be a no-op on redelivery. A MISSING thread is the same
	// no-op: it was deleted (§3.2) and must never be resurrected.
	durable, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil {
		return "", err
	}
	if durable == nil || durable.State == ports.StateCancelled ||
		durable.State == ports.StateCompleted || durable.State == ports.StateFailed {
		return OutcomeExecuted, nil
	}

	// Step ceiling (§2.1): the run's own cap when it set one, the config's
	// otherwise, and never above the config's.
	maxSteps := deps.Config.MaxSteps
	if input.MaxSteps > 0 && input.MaxSteps < maxSteps {
		maxSteps = input.MaxSteps
	}
	resume := ports.ResumeInfo{
		Agent: agent.Name, Model: input.Model, RunID: runID,
		TokenBudget: input.TokenBudget, CostBudgetMicros: input.CostBudgetMicros,
		ProviderOptions: providerOptions,
		// Carried so the resumed segment scopes its storage the same way (§2.10).
		State: input.State, MaxSteps: input.MaxSteps,
	}
	// One ledger for the whole run: a nested run's spend counts against the
	// same safety cap the main agent is checked against (§2.7).
	ledger := &RunLedger{}

	// How long the dispatch sat in the queue before a worker took it (§2.9).
	if runID != "" && input.EnqueuedAt > 0 {
		_ = deps.Admin.Runs().Patch(ctx, runID, ports.RunPatch{
			QueuedMs: ports.Ptr(time.Now().UnixMilli() - input.EnqueuedAt),
		})
	}

	// Platform-owned toolset: HITL (§2.5) over the user's set; spawnSubagent
	// added ONLY when the spec opts in (§2.7). rawTools keeps the real
	// implementations: the resolved park executes the approved tool.
	var subCtx *SubagentCtx
	if agent.Spec.Subagents != nil {
		subCtx = &SubagentCtx{
			IOCtx: ctx, ThreadID: threadID, Depth: 0, Sem: agent.Sem, Ports: deps,
			Sub: *agent.Spec.Subagents, Agent: agent, Ledger: ledger, Resume: resume,
			TokenBudget: tokenBudget, CostBudgetMicros: costBudget, BillingRunID: runID,
			ProviderOptions: providerOptions, Aborted: aborted, State: input.State,
		}
	}
	rawTools := slices.Clone(agent.Args.Tools)
	if subCtx != nil {
		rawTools = append(rawTools, SpawnSubagentTool(subCtx))
	}
	// The main agent's own toolset: nothing is waiting on its parks (§2.7).
	// Every tool also sees the run's state (§2.10) and can publish its own
	// events on the thread.
	tools := WithRunState(WithPublishEvent(deps, threadID, WithHitl(deps, threadID, rawTools, HitlCtx{Resume: resume})), input.State)

	// §2.5 resume: a WAITING thread at segment start is either the /respond
	// continuation or a redelivery of the original job while still parked.
	if durable.State == ports.StateWaitingForInput {
		open, err := LoadOpenHitls(ctx, deps, threadID)
		if err != nil {
			return "", err
		}
		if len(open) == 0 {
			// WAITING without a pending request cannot be continued: fail into
			// the §2.8 policy rather than corrupting the conversation.
			return "", fmt.Errorf("thread %s is WAITING_FOR_INPUT without a pending INPUT_REQUIRED", threadID)
		}
		resumed, err := resumePendingHitl(ctx, genCtx, deps, threadID, open, rawTools, subCtx, input.State)
		if err != nil {
			return "", err
		}
		if !resumed {
			return OutcomeExecuted, nil // still parked, nothing to do yet
		}
	}

	// Durable compaction pass: history always fits the model budget (§2.6)
	history, err := CompactContext(ctx, deps, threadID, input.Model)
	if err != nil {
		return "", err
	}
	model, err := deps.ResolveModel(input.Model)
	if err != nil {
		return "", err
	}
	// Prompt caching (§2.6): stamp the stable prefix once; appended step
	// messages extend the prompt without invalidating the breakpoints.
	messages := RepairDanglingToolCalls(MessagesFromDTOs(history))
	if deps.Config.PromptCaching {
		messages = MarkPromptCaching(messages)
	}

	loop, err := RunLoop(ctx, deps, agent, threadID, LoopInput{
		AgentID: "", RunID: runID, Kind: agent.Kind, Model: model.Instance(),
		Messages: messages, Tools: tools, MaxSteps: maxSteps,
		GenCtx: genCtx, Aborted: aborted,
		ProviderOptions: providerOptions, TokenBudget: tokenBudget,
		SystemFn: agent.Args.SystemFn, PrepareStep: agent.Args.PrepareStep, State: input.State,
		CostBudgetMicros: costBudget, BillingRunID: runID,
		ModelKey: input.Model, ModelID: model.WireID(input.Model), AgentName: agent.Name,
		CacheSystemPrompt: deps.Config.PromptCaching,
		OnChunk: func(chunk provider.StreamChunk) {
			// One canonical path for every client: durable log + live bus (§2.1, §2.2)
			_, _ = Publish(ctx, deps, threadID, "CHUNK", ChunkPayload(chunk))
			if agent.Args.OnChunk != nil {
				agent.Args.OnChunk(chunk) // user callback still fires
			}
		},
	}, ledger)
	if err != nil {
		return "", err
	}

	if loop.Parked {
		// The segment ends holding the park. Every call it made was already
		// recorded and priced as it happened (§4), so there is nothing left to
		// bill here. NO state flip. The run record accrues this segment's
		// steps, tokens and cost now, so the close after the resume sums every
		// segment rather than the last.
		if runID != "" {
			accrueRunRecord(ctx, deps, runID, loop)
		}
		return OutcomeExecuted, nil
	}

	stopReason := "completed"
	state := ports.StateCompleted
	switch {
	case aborted():
		stopReason, state = "cancelled", ports.StateCancelled
	case loop.CostExhausted:
		stopReason = "cost_budget" // the money cap (§4)
	case tokenBudget > 0 && ledger.TokensUsed() >= tokenBudget:
		stopReason = "token_budget"
	case loop.FinishReason == provider.FinishToolCalls:
		stopReason = "max_steps" // step ceiling hit (§2.1)
	}
	f := FinalizeInput{
		State: state, StopReason: stopReason, TokensUsed: ledger.TokensUsed(),
		Attribution: loop.Attribution, RunID: runID, Steps: loop.Steps,
	}
	if agent.Kind == ports.KindGenerateText {
		text := loop.Text
		f.OneShotText = &text
	}
	// The caller settles BEFORE the terminal state lands (§5.6): what the run
	// produced is committed by the time any client sees it end. A settle
	// failure is a run failure; a stop reaches the hook cancelled, on the
	// generation context, so it can tell the two apart.
	// The whole run's bill, read back from the rows the loop wrote (§4):
	// every segment and every nested run, priced and grouped into lines, so a
	// settle hook charges in one pass without keeping its own tally. Read
	// once, handed to both hooks; a failed read is reported, not hidden.
	bill, billErr := runBill(ctx, deps, threadID, runID)
	if agent.Args.OnSettle != nil {
		info := ports.RunFinishInfo{
			ThreadID: threadID, RunID: runID, State: state, StopReason: stopReason,
			TokensUsed: f.TokensUsed, Attribution: f.Attribution, Steps: f.Steps,
			Cancelled: state == ports.StateCancelled,
			Usage:     bill, UsageErr: billErr,
		}
		if err := agent.Args.OnSettle(genCtx, info); err != nil && state != ports.StateCancelled {
			state = ports.StateFailed
			f.State = state
			f.Error = err.Error()
		}
	}
	if err := Finalize(ctx, deps, agent, threadID, f); err != nil {
		return "", err
	}
	if agent.Args.OnFinish != nil {
		agent.Args.OnFinish(ports.RunFinishInfo{
			ThreadID: threadID, RunID: runID, State: state, StopReason: stopReason,
			TokensUsed: f.TokensUsed, Attribution: f.Attribution, Steps: f.Steps,
			Cancelled: state == ports.StateCancelled, Error: f.Error,
			Usage: bill, UsageErr: billErr,
		})
	}
	return OutcomeExecuted, nil
}

// FinalizeInput is how a run ended.
type FinalizeInput struct {
	State ports.ExecutionState
	// StopReason is 'completed' | 'token_budget' | 'max_steps' | 'cancelled'.
	StopReason  string
	TokensUsed  int
	Attribution TokenAttribution
	// OneShotText: generate-text flavor only, published as one TEXT_RESULT.
	OneShotText *string
	// RunID is the run this finalize speaks for (§2.1). State is written only
	// while that run is still the thread's current one.
	RunID string
	// Steps is the loop iterations this segment completed (§2.9).
	Steps int
	// Error is why it failed, when it did (§2.9).
	Error string
}

// Finalize finalizes a finished run (§5.6): attribute the segment's tokens
// (§4), then flip state on both homes and publish. Message persistence
// already happened per step inside the loop.
//
// A budget break is NOT a user stop: the run completes with stopReason
// 'token_budget' and the usage it actually spent.
func Finalize(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, threadID string, f FinalizeInput) error {
	// Nothing to bill here: every model call recorded and priced its own row
	// as it happened, inside the loop (§4). Even a run that was replaced
	// part-way through has already had its calls written.
	//
	// Close the run's durable record (§2.9). Additive: a run that parked and
	// resumed finalises once, but its steps and tokens accrued over several
	// segments. The run lock (§3.4) makes this read-modify-write single-writer.
	if f.RunID != "" {
		closeRunRecord(ctx, deps, f.RunID, f)
		// Past that, a replaced run stays silent. Its CANCELLED would otherwise
		// land on top of the next run's RUNNING and wedge the thread (§2.1).
		current, _, err := deps.Kv.Get(ctx, RunIDKey(threadID))
		if err != nil {
			return err
		}
		if current != f.RunID {
			return nil
		}
	}
	if f.OneShotText != nil {
		// One-shot flavor: no CHUNK stream; publish the final text as one event
		if _, err := Publish(ctx, deps, threadID, "TEXT_RESULT", map[string]any{"text": *f.OneShotText}); err != nil {
			return err
		}
	}
	if _, err := deps.Kv.Set(ctx, StateKey(threadID), string(f.State), ports.SetOptions{}); err != nil {
		return err
	}
	if err := SetThreadState(ctx, deps, threadID, f.State, ""); err != nil {
		return err
	}
	terminal := map[string]any{
		"state": f.State, "stopReason": f.StopReason, "tokensUsed": f.TokensUsed, "usage": f.Attribution,
	}
	if f.Error != "" {
		terminal["error"] = f.Error
	}
	_, err := Publish(ctx, deps, threadID, "STATE_CHANGE", terminal)
	return err
}

// redriveOnLockConflict: a lock conflict has two very different causes
// (§2.8), and only one of them is a no-op:
//
//   - the lock carries THIS run's id → an at-least-once duplicate of a job
//     that is already executing. Drop it.
//   - the lock belongs to an OLDER run that has not finished tearing down →
//     this job never ran. Dropping it strands the message the user just sent,
//     so come back once the lock clears. Bounded by maxAttempts, then FAILED.
func redriveOnLockConflict(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, input ExecuteInput, maxAttempts int) error {
	if input.RunID == "" {
		return nil // legacy dispatch, no identity: old drop behavior
	}
	if holder, _, err := deps.Kv.Get(ctx, RunLockKey(input.ThreadID)); err != nil {
		return err
	} else if holder == input.RunID {
		// The lock is held by THIS run. While its segment is running that is
		// a duplicate delivery, and a no-op. But a park hands the same run id
		// to two later deliveries, the approval's answer and its expiry
		// (§2.5), and either can arrive while the parking segment is still
		// winding down and holding the lock. Dropping that one would leave
		// the thread waiting forever: nobody re-sends an expiry. The thread's
		// durable state tells the two cases apart, because ParkForApproval
		// writes WAITING_FOR_INPUT before the segment ends.
		durable, err := deps.Storage.Threads.Get(ctx, input.ThreadID)
		if err != nil {
			return err
		}
		if durable == nil || durable.State != ports.StateWaitingForInput {
			return nil // own duplicate
		}
	}
	if current, _, err := deps.Kv.Get(ctx, RunIDKey(input.ThreadID)); err != nil {
		return err
	} else if current != input.RunID {
		return nil // already replaced
	}
	tries, err := deps.Kv.Incr(ctx, RedriveKey(input.ThreadID))
	if err != nil {
		return err
	}
	if tries <= int64(maxAttempts) {
		return deps.Queue.Enqueue(ctx, ports.RunJob{
			ThreadID: input.ThreadID, RunID: input.RunID, EnqueuedAt: time.Now().UnixMilli(),
			Model: input.Model, Agent: agent.Name, TokenBudget: input.TokenBudget,
			// A redrive is the SAME run trying again, so it keeps the caps it
			// was dispatched with: a retry that lost its money cap would be
			// unbounded (§4).
			CostBudgetMicros: input.CostBudgetMicros,
			ProviderOptions:  input.ProviderOptions, State: input.State, MaxSteps: input.MaxSteps,
		}, &ports.EnqueueOptions{Delay: deps.Config.RunRedriveDelay})
	}
	if err := deps.Kv.Del(ctx, RedriveKey(input.ThreadID)); err != nil {
		return err
	}
	return failRun(ctx, deps, agent, input.ThreadID, input.RunID, "the run lock never cleared")
}

// ExecuteFunc is the signature of Execute, an injection seam for tests.
type ExecuteFunc func(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, input ExecuteInput) (ExecuteOutcome, error)

// Policy tunes ExecuteWithPolicy. Zero MaxAttempts means the config's; nil
// Exec means Execute.
type Policy struct {
	MaxAttempts int
	Exec        ExecuteFunc
}

// ExecuteWithPolicy is the §2.8 failure policy: transient errors redrive
// through the queue; exhausted attempts finalize FAILED (hot cache +
// durable). A user stop is never retried, and a successful run resets the
// attempt counter.
//
// It returns nil once the outcome has been handled, whether the run ran,
// was redriven, or was finalized FAILED. It returns an error only when the
// policy itself could not be applied (a port failed).
func ExecuteWithPolicy(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, input ExecuteInput, policy *Policy) error {
	maxAttempts := deps.Config.RunMaxAttempts
	exec := ExecuteFunc(Execute)
	if policy != nil {
		if policy.MaxAttempts > 0 {
			maxAttempts = policy.MaxAttempts
		}
		if policy.Exec != nil {
			exec = policy.Exec
		}
	}
	outcome, err := exec(ctx, deps, agent, input)
	if err == nil {
		switch outcome {
		case OutcomeExecuted:
			// Only a run THIS worker executed may reset the retry budget (§2.8)
			if err := deps.Kv.Del(ctx, AttemptsKey(input.ThreadID)); err != nil {
				return err
			}
			return deps.Kv.Del(ctx, RedriveKey(input.ThreadID))
		case OutcomeStale:
			return nil // a newer run owns the thread: this job is a genuine no-op
		default:
			return redriveOnLockConflict(ctx, deps, agent, input, maxAttempts)
		}
	}

	// A user stop already finalized the thread: never retry a stop
	if state, _, kvErr := deps.Kv.Get(ctx, StateKey(input.ThreadID)); kvErr != nil {
		return errors.Join(err, kvErr)
	} else if state == string(ports.StateCancelled) {
		return nil
	}
	attempts, kvErr := deps.Kv.Incr(ctx, AttemptsKey(input.ThreadID))
	if kvErr != nil {
		return errors.Join(err, kvErr)
	}
	if attempts < int64(maxAttempts) {
		// A retry is the SAME run trying again (§2.1): it keeps the id, so it
		// can notice it was replaced and redrive if it finds the lock held.
		return deps.Queue.Enqueue(ctx, ports.RunJob{
			ThreadID: input.ThreadID, RunID: input.RunID, EnqueuedAt: time.Now().UnixMilli(),
			Model: input.Model, Agent: agent.Name, TokenBudget: input.TokenBudget,
			// A redrive is the SAME run trying again, so it keeps the caps it
			// was dispatched with: a retry that lost its money cap would be
			// unbounded (§4).
			CostBudgetMicros: input.CostBudgetMicros,
			ProviderOptions:  input.ProviderOptions, State: input.State, MaxSteps: input.MaxSteps,
		}, nil)
	}
	// Attempts exhausted: finalize FAILED on BOTH the hot cache and durable
	// truth, or subsequent runs would still treat the thread as active (§2.1)
	if failErr := failRun(ctx, deps, agent, input.ThreadID, input.RunID, err.Error()); failErr != nil {
		return errors.Join(err, failErr)
	}
	return deps.Kv.Del(ctx, AttemptsKey(input.ThreadID))
}

// runBill sums every model call a run made, nested runs included (§4). A
// storage hiccup must not turn a finished run into a failed one, so the
// read never fails the caller; but a hook that bills from these totals must
// be able to tell "spent nothing" from "could not read", so the error is
// returned beside the zero totals rather than swallowed.
func runBill(ctx context.Context, deps ports.RuntimePorts, threadID, runID string) (ports.UsageTotals, error) {
	if runID == "" {
		return ports.UsageTotals{}, nil
	}
	total, err := deps.Storage.Usage.Total(context.WithoutCancel(ctx), threadID, ports.UsageFilter{RunID: runID})
	if err != nil {
		Logger(deps).Error("run bill not read", "run", runID, "err", err)
		return ports.UsageTotals{}, err
	}
	return total, nil
}
