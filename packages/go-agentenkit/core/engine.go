package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
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
	if pending.Landed {
		return "answered", nil // its result is in the history already
	}
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
// or convert an expired request into the timeout denial. It reports whether
// the request expired.
//
// Nothing here is consumed: the answer stays until its result is in the
// history (see landVerdict). An approved tool's output is kept under
// HitlDoneKey the moment it returns, so a worker that dies before the result
// is saved does not run the tool a second time on the retry: the retry
// reuses the output.
//
// A tool failure, or a panic, is surfaced TO THE MODEL as the tool result,
// so the conversation always stays executable.
func settleVerdict(ctx, genCtx context.Context, deps ports.RuntimePorts, threadID string, pending PendingHitl, target *ports.Tool, state ports.AgentRunState, toolRun ToolRun) (json.RawMessage, bool, error) {
	raw, found, err := deps.Kv.Get(ctx, HitlKey(pending.ToolCallID))
	if err != nil {
		return nil, false, err
	}
	if !found {
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
	if done, found, err := deps.Kv.Get(ctx, HitlDoneKey(pending.ToolCallID)); err != nil {
		return nil, false, err
	} else if found {
		return json.RawMessage(done), false, nil // it already ran; its result was never saved
	}
	args := pending.Arguments
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	// The resumed tool gets the same context a live one does (§2.10), plus
	// what the human sent back with the approval.
	toolCtx := ContextWithPublisher(ContextWithRunState(genCtx, state), ThreadPublisher(deps, threadID))
	toolCtx = ContextWithApproval(ContextWithToolCallID(toolCtx, pending.ToolCallID), Approval{Payload: answer.Payload})
	toolCtx = ContextWithToolRun(toolCtx, toolRun)
	var output string
	result := json.RawMessage(nil)
	if err := CallSafely(func() error {
		var err error
		output, err = target.Execute(toolCtx, args)
		return err
	}); err != nil {
		result = MarshalPayload(map[string]any{"error": err.Error()})
	} else {
		result = jsonOrString(output)
	}
	if _, err := deps.Kv.Set(ctx, HitlDoneKey(pending.ToolCallID), string(result), ports.SetOptions{Expiry: hitlDoneTTL}); err != nil {
		return nil, false, err
	}
	return result, false, nil
}

// hitlDoneTTL is how long an approved tool's output is kept for a retry
// that has to land it: far past any retry backoff.
const hitlDoneTTL = 24 * time.Hour

// landVerdict clears a verdict once its result is in the history (§2.5):
// the answer and the kept output go, and an expiry is published. Before
// that, a retry must still find both.
func landVerdict(ctx context.Context, deps ports.RuntimePorts, threadID string, pending PendingHitl, expired bool) error {
	if expired {
		if _, err := Publish(ctx, deps, threadID, "INPUT_EXPIRED", map[string]any{"toolCallId": pending.ToolCallID}); err != nil {
			return err
		}
	}
	if err := deps.Kv.Del(ctx, HitlKey(pending.ToolCallID)); err != nil {
		return err
	}
	return deps.Kv.Del(ctx, HitlDoneKey(pending.ToolCallID))
}

// unwindVerdict lands a settled verdict and unwinds whatever was waiting on
// it (§2.7). The verdict belongs to the stream that asked: the main agent's,
// or a nested run's. When a nested run asked, its own loop is re-entered
// from its persisted turns and its result is handed to the call waiting one
// level up, repeating until the main agent's spawnSubagent call is answered.
// A park that already landed (see PendingHitl.Landed) carries on from the
// first level still waiting.
//
// A child that fails on the way up is reported to the level above as the
// delegation's result, exactly as a live spawnSubagent reports it, so one
// failed child never leaves the whole thread stuck waiting.
//
// Returns false when the unwind parked again, or a user stopped it: the
// thread stays as it is and a later dispatch picks up from there.
func unwindVerdict(ctx, genCtx context.Context, deps ports.RuntimePorts, threadID string, pending PendingHitl, result json.RawMessage, expired bool, subCtx *SubagentCtx) (bool, error) {
	start := 0
	producer := pending.Nested
	if pending.Landed {
		start = pending.ResumeFrame
		if start > 0 {
			producer = pending.Frames[start-1].Nested
		}
	} else {
		if _, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
			Role: ports.RoleTool, AgentID: pending.AgentID,
			Content: ToolResultContent(pending.ToolCallID, pending.ToolName, result),
		}); err != nil {
			return false, err
		}
		streamToolResult(ctx, deps, threadID, pending.AgentID, pending.ToolCallID, pending.ToolName, result)
		if err := landVerdict(ctx, deps, threadID, pending, expired); err != nil {
			return false, err
		}
	}
	// producer is whoever must now run to produce the next result. Nil means
	// the main agent, whose loop the caller re-enters itself.
	for i := start; i < len(pending.Frames); i++ {
		frame := pending.Frames[i]
		if producer == nil || subCtx == nil {
			break
		}
		var handed any
		outcome, err := RunNestedAgent(genCtx, subCtx, *producer, nil, pending.Frames[i:])
		if err == nil && outcome.Interrupted && !outcome.Aborted {
			// A child whose stream ended with no finish did not finish.
			err = fmt.Errorf("step %d ended without a finish", outcome.Steps+1)
		}
		switch {
		case err == nil && outcome.Parked:
			return false, nil // parked again one level down
		case (err == nil && outcome.Aborted) || genCtx.Err() != nil || stopped(ctx, deps, threadID):
			return false, nil // a user stop mid-unwind (§2.1)
		case err != nil:
			msg := err.Error()
			if run, gerr := deps.Admin.Runs().Get(ctx, producer.AgentID); gerr == nil && run != nil {
				failed := ports.StateFailed
				closeNested(ctx, subCtx, run, nil, ports.RunPatch{State: &failed, Error: &msg})
			}
			if _, err := Publish(ctx, deps, threadID, "SUBAGENT_FAILED", map[string]any{
				"agentId": producer.AgentID, "state": ports.StateFailed, "error": msg,
			}); err != nil {
				return false, err
			}
			handed = map[string]any{"agentId": producer.AgentID, "error": msg}
		default:
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
			handed = map[string]any{
				"agentId": producer.AgentID,
				"result":  capRunes(outcome.Text, deps.Config.SubagentResultCapChars),
			}
		}
		if _, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
			Role: ports.RoleTool, AgentID: frame.AgentID,
			Content: ToolResultContent(frame.ToolCallID, "spawnSubagent", handed),
		}); err != nil {
			return false, err
		}
		streamToolResult(ctx, deps, threadID, frame.AgentID, frame.ToolCallID, "spawnSubagent", handed)
		producer = frame.Nested
	}
	return true, nil
}

// streamToolResult puts a result the resume saved on the resume's run
// stream: a live tool's result goes out as its chunk, but a verdict's is
// only ever saved.
func streamToolResult(ctx context.Context, deps ports.RuntimePorts, threadID, agentID, toolCallID, toolName string, result any) {
	seg := ActiveSegment(deps, threadID)
	if seg == nil {
		return
	}
	chunk := MarshalPayload(map[string]any{"type": "tool-result", "toolCallId": toolCallID, "toolName": toolName, "result": result})
	if agentID != "" {
		seg.Forward(ctx, "SUBAGENT_CHUNK", MarshalPayload(map[string]any{"agentId": agentID, "chunk": chunk}), true)
		return
	}
	seg.Forward(ctx, "CHUNK", chunk, true)
}

// stopped reports a user stop on the hot cache (§2.1).
func stopped(ctx context.Context, deps ports.RuntimePorts, threadID string) bool {
	st, _, _ := deps.Kv.Get(ctx, StateKey(threadID))
	return st == string(ports.StateCancelled)
}

// closeRunRecord sums this segment onto the run's record and stamps how it
// ended (§2.9). Observability must never fail a run that otherwise
// succeeded, so errors are swallowed. Returns the end time it stamped, for
// the terminal event to carry: an accepted stop's time when there was one.
func closeRunRecord(ctx context.Context, deps ports.RuntimePorts, runID string, f FinalizeInput) time.Time {
	endedAt := time.Now()
	prior, err := deps.Admin.Runs().Get(ctx, runID)
	if err != nil || prior == nil {
		return endedAt // a run started elsewhere, or a foreign dispatch
	}
	// Teardown can add usage, but cannot undo a recorded user stop.
	if prior.State == ports.StateCancelled {
		f.State, f.StopReason = ports.StateCancelled, "cancelled"
		if prior.EndedAt != nil {
			endedAt = *prior.EndedAt
		}
	}
	patch := ports.RunPatch{
		State: &f.State, StopReason: ports.Ptr(f.StopReason), EndedAt: &endedAt,
		DurationMs: ports.Ptr(endedAt.Sub(prior.StartedAt).Milliseconds()),
	}
	if f.Error != "" {
		patch.Error = ports.Ptr(f.Error)
	}
	_ = deps.Admin.Runs().Patch(ctx, runID, patch)
	// The counters are added in the store, not read and written back here:
	// a nested run and its parent can close at the same moment.
	_ = deps.Admin.Runs().Increment(ctx, runID, deltasOf(f.Steps, f.Attribution))
	return endedAt
}

// deltasOf is a segment's steps and tokens as counters to add to its run.
func deltasOf(steps int, a TokenAttribution) ports.RunDeltas {
	return ports.RunDeltas{
		Steps: steps, InputTokens: a.InputTokens, CachedInputTokens: a.CachedInputTokens,
		OutputTokens: a.OutputTokens, TotalTokens: a.TotalTokens,
	}
}

// accrueRunRecord adds a parked segment's steps and tokens onto the run's
// record (§2.9) without closing it. Best effort, like every admin write.
func accrueRunRecord(ctx context.Context, deps ports.RuntimePorts, runID string, loop *LoopOutcome) {
	_ = deps.Admin.Runs().Increment(ctx, runID, deltasOf(loop.Steps, loop.Attribution))
}

// SettleClaimTTL is how long a settle claim holds (§5.6). A claim older
// than this belongs to a settler that died, and the next settle takes the
// run over.
const SettleClaimTTL = 10 * time.Minute

// settleRun runs the spec's OnSettle for a run, once (§5.6). Every path that
// ends a run goes through here: the worker that finished or aborted it, the
// worker that failed it, a stop that ended it while no worker held it, and
// the late-settle sweep.
//
// Once is kept by a claim on the run record, made in one conditional write
// before the hook runs: only the settler that wins it calls the hook, so two
// that arrive together cannot both bill. The hook's success marks the run
// settled; its failure drops the claim, so a later stop, delivery or sweep
// runs it again. A claim that is never ended (the settler died mid-hook)
// is taken over after SettleClaimTTL. That is also why the hook must be
// idempotent by RunID: a hook slower than the claim, or a mark that could
// not be written, can see the same run twice.
//
// Returns the hook's error, and whether this call ran the settle at all. A
// run with no record (a foreign dispatch) settles unclaimed, as it always
// did. A store that cannot take the claim leaves the run unsettled for the
// sweep rather than risk a second bill.
//
// Everything here runs on a context no stop or shutdown can cancel. A
// stopped run reaches this with its generation context already cancelled,
// and the hook's own writes must still land. The hook learns about a stop
// from info.Cancelled.
func settleRun(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, info ports.RunFinishInfo) (bool, error) {
	ctx = context.WithoutCancel(ctx)
	log := Logger(deps).With("run", info.RunID)
	hook := func() error {
		if agent == nil || agent.Args.OnSettle == nil {
			return nil
		}
		return CallSafely(func() error { return agent.Args.OnSettle(ctx, info) })
	}
	if info.RunID == "" {
		return true, hook()
	}
	token := NewID()
	var claimed bool
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
		if claimed, err = deps.Admin.Runs().ClaimSettle(ctx, info.RunID, token, time.Now().Add(-SettleClaimTTL)); err == nil {
			break
		}
	}
	if err != nil {
		log.Error("settle not claimed; the run stays unsettled for the late-settle sweep", "err", err)
		return false, nil
	}
	if !claimed {
		rec, err := deps.Admin.Runs().Get(ctx, info.RunID)
		if err == nil && rec == nil {
			return true, hook() // no record to claim: settled unclaimed
		}
		return false, nil // settled already, or being settled by someone else
	}
	hookErr := hook()
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
		if err = deps.Admin.Runs().EndSettle(ctx, info.RunID, token, hookErr == nil); err == nil {
			break
		}
	}
	switch {
	case hookErr != nil:
		// The claim is dropped, so a later stop, delivery or sweep can run
		// the hook again.
		log.Error("settle hook failed; the run stays unsettled for a retry", "err", hookErr)
	case err != nil:
		log.Error("settle mark not written; the claim lapses and the late-settle sweep may settle this run again", "err", err)
	}
	return true, hookErr
}

// settleEndedRun settles a run whose record has ended but whose settle
// never ran (§2.1, §5.6): a stop that ended it while it was queued or
// parked, a worker that died between finishing and settling, or a settle
// hook that failed and left the mark unset. The steps it did make were
// priced as they happened, so its bill is real and has to reach the hook.
// Nothing happens when the run already settled, or when its record is
// still open.
//
// The caller holds the run lock. Stop takes it for exactly this; a worker
// that finds the thread already ended at segment start holds it too, and so
// does the stuck-run sweep.
func settleEndedRun(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, threadID, runID string) bool {
	if agent == nil || runID == "" {
		return false
	}
	rec, err := deps.Admin.Runs().Get(ctx, runID)
	if err != nil || rec == nil || rec.SettledAt != nil || !isTerminal(rec.State) {
		return false
	}
	bill, billErr := runBill(ctx, deps, threadID, runID)
	info := ports.RunFinishInfo{
		ThreadID: threadID, RunID: runID, State: rec.State, StopReason: rec.StopReason, Error: rec.Error,
		TokensUsed: rec.TotalTokens, Steps: rec.Steps, Cancelled: rec.State == ports.StateCancelled,
		Usage: bill, UsageErr: billErr,
	}
	if info.StopReason == "" {
		info.StopReason = "cancelled"
	}
	// The settle error is ignored, as it is for a stopped worker: the run
	// has already ended either way, and the mark stays unset for a retry.
	settled, _ := settleRun(ctx, deps, agent, info)
	if settled && agent.Args.OnFinish != nil {
		agent.Args.OnFinish(info)
	}
	return settled
}

// finishedEarlier reports whether this run already made its last step in an
// earlier delivery (§2.8): a worker saved the step that answered, then died
// before the run ended. Every run appends its own user turn when it is
// dispatched, so a main-agent history that ends in an assistant message with
// no tool calls can only be this run's answer. A run already settled is
// finished too. The text is that answer, for a generate-text run's result.
func finishedEarlier(ctx context.Context, deps ports.RuntimePorts, threadID, runID string) (string, bool) {
	if runID == "" {
		return "", false
	}
	settled := false
	if rec, err := deps.Admin.Runs().Get(ctx, runID); err == nil && rec != nil {
		settled = rec.SettledAt != nil
	}
	msgs, err := deps.Storage.Messages.List(ctx, threadID, ports.MainAgent)
	if err != nil || len(msgs) == 0 {
		return "", settled
	}
	last := msgs[len(msgs)-1]
	if last.Role != ports.RoleAssistant {
		return "", settled
	}
	text := ""
	for _, p := range ParseContent(last.Content) {
		switch p.Type {
		case "tool-call":
			return "", settled // the loop was not done
		case "text":
			text += p.Text
		}
	}
	return text, true
}

// isTerminal reports whether a state is one a run cannot leave.
func isTerminal(state ports.ExecutionState) bool {
	return state == ports.StateCancelled || state == ports.StateCompleted || state == ports.StateFailed
}

// closeIfOpen closes a run record that can never be worked on again (§2.9):
// the thread is gone, or a newer run replaced this one. Without this the
// record stays RUNNING for ever and every "in flight" count carries it.
func closeIfOpen(ctx context.Context, deps ports.RuntimePorts, runID, stopReason string) {
	if runID == "" {
		return
	}
	rec, err := deps.Admin.Runs().Get(ctx, runID)
	if err != nil || rec == nil || rec.EndedAt != nil {
		return
	}
	closeRunRecord(ctx, deps, runID, FinalizeInput{State: ports.StateCancelled, StopReason: stopReason, RunID: runID})
}

// failRun finalises a run as FAILED on both homes AND keeps why (§2.9).
//
// The spec's OnSettle still runs (§5.6): a caller that opened records for
// this run must get to close them as failed. Its own error cannot change
// the outcome, which is already a failure.
func failRun(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, threadID, runID, reason string) error {
	// A failed run still spent money on the steps it did make (§4), so it
	// settles like any other end. Settled even with no hook, so the late
	// sweep does not keep coming back to it.
	bill, billErr := runBill(ctx, deps, threadID, runID)
	info := ports.RunFinishInfo{
		ThreadID: threadID, RunID: runID, State: ports.StateFailed, StopReason: "failed", Error: reason,
		TokensUsed: bill.TotalTokens, Usage: bill, UsageErr: billErr,
	}
	settled, _ := settleRun(ctx, deps, agent, info)
	// Only while the thread is still this run's and still going: a stop or a
	// newer run that got there first keeps its own ending (§3.4).
	won, err := Transition(ctx, deps, threadID, StateChange{From: ActiveStates, To: ports.StateFailed, RunID: runID})
	if err != nil {
		return err
	}
	if !won {
		Logger(deps).Info("run not failed: the thread has moved on", "thread", threadID, "run", runID, "reason", reason)
		return nil
	}
	endedAt := time.Now()
	if runID != "" {
		endedAt = closeRunRecord(ctx, deps, runID, FinalizeInput{
			State: ports.StateFailed, StopReason: "failed", Error: reason, RunID: runID,
		})
	}
	terminal := map[string]any{"state": ports.StateFailed, "stopReason": "failed", "error": reason, "endedAt": endedAt}
	if runID != "" {
		terminal["runId"] = runID
	}
	if _, err = Publish(ctx, deps, threadID, "STATE_CHANGE", terminal); err != nil {
		return err
	}
	// OnFinish fires on every end, a failure included, once.
	if settled && agent != nil && agent.Args.OnFinish != nil {
		agent.Args.OnFinish(info)
	}
	return nil
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
func resumePendingHitl(ctx, genCtx context.Context, deps ports.RuntimePorts, threadID string, open []PendingHitl, rawTools []ports.Tool, subCtx *SubagentCtx, state ports.AgentRunState, toolRun ToolRun) (bool, error) {
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
		var result json.RawMessage
		expired := false
		if !pending.Landed {
			var err error
			run := toolRun
			run.AgentID = pending.AgentID
			if pending.AgentID != "" && pending.Nested != nil {
				run.AgentName = pending.Nested.Name
			}
			if result, expired, err = settleVerdict(ctx, genCtx, deps, threadID, pending, target, state, run); err != nil {
				return false, err
			}
		}
		ok, err := unwindVerdict(ctx, genCtx, deps, threadID, pending, result, expired, subCtx)
		if err != nil || !ok {
			return false, err
		}
	}
	// A stop that landed while the verdicts were applied wins: the thread
	// stays CANCELLED and this segment goes no further (§3.4).
	runID := RunIDFromContext(ctx)
	if won, err := Transition(ctx, deps, threadID, StateChange{
		From: []ports.ExecutionState{ports.StateWaitingForInput}, To: ports.StateRunning, RunID: runID,
	}); err != nil || !won {
		return false, err
	}
	if _, err := Publish(ctx, deps, threadID, "STATE_CHANGE", runStatePayload(ctx, deps, ports.StateRunning, runID)); err != nil {
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
	// DispatchID is the job's delivery id (see ports.RunJob). Empty means a
	// call that did not come off the queue; the lock then makes one up.
	DispatchID string
	// EnqueuedAt is epoch ms at enqueue, for the queue-wait measurement (§2.9).
	EnqueuedAt int64
	// DispatchedAt is epoch ms of the run's first dispatch (§2.8), carried
	// onto every retry so the run keeps its place in line.
	DispatchedAt int64
	// Kind says why this job exists. See ports.JobKind.
	Kind ports.JobKind
	// PartitionKey is the caller's tenant on the ticket, carried onto retries.
	PartitionKey string
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

// EnqueueJob is the one way the platform puts a job on the queue: it stamps
// the job with a fresh DispatchID, so every enqueue is a delivery of its own
// and only the queue's own redelivery repeats one (§3.4).
func EnqueueJob(ctx context.Context, deps ports.RuntimePorts, job ports.RunJob, opts *ports.EnqueueOptions) error {
	if job.DispatchID == "" {
		job.DispatchID = NewID()
	}
	return deps.Queue.Enqueue(ctx, job, opts)
}

// job rebuilds the dispatch ticket for a job that goes back on the queue: a
// retry, a redrive. Same run, same caps, same place in line; a new
// delivery, so no DispatchID (EnqueueJob stamps a fresh one).
func (input ExecuteInput) job(agent string, kind ports.JobKind) ports.RunJob {
	return ports.RunJob{
		ThreadID: input.ThreadID, RunID: input.RunID, Model: input.Model, Agent: agent,
		Kind: kind, PartitionKey: input.PartitionKey,
		EnqueuedAt: time.Now().UnixMilli(), DispatchedAt: input.DispatchedAt,
		State: input.State, TokenBudget: input.TokenBudget,
		// A redrive is the SAME run trying again, so it keeps the caps it
		// was dispatched with: a retry that lost its money cap would be
		// unbounded (§4).
		CostBudgetMicros: input.CostBudgetMicros,
		ProviderOptions:  input.ProviderOptions, MaxSteps: input.MaxSteps,
	}
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
	// OutcomeLockLost: this worker held the lock and could not keep it
	// renewed; the segment ended early and the job should come back once
	// the lock is free. Every step it finished is already persisted.
	OutcomeLockLost ExecuteOutcome = "lock-lost"
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
// work and renews it while the segment runs, so a held lock always means a
// live worker. Two workers can never run one thread, and a crashed worker's
// lock expires within a lease instead of blocking forever (§3.4).
func Execute(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, input ExecuteInput) (outcome ExecuteOutcome, retErr error) {
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
	log := Logger(deps).With("thread", threadID, "run", runID)

	// True once the thread has started a NEWER run than this one (§2.1).
	stale := func() (bool, error) {
		if runID == "" {
			return false, nil
		}
		current, _, err := deps.Kv.Get(ctx, RunIDKey(threadID))
		return current != runID, err
	}

	// The lock names this run and this delivery of it (§3.4), so a later
	// conflict can tell a duplicate of THIS job apart from another delivery
	// of the same run and from an older run that is still finishing.
	lease, err := AcquireRunLock(ctx, deps, threadID, runID, input.DispatchID)
	if err != nil {
		return "", err
	}
	if lease == nil {
		return OutcomeLockConflict, nil // another worker owns this thread (§2.8)
	}
	// Release on success, failure, or stop: only while the lock is still
	// this worker's. A lock another worker took after this one lapsed is
	// theirs to free (§3.4). Registered first, so it runs last: the lease
	// is held through the settle and the finalize below.
	defer lease.Release()

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

	// Three ways a segment ends early, one behavior: everything tears down at once.
	//   1. the state key reads CANCELLED: the user pressed stop (§2.1);
	//   2. the run id has moved on: the user pressed stop and then sent another
	//      message, which put RUNNING back over CANCELLED before this poll
	//      could read it. The state key lies in that window; the run id never does;
	//   3. the run lock could not be kept: by then another worker may own
	//      the thread, so this one must stop touching it (§3.4).
	// A SegmentTimeout, when set, is a fourth: the segment is bounded in
	// wall time and settles FAILED past it rather than holding the worker.
	genBase := ctx
	var segmentDeadline time.Time
	if deps.Config.SegmentTimeout > 0 {
		segmentDeadline = time.Now().Add(deps.Config.SegmentTimeout)
		var cancelSegment context.CancelFunc
		genBase, cancelSegment = context.WithDeadline(ctx, segmentDeadline)
		defer cancelSegment()
	}
	genCtx, cancel := context.WithCancel(genBase)
	var abortedFlag, lockLost atomic.Bool
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
	// The lease is renewed until it is released, not only while the model
	// runs: the settle and the finalize after the loop are writes too. A
	// lease that cannot be kept ends the segment (§3.4).
	lease.Keep(func() {
		lockLost.Store(true)
		cancel()
	})
	defer func() {
		cancel()
		<-pollDone
	}()

	// The segment's run stream, open from pickup (see SegmentStream), and how
	// it ends: set where the segment's outcome is known, or worked out here
	// from how it stopped. Closed before the lock goes, so the next
	// segment's stream never starts while this one is still open.
	var seg *SegmentStream
	var segEnd ports.StreamEnd
	defer func() {
		if seg == nil || seg.Closed() {
			return
		}
		end := segEnd
		if end == nil {
			switch {
			case lockLost.Load() || outcome == OutcomeLockLost:
				end = &ports.RunErrorEvent{Status: "lost", Error: "the worker lost the run lock"}
			case ctx.Err() != nil:
				end = &ports.RunErrorEvent{Status: "lost", Error: "the worker shut down mid-run"}
			case retErr != nil:
				end = &ports.RunErrorEvent{Status: "error", Error: retErr.Error()}
			default:
				end = &ports.RunErrorEvent{Status: "error", Error: "the segment ended"}
			}
		}
		seg.Close(ctx, end)
	}()

	// A newer run already owns this thread: this job has nothing to do, and
	// must not touch state on the live run's behalf (§2.1). Its own record
	// is closed, so it does not count as in flight for ever.
	if replaced, err := stale(); err != nil {
		return "", err
	} else if replaced {
		closeIfOpen(ctx, deps, runID, "replaced")
		return OutcomeStale, nil
	}
	// At-least-once idempotency (§2.8): a job whose run already ended, or was
	// stopped, must be a no-op on redelivery. A MISSING thread is the same
	// no-op: it was deleted (§3.2) and must never be resurrected.
	durable, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil {
		return "", err
	}
	if durable == nil {
		closeIfOpen(ctx, deps, runID, "orphaned")
		return OutcomeExecuted, nil
	}
	if isTerminal(durable.State) {
		// A stop ended this run before any worker got to it, or while a
		// worker was between segments; or the run ended and its settle
		// never landed. Whoever holds the lock settles it, once.
		settleEndedRun(ctx, deps, agent, threadID, runID)
		return OutcomeExecuted, nil
	}

	// A wait has a ceiling (§2.8): a job picked up long past it fails with
	// the reason, instead of doing work nobody is waiting for any more.
	if deps.Config.MaxQueueWait > 0 && input.EnqueuedAt > 0 {
		if waited := time.Since(time.UnixMilli(input.EnqueuedAt)); waited > deps.Config.MaxQueueWait {
			reason := fmt.Sprintf("the run waited %s in the queue, past the %s limit", waited.Round(time.Second), deps.Config.MaxQueueWait)
			log.Warn("run queued too long; failing it", "waited", waited)
			if durable.State == ports.StateWaitingForInput {
				_ = closeOpenParks(ctx, deps, threadID)
			}
			return OutcomeExecuted, failRun(ctx, deps, agent, threadID, runID, reason)
		}
	}

	// Billing at pickup (§4): the dispatch check ran before the wait; the
	// balance may have moved since. A refusal here fails the run with the
	// reason; a lowered cap applies to this segment.
	if deps.Config.BillingPreCheck != nil && runID != "" {
		budget := &ports.RunBudget{CostBudgetMicros: costBudget, MaxSteps: input.MaxSteps}
		check := ports.BillingCheck{
			ThreadID: threadID, RunID: runID, State: input.State, Stage: ports.BillingAtPickup, Budget: budget,
			PublishEvent: func(ctx context.Context, typ string, payload any, durable bool) (ports.AgentEvent, error) {
				return PublishEvent(ctx, deps, threadID, typ, payload, PublishOptions{Durable: durable})
			},
		}
		if err := deps.Config.BillingPreCheck(ctx, check); err != nil {
			log.Warn("run refused at pickup", "err", err)
			_, _ = Publish(ctx, deps, threadID, "RUN_REFUSED", map[string]any{"reason": ports.RefusedBilling, "error": err.Error(), "runId": runID})
			if durable.State == ports.StateWaitingForInput {
				_ = closeOpenParks(ctx, deps, threadID)
			}
			return OutcomeExecuted, failRun(ctx, deps, agent, threadID, runID, err.Error())
		}
		if budget.CostBudgetMicros > 0 && (costBudget == 0 || budget.CostBudgetMicros < costBudget) {
			costBudget = budget.CostBudgetMicros
		}
		if budget.MaxSteps > 0 && (input.MaxSteps == 0 || budget.MaxSteps < input.MaxSteps) {
			input.MaxSteps = budget.MaxSteps
		}
	}

	// Step ceiling (§2.1): the run's own cap when it set one, the config's
	// otherwise, and never above the config's.
	maxSteps := deps.Config.MaxSteps
	if input.MaxSteps > 0 && input.MaxSteps < maxSteps {
		maxSteps = input.MaxSteps
	}
	resume := ports.ResumeInfo{
		Agent: agent.Name, Model: input.Model, RunID: runID, DispatchedAt: input.DispatchedAt,
		TokenBudget: input.TokenBudget, CostBudgetMicros: input.CostBudgetMicros,
		ProviderOptions: providerOptions,
		// Carried so the resumed segment scopes its storage the same way (§2.10).
		State: input.State, MaxSteps: input.MaxSteps,
	}
	// One ledger for the whole run: a nested run's spend counts against the
	// same caps the main agent is checked against (§2.7), and it starts from
	// what the run spent before a park or a retry, so no segment gets a
	// fresh budget.
	ledger := SeedRunLedger(ctx, deps, threadID, runID)

	// Pickup (§2.8): the run has a worker now. A QUEUED thread becomes
	// RUNNING on every home, its record takes the moment work started, and
	// the wire says so, so a client's clock measures work rather than
	// waiting. The wait itself is kept on the record: the latest dispatch's,
	// so a resume's or a retry's wait shows as its own.
	pickedUp := time.Now()
	if runID != "" {
		patch := ports.RunPatch{}
		if input.EnqueuedAt > 0 {
			patch.QueuedMs = ports.Ptr(pickedUp.UnixMilli() - input.EnqueuedAt)
		}
		if durable.State == ports.StateQueued {
			// A stop (or a newer run) that landed since the read above wins:
			// this job has nothing to pick up (§3.4).
			won, err := Transition(ctx, deps, threadID, StateChange{
				From: []ports.ExecutionState{ports.StateQueued}, To: ports.StateRunning, RunID: runID, Model: input.Model,
			})
			if err != nil {
				return "", err
			}
			if !won {
				log.Info("run not picked up: the thread moved on before this worker got to it")
				return OutcomeExecuted, nil
			}
			startedAt := pickedUp
			if rec, err := deps.Admin.Runs().Get(ctx, runID); err == nil && rec != nil && rec.QueuedMs != nil {
				startedAt = rec.StartedAt // a retry: the run started when it first ran
			} else {
				patch.StartedAt = &pickedUp
			}
			patch.State = ports.Ptr(ports.StateRunning)
			_ = deps.Admin.Runs().Patch(ctx, runID, patch)
			if _, err := Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{
				"state": ports.StateRunning, "runId": runID, "startedAt": startedAt,
			}); err != nil {
				return "", err
			}
			durable.State = ports.StateRunning
		} else if patch.QueuedMs != nil {
			_ = deps.Admin.Runs().Patch(ctx, runID, patch)
		}
	}

	// From here on the segment does work, and says so on its own stream.
	seg = OpenSegment(ctx, deps, threadID, runID)

	// Platform-owned toolset: HITL (§2.5) over the user's set; spawnSubagent
	// added ONLY when the spec opts in (§2.7). rawTools keeps the real
	// implementations: the resolved park executes the approved tool.
	// Parks raised during this segment wait here until the step that raised
	// them is saved (see ParkBox).
	parks := &ParkBox{}
	var subCtx *SubagentCtx
	if agent.Spec.Subagents != nil {
		subCtx = &SubagentCtx{
			IOCtx: ctx, ThreadID: threadID, Depth: 0, Ports: deps,
			// Made per run: the cap is this run's, never shared with others.
			Slots: NewRunSlots(deps.Config.SubagentMaxConcurrent),
			Sub:   *agent.Spec.Subagents, Agent: agent, Ledger: ledger, Resume: resume,
			TokenBudget: tokenBudget, CostBudgetMicros: costBudget, BillingRunID: runID,
			ProviderOptions: providerOptions, Aborted: aborted, Fenced: lease.Lost, Parks: parks, State: input.State,
		}
	}
	rawTools := slices.Clone(agent.Args.Tools)
	if subCtx != nil {
		rawTools = append(rawTools, SpawnSubagentTool(subCtx))
	}
	// The main agent's own toolset: nothing is waiting on its parks (§2.7).
	// Every tool also sees the run's state (§2.10) and can publish its own
	// events on the thread.
	// ...and the run it is part of: its ports, its ledger, its ids.
	toolRun := ToolRun{Deps: deps, ThreadID: threadID, RunID: runID, AgentName: agent.Name, Ledger: ledger}
	tools := WithToolRun(WithRunState(WithPublishEvent(deps, threadID, WithHitl(deps, threadID, rawTools, HitlCtx{Resume: resume, Parks: parks})), input.State), toolRun)

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
		resumed, err := resumePendingHitl(ctx, genCtx, deps, threadID, open, rawTools, subCtx, input.State, toolRun)
		if err != nil {
			return "", err
		}
		if !resumed {
			// Still parked, or parked again one level down while unwinding:
			// the new park's step is saved by now, so it is written here.
			if err := CommitParks(ctx, deps, parks); err != nil {
				return "", err
			}
			segEnd = &ports.RunFinishedEvent{Status: "parked"}
			return OutcomeExecuted, nil
		}
	}

	// A retry after the run's last step was already saved (§2.8): the worker
	// died between that step and the end of the run. The answer is in the
	// history, so the run is finalized from it rather than asking the model
	// again, which would answer twice. The same when the run already settled.
	var loop *LoopOutcome
	timedOut := false
	if text, done := finishedEarlier(ctx, deps, threadID, runID); done {
		log.Info("run already made its last step; finalized without calling the model again")
		loop = &LoopOutcome{Text: text, FinishReason: provider.FinishStop}
	} else {
		// Durable compaction pass: history always fits the model budget (§2.6)
		var history []ports.MessageDTO
		history, err = CompactContext(ctx, deps, threadID, input.Model, CompactOptions{RunID: runID, GenCtx: genCtx, Ledger: ledger})
		if err != nil {
			return "", err
		}
		var model ports.ResolvedModel
		model, err = deps.ResolveModel(input.Model)
		if err != nil {
			return "", err
		}
		// Prompt caching (§2.6): stamp the stable prefix once; appended step
		// messages extend the prompt without invalidating the breakpoints.
		messages := RepairDanglingToolCalls(MessagesFromDTOs(history))
		if deps.Config.PromptCaching {
			messages = MarkPromptCaching(messages)
		}

		loop, err = RunLoop(ctx, deps, agent, threadID, LoopInput{
			AgentID: "", RunID: runID, Kind: agent.Kind, Model: model.Instance(),
			Messages: messages, Tools: tools, MaxSteps: maxSteps,
			GenCtx: genCtx, Aborted: aborted, Fenced: lease.Lost,
			CommitParks:     func(c context.Context) error { return CommitParks(c, deps, parks) },
			ProviderOptions: providerOptions, TokenBudget: tokenBudget,
			SystemFn: agent.Args.SystemFn, PrepareStep: agent.Args.PrepareStep, State: input.State,
			CostBudgetMicros: costBudget, BillingRunID: runID,
			ModelKey: input.Model, ModelID: model.WireID(input.Model), AgentName: agent.Name,
			CacheSystemPrompt: deps.Config.PromptCaching,
			// One canonical path for every client: durable log + live bus (§2.1,
			// §2.2), with token deltas merged (see chunkBatcher).
			PublishChunk: func(p map[string]any) {
				_, _ = Publish(ctx, deps, threadID, "CHUNK", p)
			},
			OnChunk: agent.Args.OnChunk, // the user callback sees every raw chunk
		}, ledger)
		// A lost lock ends the segment whatever the loop returned: another
		// worker may own the thread now, so nothing below may write to it. A
		// loop cut short by the lock loss can even come back without an error.
		if lockLost.Load() {
			// Every finished step is persisted; the job comes back once the
			// lock is free and resumes from the last one.
			log.Warn("segment ended early: run lock lost", "steps", loop.Steps)
			return OutcomeLockLost, nil
		}
		if err != nil || loop.Interrupted {
			switch {
			case errors.Is(genBase.Err(), context.DeadlineExceeded):
				// The segment's own deadline ended it, whatever the provider
				// turned that into.
				timedOut = true
				log.Warn("segment timed out", "after", deps.Config.SegmentTimeout, "steps", loop.Steps)
			case err != nil:
				return "", err
			default:
				// The stream ended with no finish and no error. The step was
				// not completed, so it goes to the retry policy rather than
				// finalizing as COMPLETED.
				return "", fmt.Errorf("step %d ended without a finish", loop.Steps+1)
			}
		}
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
		segEnd = &ports.RunFinishedEvent{Status: "parked"}
		return OutcomeExecuted, nil
	}

	stopReason := "completed"
	state := ports.StateCompleted
	switch {
	case timedOut:
		stopReason, state = "timeout", ports.StateFailed
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
	if timedOut {
		f.Error = fmt.Sprintf("the run ran longer than %s and was stopped", deps.Config.SegmentTimeout)
	}
	if agent.Kind == ports.KindGenerateText && !timedOut {
		text := loop.Text
		f.OneShotText = &text
	}
	// The caller settles BEFORE the terminal state lands (§5.6): what the run
	// produced is committed by the time any client sees it end. A settle
	// failure is a run failure. A stop reaches the hook with Cancelled set,
	// on a context that is not cancelled, so the hook's own writes land.
	// The whole run's bill, read back from the rows the loop wrote (§4):
	// every segment and every nested run, priced and grouped into lines, so a
	// settle hook charges in one pass without keeping its own tally. Read
	// once, handed to both hooks; a failed read is reported, not hidden.
	bill, billErr := runBill(ctx, deps, threadID, runID)
	if _, err := settleRun(ctx, deps, agent, ports.RunFinishInfo{
		ThreadID: threadID, RunID: runID, State: state, StopReason: stopReason, Error: f.Error,
		TokensUsed: f.TokensUsed, Attribution: f.Attribution, Steps: f.Steps,
		Cancelled: state == ports.StateCancelled,
		Usage:     bill, UsageErr: billErr,
	}); err != nil && state != ports.StateCancelled {
		state = ports.StateFailed
		f.State = state
		f.Error = err.Error()
	}
	if err := Finalize(ctx, deps, agent, threadID, f); err != nil {
		return "", err
	}
	segEnd = segmentEnd(state, f.Error, bill, string(loop.FinishReason), seg)
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
	// StopReason is 'completed' | 'token_budget' | 'max_steps' | 'cancelled' | 'timeout'.
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

// segmentEnd is how a finalized segment's stream ends.
func segmentEnd(state ports.ExecutionState, failure string, bill ports.UsageTotals, finishReason string, seg *SegmentStream) ports.StreamEnd {
	if state == ports.StateFailed {
		if failure == "" {
			failure = "the run failed"
		}
		return &ports.RunErrorEvent{Status: "error", Error: failure}
	}
	end := &ports.RunFinishedEvent{
		Status: "finished",
		Usage: &ports.StreamUsage{
			InputTokens: int64(bill.InputTokens), CachedInputTokens: int64(bill.CachedInputTokens),
			OutputTokens: int64(bill.OutputTokens), TotalTokens: int64(bill.TotalTokens),
		},
		FinishReason: finishReason,
	}
	if state == ports.StateCancelled {
		end.Status = "stopped"
	}
	for _, c := range bill.Costs {
		end.Costs = append(end.Costs, ports.StreamCost{Currency: c.Currency, Micros: c.CostMicros})
	}
	if seg != nil {
		if text, ok := seg.OneShotText(); ok {
			end.Text = text
		}
	}
	return end
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
	// The thread moves only while it is still this run's and still RUNNING
	// (§3.4). A stop that got there first has already written CANCELLED and
	// said so; a newer run owns the thread. Either way this finish loses and
	// stays silent: publishing it would land on top of the stop, or wedge the
	// newer run (§2.1).
	won, err := Transition(ctx, deps, threadID, StateChange{
		From: []ports.ExecutionState{ports.StateRunning}, To: f.State, RunID: f.RunID,
	})
	if err != nil {
		return err
	}
	// Close the run's durable record (§2.9) either way: the segment's steps
	// and tokens are real. Additive: a run that parked and resumed finalises
	// once, but its steps and tokens accrued over several segments. A stop
	// already recorded on it is kept (closeRunRecord never undoes one).
	endedAt := time.Now()
	if f.RunID != "" {
		endedAt = closeRunRecord(ctx, deps, f.RunID, f)
	}
	if !won {
		return nil
	}
	if f.OneShotText != nil {
		// One-shot flavor: no CHUNK stream; publish the final text as one event
		if _, err := Publish(ctx, deps, threadID, "TEXT_RESULT", map[string]any{"text": *f.OneShotText}); err != nil {
			return err
		}
	}
	// The run's identity and end time ride the terminal event, so a client
	// closes the right timer without reading the run record back.
	terminal := map[string]any{
		"state": f.State, "stopReason": f.StopReason, "tokensUsed": f.TokensUsed, "usage": f.Attribution,
		"endedAt": endedAt,
	}
	if f.RunID != "" {
		terminal["runId"] = f.RunID
	}
	if f.Error != "" {
		terminal["error"] = f.Error
	}
	_, err = Publish(ctx, deps, threadID, "STATE_CHANGE", terminal)
	return err
}

// redriveOnLockConflict: the lock names the run and the delivery that hold
// it (§3.4), so a conflict can say which of these it is:
//
//   - the same job, delivered twice by the queue (same run, same dispatch):
//     a duplicate of work already running. Drop it, and say so.
//   - another delivery of the same run: a retry, an approval's answer or its
//     expiry (§2.5), arriving while the current holder winds down. It has
//     work to do once the lock clears, so it comes back.
//   - an OLDER run that has not finished tearing down: this job never ran.
//     Dropping it strands the message the user just sent, so it comes back.
//
// Because the lock is renewed while its holder runs, a held lock means a
// live worker, and waiting for it is always right. The job comes back with
// a growing delay until it has waited at least one lease and used its
// attempts; only then is the lock taken to be wedged and the run FAILED.
//
// One case is none of these: the thread already ended under the held lock
// and its settle never ran. That job is the last chance to settle, so it
// comes back once the lock has surely cleared.
func redriveOnLockConflict(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, input ExecuteInput, maxAttempts int) error {
	if input.RunID == "" {
		return nil // legacy dispatch, no identity: old drop behavior
	}
	log := Logger(deps).With("thread", input.ThreadID, "run", input.RunID, "kind", string(input.Kind))
	scope := CounterScope(input.ThreadID, input.RunID)
	holder, held, err := deps.Kv.Get(ctx, RunLockKey(input.ThreadID))
	if err != nil {
		return err
	}
	durable, err := deps.Storage.Threads.Get(ctx, input.ThreadID)
	if err != nil {
		return err
	}
	holderRun, holderDispatch := ParseLockValue(holder)
	if held && holderRun != input.RunID && durable != nil && isTerminal(durable.State) {
		// The thread has ended: this job has nothing left to do, whoever
		// holds the lock. Redriving it would only fail the ended run again.
		log.Info("delivery dropped: the thread has already ended")
		return nil
	}
	if held && holderRun == input.RunID {
		if holderDispatch != "" && holderDispatch == input.DispatchID {
			log.Info("duplicate delivery dropped: this job is already running")
			return nil
		}
		switch {
		case durable == nil || durable.State == ports.StateWaitingForInput:
			// fall through to the redrive below
		case isTerminal(durable.State):
			rec, err := deps.Admin.Runs().Get(ctx, input.RunID)
			if err == nil && rec != nil && rec.SettledAt == nil {
				log.Info("run ended under a held lock and is not settled; settle retried once the lock clears")
				err := EnqueueJob(ctx, deps, input.job(agent.Name, ports.JobRedrive),
					&ports.EnqueueOptions{Delay: deps.Config.RunLockLease, Key: "settle:" + input.RunID, Priority: ports.PriorityLow})
				if errors.Is(err, ports.ErrDuplicateJob) {
					return nil
				}
				return err
			}
			return nil
		case holderDispatch == "" || input.DispatchID == "":
			// A lock or a job from before dispatch ids: the two deliveries
			// cannot be told apart, so the old rule stands.
			log.Info("duplicate delivery dropped: this run already holds the lock")
			return nil
		default:
			// Another delivery of this run, while it runs: a retry its own
			// holder queued before letting go. Come back once it has.
		}
	}
	if current, _, err := deps.Kv.Get(ctx, RunIDKey(input.ThreadID)); err != nil {
		return err
	} else if current != input.RunID {
		log.Info("delivery dropped: a newer run owns the thread")
		return nil // already replaced
	}
	tries, err := deps.Kv.IncrWithExpiry(ctx, RedriveKey(scope), counterTTL)
	if err != nil {
		return err
	}
	delay, waited := redriveDelay(deps.Config, tries)
	if tries <= int64(maxAttempts) || waited < deps.Config.RunLockLease {
		log.Info("run lock held; redriven", "try", tries, "in", delay)
		return EnqueueJob(ctx, deps, input.job(agent.Name, ports.JobRedrive), &ports.EnqueueOptions{Delay: delay})
	}
	if err := deps.Kv.Del(ctx, RedriveKey(scope)); err != nil {
		return err
	}
	return failRun(ctx, deps, agent, input.ThreadID, input.RunID, "the run lock never cleared")
}

// redriveDelay is how long the given try waits (RunRedriveDelay, doubled on
// each try, capped at the lease), and how long the tries before it waited in
// all.
func redriveDelay(cfg ports.AgentConfig, tries int64) (delay, waited time.Duration) {
	delay = cfg.RunRedriveDelay
	if delay <= 0 {
		delay = time.Second // a zero base would never grow, and never give up
	}
	for i := int64(1); i < tries; i++ {
		waited += delay
		delay = min(delay*2, cfg.RunLockLease)
	}
	return delay, waited
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
// through the queue, each retry waiting longer than the last; exhausted
// attempts finalize FAILED (hot cache + durable). A user stop is never
// retried, a shutdown never costs an attempt, and a successful run resets
// the attempt counter.
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
	scope := CounterScope(input.ThreadID, input.RunID)
	outcome, err := exec(ctx, deps, agent, input)
	if err == nil {
		switch outcome {
		case OutcomeExecuted:
			// Only a run THIS worker executed may reset the retry budget (§2.8)
			if err := deps.Kv.Del(ctx, AttemptsKey(scope)); err != nil {
				return err
			}
			return deps.Kv.Del(ctx, RedriveKey(scope))
		case OutcomeStale:
			return nil // a newer run owns the thread: this job is a genuine no-op
		default:
			return redriveOnLockConflict(ctx, deps, agent, input, maxAttempts)
		}
	}

	// From here the policy works on a context a shutdown cannot cancel: a
	// worker told to stop must still hand its job back or fail it, or the
	// thread reads RUNNING for ever.
	bg := context.WithoutCancel(ctx)
	log := Logger(deps).With("thread", input.ThreadID, "run", input.RunID)
	if ctx.Err() != nil {
		// The worker was cut off: a shutdown, or the queue's own cap. That
		// is not the run's fault, so it goes back at once and the attempt
		// is not counted.
		log.Info("run interrupted; requeued", "err", err)
		return requeue(bg, deps, agent, input, 0)
	}

	// A user stop already finalized the thread: never retry a stop
	if state, _, kvErr := deps.Kv.Get(bg, StateKey(input.ThreadID)); kvErr != nil {
		return errors.Join(err, kvErr)
	} else if state == string(ports.StateCancelled) {
		return nil
	}
	// A run that a newer one replaced failed after it stopped mattering: its
	// error is not the thread's, so it spends no attempt and fails nothing.
	if input.RunID != "" {
		if current, curErr := CurrentRunID(bg, deps, input.ThreadID); curErr != nil {
			return errors.Join(err, curErr)
		} else if current != input.RunID {
			log.Info("replaced run failed; nothing to retry", "err", err)
			return nil
		}
	}
	attempts, kvErr := deps.Kv.IncrWithExpiry(bg, AttemptsKey(scope), counterTTL)
	if kvErr != nil {
		return errors.Join(err, kvErr)
	}
	if attempts < int64(maxAttempts) {
		// A retry is the SAME run trying again (§2.1): it keeps the id, so it
		// can notice it was replaced and redrive if it finds the lock held.
		delay := retryBackoff(deps.Config, int(attempts))
		log.Warn("run failed; retry scheduled", "err", err, "attempt", attempts, "maxAttempts", maxAttempts, "in", delay.Round(time.Millisecond))
		return requeue(bg, deps, agent, input, delay)
	}
	// Attempts exhausted: finalize FAILED on BOTH the hot cache and durable
	// truth, or subsequent runs would still treat the thread as active (§2.1)
	log.Error("run failed; attempts spent", "err", err, "attempts", attempts)
	if failErr := failRun(bg, deps, agent, input.ThreadID, input.RunID, err.Error()); failErr != nil {
		return errors.Join(err, failErr)
	}
	return deps.Kv.Del(bg, AttemptsKey(scope))
}

// requeue puts the same run back on the queue as a retry (§2.8). The
// thread reads QUEUED while it waits, so a client sees a run waiting to
// retry rather than one that looks like it is working.
func requeue(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, input ExecuteInput, delay time.Duration) error {
	if input.RunID != "" {
		if err := markQueued(ctx, deps, input.ThreadID, input.RunID, input.Model); err != nil {
			return err
		}
	}
	return EnqueueJob(ctx, deps, input.job(agent.Name, ports.JobRetry), &ports.EnqueueOptions{Delay: delay})
}

// markQueued moves a RUNNING thread back to QUEUED for a retry, on every
// home, and says so on the wire. A parked thread keeps WAITING_FOR_INPUT:
// the park machinery reads that state. A replaced run leaves the live run's
// state alone.
func markQueued(ctx context.Context, deps ports.RuntimePorts, threadID, runID, model string) error {
	current, err := CurrentRunID(ctx, deps, threadID)
	if err != nil || current != runID {
		return err
	}
	// Only a RUNNING thread that is still this run's (§3.4).
	won, err := Transition(ctx, deps, threadID, StateChange{
		From: []ports.ExecutionState{ports.StateRunning}, To: ports.StateQueued, RunID: runID, Model: model,
	})
	if err != nil || !won {
		return err
	}
	_ = deps.Admin.Runs().Patch(ctx, runID, ports.RunPatch{State: ports.Ptr(ports.StateQueued)})
	_, err = Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{
		"state": ports.StateQueued, "runId": runID, "enqueuedAt": time.Now(),
	})
	return err
}

// retryBackoff is how long the nth retry waits (§2.8): the base doubled per
// attempt, capped, with up to a quarter of jitter so a fleet that failed
// together does not retry together.
func retryBackoff(cfg ports.AgentConfig, attempt int) time.Duration {
	if cfg.RunRetryBackoff <= 0 {
		return 0
	}
	d := cfg.RunRetryBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if cfg.RunRetryBackoffMax > 0 && d >= cfg.RunRetryBackoffMax {
			d = cfg.RunRetryBackoffMax
			break
		}
	}
	if cfg.RunRetryBackoffMax > 0 && d > cfg.RunRetryBackoffMax {
		d = cfg.RunRetryBackoffMax
	}
	return d + time.Duration(rand.Int64N(int64(d)/4+1))
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

// FailLostRun fails a run the queue gave up on (§2.8), when it is still the
// thread's current run and the thread is still waiting on it. Returns false
// when the run had already moved on. Runs under the run lock like every
// other terminal write.
func FailLostRun(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, threadID, runID, reason string) (bool, error) {
	if runID == "" {
		return false, nil
	}
	current, err := CurrentRunID(ctx, deps, threadID)
	if err != nil || current != runID {
		return false, err
	}
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil || thread == nil || !IsActive(thread.State) {
		return false, err
	}
	lease, err := AcquireRunLock(ctx, deps, threadID, runID, "")
	if err != nil || lease == nil {
		return false, err // a worker still holds it; the run is not lost
	}
	lease.Keep(nil) // the settle hook in failRun can be slow
	defer lease.Release()
	if thread.State == ports.StateWaitingForInput {
		_ = closeOpenParks(ctx, deps, threadID)
	}
	err = failRun(ctx, deps, agent, threadID, runID, reason)
	// The dead worker never closed its segment's stream: a reader waiting on
	// it stops here.
	CloseLostSegment(ctx, deps, threadID, runID, reason)
	return true, err
}

// SettleLate settles an ended run whose settle never ran (§5.6), under the
// run lock. Returns false when the run was settled meanwhile or a worker
// holds it.
func SettleLate(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, threadID, runID string) (bool, error) {
	if runID == "" {
		return false, nil
	}
	lease, err := AcquireRunLock(ctx, deps, threadID, runID, "")
	if err != nil || lease == nil {
		return false, err
	}
	lease.Keep(nil) // renewed while the settle hook runs
	defer lease.Release()
	return settleEndedRun(ctx, deps, agent, threadID, runID), nil
}
