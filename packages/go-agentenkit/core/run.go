package core

import (
	"context"
	"errors"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// DefaultModel is used when neither the run input nor the spec names one.
const DefaultModel = "gpt-4o"

// capText keeps a recorded value small (§2.9).
func capText(text string, limit int) string {
	if limit > 0 && len([]rune(text)) > limit {
		return string([]rune(text)[:limit]) + "…"
	}
	return text
}

// Run is the §5.1 behavior: heal orphans → billing pre-check (§4) → persist
// the user message → state RUNNING (hot + durable) → enqueue on the dispatch
// queue (§2.8). It accepts no execution responsibility whatsoever; the queue
// does the rest, and the job dispatches back to THIS handle.
func Run(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, input ports.RunInput) (result ports.RunResult, runErr error) {
	// Model resolution order (§3.1): run input → spec default → DefaultModel
	model := input.Model
	if model == "" {
		model = agent.Spec.Model
	}
	if model == "" {
		model = DefaultModel
	}

	threadID := input.ThreadID
	if threadID == "" {
		created, err := deps.Storage.Threads.Create(ctx, ports.ThreadInit{Model: model})
		if err != nil {
			return ports.RunResult{}, err
		}
		threadID = created.ID
	}
	refuse := func(msg string) (ports.RunResult, error) {
		return ports.RunResult{Accepted: false, ThreadID: threadID, Error: msg}, nil
	}

	// Heal an orphaned HITL wait first (§2.5)
	if _, err := ReclaimIfOrphaned(ctx, deps, threadID); err != nil {
		return ports.RunResult{}, err
	}

	state, _, err := deps.Kv.Get(ctx, StateKey(threadID))
	if err != nil {
		return ports.RunResult{}, err
	}
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil {
		return ports.RunResult{}, err
	}
	if thread == nil {
		return refuse("Thread not found")
	}
	initialState := thread.State
	if state == string(ports.StateRunning) || state == string(ports.StateWaitingForInput) || thread.State == ports.StateRunning || thread.State == ports.StateWaitingForInput {
		return refuse("Thread has an active run")
	}

	// Billing pre-execution check (§4), a user-injected hook. A refusal is
	// published on the thread as well as returned, so every client on the
	// thread sees it, and a reload still shows it.
	if deps.Config.BillingPreCheck != nil {
		check := ports.BillingCheck{
			ThreadID: threadID, State: input.State,
			PublishEvent: func(ctx context.Context, typ string, payload any, notice bool) (ports.AgentEvent, error) {
				return PublishEvent(ctx, deps, threadID, typ, payload, PublishOptions{Notice: notice})
			},
		}
		if err := deps.Config.BillingPreCheck(ctx, check); err != nil {
			_, _ = Publish(ctx, deps, threadID, "RUN_REFUSED", map[string]any{"reason": "billing", "error": err.Error()})
			return refuse(err.Error())
		}
	}

	// A caller-named run (§2.1) is checked before anything is written: a
	// reused id must refuse, never resend.
	if input.RunID != "" {
		existing, err := deps.Admin.Runs().Get(ctx, input.RunID)
		if err != nil {
			return ports.RunResult{}, err
		}
		if existing != nil {
			return refuse("Run id already used")
		}
	}
	if input.MaxSteps < 0 {
		return refuse("maxSteps must be zero or a positive number")
	}
	// A run may cap itself below the config, never above it.
	maxSteps := input.MaxSteps
	if maxSteps > deps.Config.MaxSteps {
		maxSteps = deps.Config.MaxSteps
	}

	// Edit + resend (§5.1): the edited turn and everything it led to are
	// dropped, then the new text is appended in its place. Only a user turn
	// may be edited: cutting from anywhere else can strip a tool result off
	// the assistant tool-call that produced it.
	if input.EditMessageID != "" {
		// Only the main agent's turns are editable (§2.7)
		history, err := deps.Storage.Messages.List(ctx, threadID, ports.MainAgent)
		if err != nil {
			return ports.RunResult{}, err
		}
		var target *ports.MessageDTO
		for i := range history {
			if history[i].ID == input.EditMessageID {
				target = &history[i]
				break
			}
		}
		if target == nil {
			return refuse("Message not found")
		}
		if target.Role != ports.RoleUser {
			return refuse("Only a user message can be edited")
		}
	}

	// Claim durable state before changing history. Only the winning send can
	// append a message or dispatch a job, even when the hot cache is missing.
	runID := input.RunID
	if runID == "" {
		runID = NewID()
	}
	previousRunID, err := CurrentRunID(ctx, deps, threadID)
	if err != nil {
		return ports.RunResult{}, err
	}
	admitted, err := deps.Storage.Threads.ClaimState(ctx, threadID, initialState, ports.StateRunning)
	if err != nil {
		return ports.RunResult{}, err
	}
	if !admitted {
		return refuse("Thread has an active run")
	}
	installed := false
	defer func() {
		if runErr == nil {
			return
		}
		original := runErr.Error()
		var prior *string
		if !installed {
			prior = &previousRunID
		}
		cleanupErr := failDispatch(context.WithoutCancel(ctx), deps, threadID, runID, model, original, prior)
		runErr = errors.Join(runErr, cleanupErr)
		result = ports.RunResult{Accepted: false, ThreadID: threadID, RunID: runID, Error: original}
	}()
	if _, err := ClaimRunAs(ctx, deps, threadID, runID); err != nil {
		return ports.RunResult{}, err
	}
	installed = true

	if _, err := deps.Kv.Set(ctx, StateKey(threadID), string(ports.StateRunning), ports.SetOptions{}); err != nil {
		return ports.RunResult{}, err
	}
	// The run's durable record opens here (§2.9).
	rec := ports.NewRunRecord{ID: runID, ThreadID: threadID, Agent: agent.Name, Model: model}
	start := &ports.ThreadStart{RunID: runID, Agent: agent.Name, Model: model, At: time.Now()}
	if deps.Config.RecordPayloads {
		rec.Prompt = capText(input.Prompt, deps.Config.PayloadCapChars)
		if input.TokenBudget > 0 {
			rec.TokenBudget = ports.Ptr(input.TokenBudget)
		}
		rec.RunState = input.State
		rec.ProviderOptions = providerOptionsFor(deps, agent, input)
		start.Prompt, start.TokenBudget, start.State, start.ProviderOptions = rec.Prompt, rec.TokenBudget, rec.RunState, rec.ProviderOptions
	}
	if _, err := deps.Admin.Runs().Start(ctx, rec); err != nil {
		return ports.RunResult{}, err
	}
	if active, err := dispatchActive(ctx, deps, threadID, runID); err != nil {
		return ports.RunResult{}, err
	} else if !active {
		return refuse("Run was stopped before dispatch")
	}

	if input.EditMessageID != "" {
		if _, err := deps.Storage.Messages.DeleteFrom(ctx, threadID, input.EditMessageID); err != nil {
			return ports.RunResult{}, err
		}
		// Other clients are showing turns that no longer exist (§2.2).
		if _, err := Publish(ctx, deps, threadID, "MESSAGES_DROPPED", map[string]any{"fromMessageId": input.EditMessageID}); err != nil {
			return ports.RunResult{}, err
		}
	}

	userMessage, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
		Role: ports.RoleUser, Content: UserContent(input.Prompt, input.Attachments),
	})
	if err != nil {
		return ports.RunResult{}, err
	}
	// The user's turn goes on the bus like everything else (§2.2). Without it
	// a second client watching the same thread sees the reply stream in with
	// no question in front of it.
	if _, err := Publish(ctx, deps, threadID, "MESSAGE_APPENDED", map[string]any{
		"id": userMessage.ID, "role": userMessage.Role, "content": userMessage.Content,
		"agentId": nullable(userMessage.AgentID), "createdAt": userMessage.CreatedAt,
	}); err != nil {
		return ports.RunResult{}, err
	}

	if active, err := dispatchActive(ctx, deps, threadID, runID); err != nil {
		return ports.RunResult{}, err
	} else if !active {
		return refuse("Run was stopped before dispatch")
	}
	// A durable run boundary lets reconnecting clients distinguish this
	// turn's in-flight chunks from earlier completed turns.
	if _, err := Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{"state": ports.StateRunning}); err != nil {
		return ports.RunResult{}, err
	}

	// What started the thread (§2.9), recorded once: the first dispatched
	// run's parameters. A later run never overwrites it. Observability must
	// never fail a run, so this is best-effort.
	_ = deps.Admin.Threads().Upsert(ctx, ports.NewAdminThread{ID: threadID, State: ports.StateRunning, Model: model, StartedWith: start})

	if active, err := dispatchActive(ctx, deps, threadID, runID); err != nil {
		return ports.RunResult{}, err
	} else if !active {
		return refuse("Run was stopped before dispatch")
	}
	if err := deps.Queue.Enqueue(ctx, ports.RunJob{
		ThreadID: threadID, RunID: runID, Model: model, Agent: agent.Name,
		EnqueuedAt: time.Now().UnixMilli(),
		// Persisted on the ticket so a worker, or a resume after an approval,
		// hours later, in another process, rehydrates the same state (§2.10).
		State: input.State, TokenBudget: input.TokenBudget,
		CostBudgetMicros: input.CostBudgetMicros, ProviderOptions: input.ProviderOptions,
		MaxSteps: maxSteps,
	}, nil); err != nil {
		return ports.RunResult{}, err
	}
	return ports.RunResult{Accepted: true, ThreadID: threadID, RunID: runID, State: ports.StateRunning}, nil
}

// A stop can arrive while the request is still preparing its dispatch.
func dispatchActive(ctx context.Context, deps ports.RuntimePorts, threadID, runID string) (bool, error) {
	current, err := CurrentRunID(ctx, deps, threadID)
	if err != nil {
		return false, err
	}
	thread, err := deps.Storage.Threads.Get(ctx, threadID)
	if err != nil {
		return false, err
	}
	if current == runID && thread != nil && thread.State == ports.StateRunning {
		return true, nil
	}
	recordStoppedRun(ctx, deps, runID, time.Now())
	if current == runID && thread != nil && thread.State == ports.StateCancelled {
		_, err = deps.Kv.Set(ctx, StateKey(threadID), string(ports.StateCancelled), ports.SetOptions{})
	}
	return false, err
}

// A dispatch rejected by the queue must release the thread for another run.
// Conditional cleanup cannot replace a cancellation or a newer admission.
func failDispatch(ctx context.Context, deps ports.RuntimePorts, threadID, runID, model, reason string, previousRunID *string) error {
	current, err := CurrentRunID(ctx, deps, threadID)
	if err != nil {
		return err
	}
	if current != runID && (previousRunID == nil || current != *previousRunID) {
		return nil
	}
	changed, err := deps.Storage.Threads.ClaimState(ctx, threadID, ports.StateRunning, ports.StateFailed)
	if err != nil || !changed {
		return err
	}
	if _, err := deps.Kv.Set(ctx, StateKey(threadID), string(ports.StateFailed), ports.SetOptions{}); err != nil {
		return err
	}
	if err := SetThreadState(ctx, deps, threadID, ports.StateFailed, model); err != nil {
		return err
	}
	closeRunRecord(ctx, deps, runID, FinalizeInput{State: ports.StateFailed, StopReason: "failed", Error: reason})
	_, err = Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{
		"state": ports.StateFailed, "stopReason": "failed", "runId": runID, "error": reason,
	})
	return err
}

// providerOptionsFor is the provider options a run is dispatched with (§3.1):
// config → spec → input, each winning over the one before, per provider
// namespace. Nil when no level sets any, so the column stays empty.
func providerOptionsFor(deps ports.RuntimePorts, agent *RegisteredAgent, input ports.RunInput) ports.ProviderOptions {
	merged := ports.MergeProviderOptions(
		ports.MergeProviderOptions(deps.Config.ProviderOptions, agent.Spec.ProviderOptions), input.ProviderOptions)
	if len(merged) == 0 {
		return nil
	}
	return merged
}
