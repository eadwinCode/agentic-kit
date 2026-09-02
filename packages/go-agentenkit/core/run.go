package core

import (
	"context"
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
func Run(ctx context.Context, deps ports.RuntimePorts, agent *RegisteredAgent, input ports.RunInput) (ports.RunResult, error) {
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
	if state == string(ports.StateRunning) || state == string(ports.StateWaitingForInput) {
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
		if _, err := deps.Storage.Messages.DeleteFrom(ctx, threadID, input.EditMessageID); err != nil {
			return ports.RunResult{}, err
		}
		// Other clients are showing turns that no longer exist (§2.2).
		if _, err := Publish(ctx, deps, threadID, "MESSAGES_DROPPED", map[string]any{"fromMessageId": input.EditMessageID}); err != nil {
			return ports.RunResult{}, err
		}
	}

	userMessage, err := deps.Storage.Messages.Append(ctx, threadID, ports.NewMessage{
		Role: ports.RoleUser, Content: TextContent(input.Prompt),
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

	// Claim the thread for THIS run before the state key is touched (§2.1).
	runID, err := ClaimRun(ctx, deps, threadID)
	if err != nil {
		return ports.RunResult{}, err
	}
	if _, err := deps.Kv.Set(ctx, StateKey(threadID), string(ports.StateRunning), ports.SetOptions{}); err != nil {
		return ports.RunResult{}, err
	}
	if err := SetThreadState(ctx, deps, threadID, ports.StateRunning, model); err != nil {
		return ports.RunResult{}, err
	}
	// A durable run boundary lets reconnecting clients distinguish this
	// turn's in-flight chunks from earlier completed turns.
	if _, err := Publish(ctx, deps, threadID, "STATE_CHANGE", map[string]any{"state": ports.StateRunning}); err != nil {
		return ports.RunResult{}, err
	}

	// The run's durable record opens here (§2.9).
	rec := ports.NewRunRecord{ID: runID, ThreadID: threadID, Agent: agent.Name, Model: model}
	if deps.Config.RecordPayloads {
		rec.Prompt = capText(input.Prompt, deps.Config.PayloadCapChars)
		if input.TokenBudget > 0 {
			rec.TokenBudget = ports.Ptr(input.TokenBudget)
		}
		rec.RunState = input.State
	}
	if _, err := deps.Admin.Runs().Start(ctx, rec); err != nil {
		return ports.RunResult{}, err
	}

	if err := deps.Queue.Enqueue(ctx, ports.RunJob{
		ThreadID: threadID, RunID: runID, Model: model, Agent: agent.Name,
		EnqueuedAt: time.Now().UnixMilli(),
		// Persisted on the ticket so a worker, or a resume after an approval,
		// hours later, in another process, rehydrates the same state (§2.10).
		State: input.State, TokenBudget: input.TokenBudget, ProviderOptions: input.ProviderOptions,
	}, nil); err != nil {
		return ports.RunResult{}, err
	}
	return ports.RunResult{Accepted: true, ThreadID: threadID, RunID: runID, State: ports.StateRunning}, nil
}
