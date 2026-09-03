package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/zendev-sh/goai"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// HITLTTL is the default time a parked request stays answerable (§2.5).
const HITLTTL = 15 * time.Minute

// HITLParked is the key of the result a parked `RequiresConfirmation` tool
// returns (§2.5). The engine scans a step's tool results for this marker to
// end the run segment; it is never persisted as a tool result. The resumed
// segment appends the user's verdict (or the timeout denial) instead.
const HITLParked = "__hitl_parked__"

// HitlKey is the handoff key an answer is written to.
func HitlKey(toolCallID string) string { return "agent:hitl:" + toolCallID }

// HitlResponse is what /respond writes to the handoff key.
type HitlResponse struct {
	Approved bool `json:"approved"`
	Payload  any  `json:"payload,omitempty"`
}

// HitlFrame is one tool call left waiting on an approval further down
// (§2.7). A park by the main agent has none; a park inside a nested run has
// one per level, the innermost waiter first. AgentID names the stream the
// waiting call lives in; empty for the main agent.
type HitlFrame struct {
	AgentID    string `json:"agentId"`
	ToolCallID string `json:"toolCallId"`
	// Nested says how to re-enter the owner's loop when this frame unwinds.
	// Absent for the main agent, whose loop the engine re-enters itself.
	Nested *ports.NestedDescriptor `json:"nested,omitempty"`
}

// MarshalJSON writes the main agent's empty AgentID as null, matching the
// TypeScript package's persisted payloads.
func (f HitlFrame) MarshalJSON() ([]byte, error) {
	type alias HitlFrame
	return json.Marshal(struct {
		alias
		AgentID *string `json:"agentId"`
	}{alias: alias(f), AgentID: nullable(f.AgentID)})
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ParkInput describes a park.
type ParkInput struct {
	ThreadID   string
	ToolCallID string
	ToolName   string
	Args       json.RawMessage
	AgentID    string
	// Frames are the calls waiting on this answer, innermost first (§2.7).
	Frames []HitlFrame
	// Nested is the run that raised this park; nil when the main agent did.
	Nested *ports.NestedDescriptor
	// Resume is the dispatch ticket persisted in the INPUT_REQUIRED payload.
	Resume ports.ResumeInfo
}

// HitlCtx is what WithHitl needs to know about the stream it wraps.
type HitlCtx struct {
	Resume  ports.ResumeInfo
	AgentID string
	Frames  []HitlFrame
	Nested  *ports.NestedDescriptor
}

// ParkedResult is the sentinel a wrapped tool returns.
func ParkedResult(toolCallID string) string {
	b, _ := json.Marshal(map[string]string{HITLParked: toolCallID})
	return string(b)
}

// WithHitl wraps every marked tool so a call parks (§2.5) instead of
// executing: the request is persisted as INPUT_REQUIRED and the wrapper
// returns the park sentinel. Nothing blocks. The real tool runs when the park
// is resolved (see resumePendingHitl), in whichever stream owns it.
//
// Shared by the main agent and every nested run (§2.7): the only difference
// is the AgentID asking and the Frames waiting on the answer.
func WithHitl(deps ports.RuntimePorts, threadID string, tools []ports.Tool, hc HitlCtx) []ports.Tool {
	out := make([]ports.Tool, 0, len(tools))
	for _, t := range tools {
		if !t.RequiresConfirmation {
			out = append(out, t)
			continue
		}
		name := t.Name
		wrapped := t
		wrapped.Execute = func(ctx context.Context, input json.RawMessage) (string, error) {
			toolCallID := goai.ToolCallIDFromContext(ctx)
			if toolCallID == "" {
				toolCallID = NewID()
			}
			// The park must land even when the generation context is being
			// torn down; it is durable state, not part of the model call.
			if err := ParkForApproval(context.WithoutCancel(ctx), deps, ParkInput{
				ThreadID: threadID, ToolCallID: toolCallID, ToolName: name, Args: input,
				AgentID: hc.AgentID, Frames: hc.Frames, Nested: hc.Nested, Resume: hc.Resume,
			}); err != nil {
				return "", err
			}
			return ParkedResult(toolCallID), nil
		}
		out = append(out, wrapped)
	}
	return out
}

// inputRequiredPayload is the persisted INPUT_REQUIRED payload.
type inputRequiredPayload struct {
	ToolCallID  string                  `json:"toolCallId"`
	ToolName    string                  `json:"toolName"`
	AgentID     *string                 `json:"agentId"`
	Arguments   json.RawMessage         `json:"arguments"`
	InputSchema any                     `json:"inputSchema"`
	Frames      []HitlFrame             `json:"frames"`
	Nested      *ports.NestedDescriptor `json:"nested,omitempty"`
	Resume      *ports.ResumeInfo       `json:"resume,omitempty"`
}

// ParkForApproval is the §2.5 suspension as a durable state transition. NO
// process waits. It flips WAITING_FOR_INPUT on both homes and appends
// INPUT_REQUIRED to the replayable event log (with the resume ticket). The
// engine then ends the run segment; Respond (or the expiry job below)
// resumes it via the queue.
//
// The park also schedules its OWN expiry: one delayed dispatch of the same
// run, timed for just after the TTL. Without it the deadline only exists
// while somebody happens to be watching the thread. The delayed job holds no
// process; the queue holds it, exactly like the original dispatch (§2.8).
//
// It carries the PARKED run's id, so the answer and the expiry are two
// deliveries of one run: whichever resolves the park first wins, and the run
// lock makes the other a no-op.
func ParkForApproval(ctx context.Context, deps ports.RuntimePorts, i ParkInput) error {
	if _, err := deps.Kv.Set(ctx, StateKey(i.ThreadID), string(ports.StateWaitingForInput), ports.SetOptions{}); err != nil {
		return err
	}
	if err := SetThreadState(ctx, deps, i.ThreadID, ports.StateWaitingForInput, i.Resume.Model); err != nil {
		return err
	}
	args := i.Args
	if len(args) == 0 || !json.Valid(args) {
		args = json.RawMessage("null")
	}
	frames := i.Frames
	if frames == nil {
		frames = []HitlFrame{}
	}
	resume := i.Resume
	if _, err := Publish(ctx, deps, i.ThreadID, "INPUT_REQUIRED", inputRequiredPayload{
		ToolCallID: i.ToolCallID, ToolName: i.ToolName, AgentID: nullable(i.AgentID),
		Arguments: args, InputSchema: nil, Frames: frames, Nested: i.Nested, Resume: &resume,
	}); err != nil {
		return err
	}
	if _, err := Publish(ctx, deps, i.ThreadID, "STATE_CHANGE", map[string]any{"state": ports.StateWaitingForInput}); err != nil {
		return err
	}

	// Best-effort, and deliberately last. The park is ALREADY durable by this
	// point, so a queue that cannot schedule must not be allowed to fail the
	// run through the tool call. Reclamation (§2.5) covers the thread instead.
	// Arriving early is equally harmless: an unexpired, unanswered request
	// resolves to nothing and the job is a no-op (see resumePendingHitl).
	runID, _ := CurrentRunID(ctx, deps, i.ThreadID)
	_ = deps.Queue.Enqueue(ctx, ports.RunJob{
		ThreadID: i.ThreadID, RunID: runID, Model: i.Resume.Model, Agent: i.Resume.Agent,
		TokenBudget: i.Resume.TokenBudget, CostBudgetMicros: i.Resume.CostBudgetMicros,
		ProviderOptions: i.Resume.ProviderOptions, State: i.Resume.State,
		MaxSteps: i.Resume.MaxSteps,
	}, &ports.EnqueueOptions{Delay: ReclaimGraceAfter(deps)})
	return nil
}

// PendingHitl is the pending request behind a WAITING_FOR_INPUT thread,
// hydrated from the durable event log (§2.5).
type PendingHitl struct {
	ToolCallID string
	ToolName   string
	AgentID    string
	Arguments  json.RawMessage
	// Frames are the calls waiting on this answer, innermost first (§2.7).
	Frames []HitlFrame
	// Nested is the run that raised it; nil for a main-agent park.
	Nested *ports.NestedDescriptor
	// RequestedAt is when INPUT_REQUIRED was published: the TTL clock (§2.5).
	RequestedAt time.Time
	// Resume is the dispatch ticket, when the park recorded one.
	Resume *ports.ResumeInfo
}

func fromInputRequired(e ports.AgentEvent) (PendingHitl, error) {
	var p inputRequiredPayload
	if err := e.PayloadInto(&p); err != nil {
		return PendingHitl{}, err
	}
	agentID := ""
	if p.AgentID != nil {
		agentID = *p.AgentID
	}
	frames := p.Frames
	if frames == nil {
		frames = []HitlFrame{} // a park recorded before frames existed unwinds as a main-agent park
	}
	return PendingHitl{
		ToolCallID: p.ToolCallID, ToolName: p.ToolName, AgentID: agentID, Arguments: p.Arguments,
		Frames: frames, Nested: p.Nested, RequestedAt: e.CreatedAt, Resume: p.Resume,
	}, nil
}

// LoadPendingHitl reads the most recent pending request.
func LoadPendingHitl(ctx context.Context, deps ports.RuntimePorts, threadID string) (*PendingHitl, error) {
	e, err := deps.Storage.Events.Latest(ctx, threadID, "INPUT_REQUIRED")
	if err != nil || e == nil {
		return nil, err
	}
	p, err := fromInputRequired(*e)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// LoadOpenHitls lists every approval on the thread that is still open (§2.7).
//
// Derived from durable state, never cached: a request is settled once a tool
// result carries its ToolCallID, in whichever stream owns it, or an
// INPUT_EXPIRED event names it. Both are already written on the settling
// path.
func LoadOpenHitls(ctx context.Context, deps ports.RuntimePorts, threadID string) ([]PendingHitl, error) {
	requested, err := deps.Storage.Events.ListByType(ctx, threadID, "INPUT_REQUIRED")
	if err != nil || len(requested) == 0 {
		return nil, err
	}
	expiredEvents, err := deps.Storage.Events.ListByType(ctx, threadID, "INPUT_EXPIRED")
	if err != nil {
		return nil, err
	}
	expired := map[string]bool{}
	for _, e := range expiredEvents {
		var p struct {
			ToolCallID string `json:"toolCallId"`
		}
		if e.PayloadInto(&p) == nil && p.ToolCallID != "" {
			expired[p.ToolCallID] = true
		}
	}
	answered := map[string]bool{}
	rows, err := deps.Storage.Messages.List(ctx, threadID, nil)
	if err != nil {
		return nil, err
	}
	for _, m := range rows {
		if m.Role != ports.RoleTool {
			continue
		}
		for _, id := range ToolCallIDsIn(m.Content) {
			answered[id] = true
		}
	}
	var open []PendingHitl
	for _, e := range requested {
		p, err := fromInputRequired(e)
		if err != nil {
			continue
		}
		if !answered[p.ToolCallID] && !expired[p.ToolCallID] {
			open = append(open, p)
		}
	}
	return open, nil
}

// Respond is the §5.4 behavior: heal orphans first (§2.5), then record the
// answer in the handoff key and resume the run segment via the queue (§2.8).
// The resumed worker appends the tool result and continues the loop.
func Respond(ctx context.Context, deps ports.RuntimePorts, input ports.RespondInput) (ports.RespondResult, error) {
	if _, err := ReclaimIfOrphaned(ctx, deps, input.ThreadID); err != nil {
		return ports.RespondResult{}, err
	}
	thread, err := deps.Storage.Threads.Get(ctx, input.ThreadID)
	if err != nil {
		return ports.RespondResult{}, err
	}
	// ANY open request is answerable, not just the newest (§2.7): one parent
	// step can park several nested runs at once, and each is answered on its
	// own. The run resumes when the last of them is settled.
	open, err := LoadOpenHitls(ctx, deps, input.ThreadID)
	if err != nil {
		return ports.RespondResult{}, err
	}
	var match *PendingHitl
	for i := range open {
		if open[i].ToolCallID == input.ToolCallID {
			match = &open[i]
			break
		}
	}
	if thread == nil || thread.State != ports.StateWaitingForInput || match == nil {
		return ports.RespondResult{Delivered: false, Error: "No matching pending input request"}, nil
	}

	// Remaining-TTL expiry so a stale answer can never outlive its request:
	// the key vanishing is what makes the resumed segment treat the request
	// as unanswered (§2.5).
	remaining := deps.Config.HITLTTL - time.Since(match.RequestedAt)
	if remaining < time.Minute {
		remaining = time.Minute
	}
	answer, _ := json.Marshal(HitlResponse{Approved: input.Approved, Payload: input.Payload})
	if _, err := deps.Kv.Set(ctx, HitlKey(input.ToolCallID), string(answer), ports.SetOptions{Expiry: remaining}); err != nil {
		return ports.RespondResult{}, err
	}
	// Bus-only fast-path notice (seq 0 = never persisted) for live UIs (§2.5)
	_ = PublishNotice(ctx, deps, input.ThreadID, "HITL_RESPONSE", map[string]any{
		"toolCallId": input.ToolCallID, "approved": input.Approved,
	})

	// Resume the run segment through the queue, rebuilt from the ticket
	// persisted in the event payload. A legacy park without a ticket falls
	// back to the default handle. REUSE the parked run's id, never mint a new
	// one: this dispatch and the park's expiry job are the same run.
	runID, err := CurrentRunID(ctx, deps, input.ThreadID)
	if err != nil {
		return ports.RespondResult{}, err
	}
	job := ports.RunJob{ThreadID: input.ThreadID, RunID: runID, Model: thread.Model}
	if r := match.Resume; r != nil {
		job.Model = r.Model
		job.Agent = r.Agent
		job.TokenBudget = r.TokenBudget
		job.CostBudgetMicros = r.CostBudgetMicros
		job.ProviderOptions = r.ProviderOptions
		// The answer resumes the SAME run, so it scopes storage the same way
		// the parked segment did (§2.10).
		job.State = r.State
		job.MaxSteps = r.MaxSteps
	}
	if err := deps.Queue.Enqueue(ctx, job, nil); err != nil {
		return ports.RespondResult{}, fmt.Errorf("resume dispatch: %w", err)
	}
	return ports.RespondResult{Delivered: true}, nil
}
