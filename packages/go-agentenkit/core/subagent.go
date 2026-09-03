package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// SubagentCtx is everything a nested run needs from the run that spawned it
// (§2.7). The thread, lock, run id, abort and token ledger are the parent's;
// the message stream, step ceiling and toolset are the child's own.
type SubagentCtx struct {
	// IOCtx is the parent's storage context: not cancelled by a user stop, so
	// a stopped child can still close its record.
	IOCtx    context.Context
	ThreadID string
	Depth    int // 0 = called from the main agent
	Sem      *Semaphore
	Ports    ports.RuntimePorts
	// Sub is the delegation config carried from the parent's spec.
	Sub ports.SubagentsConfig
	// Agent is the registered agent whose generation args every nested run
	// inherits (§3.1); its System and Tools are overridden per child.
	Agent  *RegisteredAgent
	Ledger *RunLedger
	// Resume is the dispatch ticket persisted with any park raised beneath here.
	Resume ports.ResumeInfo
	// AgentID is the stream this spawner writes to; empty for the main agent.
	AgentID string
	// Frames are calls already waiting on an approval above this level.
	Frames []HitlFrame
	// Descriptor is this spawner's own; nil when it is the main agent.
	Descriptor  *ports.NestedDescriptor
	TokenBudget int
	// CostBudgetMicros is the run's money cap, shared with the parent (§4).
	CostBudgetMicros int64
	// BillingRunID is the DISPATCHED run every call beneath here is billed
	// to, so one run's bill is one query however deep the delegation went.
	BillingRunID    string
	ProviderOptions ports.ProviderOptions
	// Aborted reports a user stop (§2.1).
	Aborted func() bool
	// State is the run's state, handed down unchanged (§2.10).
	State ports.AgentRunState
}

// Semaphore is a run-scoped concurrency cap: sibling subagents queue instead
// of running away (§2.7).
type Semaphore struct{ slots chan struct{} }

// NewSemaphore makes a semaphore with limit slots.
func NewSemaphore(limit int) *Semaphore {
	if limit < 1 {
		limit = 1
	}
	return &Semaphore{slots: make(chan struct{}, limit)}
}

// Acquire takes a slot, waiting for one. The release is idempotent.
func (s *Semaphore) Acquire(ctx context.Context) (release func(), err error) {
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	released := false
	return func() {
		if released {
			return
		}
		released = true
		<-s.slots
	}, nil
}

type spawnInput struct {
	Name         string `json:"name" jsonschema:"description=Short name for the sub-task"`
	Instructions string `json:"instructions" jsonschema:"description=Complete self-contained brief: goal, constraints, expected output format"`
	Model        string `json:"model,omitempty" jsonschema:"description=Optional model registry key"`
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// profileFor resolves a named specialist (§2.7), or nil when the config
// has no profiles or none by that name.
func profileFor(sctx *SubagentCtx, name string) *ports.SubagentProfile {
	if sctx == nil || len(sctx.Sub.Profiles) == 0 {
		return nil
	}
	p, ok := sctx.Sub.Profiles[name]
	if !ok {
		return nil
	}
	return &p
}

// nestedRawTools is the unwrapped toolset a nested run owns: its profile's
// when it has one, the shared delegation tools otherwise. The resolved park
// executes the approved tool from here.
func nestedRawTools(sctx *SubagentCtx, d *ports.NestedDescriptor) []ports.Tool {
	if d != nil {
		if p := profileFor(sctx, d.Name); p != nil {
			return p.Tools
		}
	}
	return sctx.Sub.Tools
}

func nestedModelName(sctx *SubagentCtx, profile *ports.SubagentProfile, requested string) string {
	if requested != "" {
		return requested
	}
	if profile != nil && profile.Model != "" {
		return profile.Model
	}
	if sctx.Sub.Model != "" {
		return sctx.Sub.Model
	}
	return DefaultModel
}

// profileNames lists the specialists, sorted, for the tool's description and
// its error messages.
func profileNames(sctx *SubagentCtx) []string {
	names := make([]string, 0, len(sctx.Sub.Profiles))
	for name := range sctx.Sub.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// spawnDescription is the delegation tool's description. With profiles, the
// model is told exactly who it can delegate to.
func spawnDescription(sctx *SubagentCtx) string {
	desc := "Delegates a self-contained task to a subagent with an isolated context"
	if len(sctx.Sub.Profiles) == 0 {
		return desc
	}
	var b strings.Builder
	b.WriteString(desc)
	b.WriteString(". name MUST be one of the available subagents: ")
	for i, name := range profileNames(sctx) {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(name)
		if d := sctx.Sub.Profiles[name].Description; d != "" {
			b.WriteString(" (")
			b.WriteString(d)
			b.WriteString(")")
		}
	}
	return b.String()
}

// SpawnSubagentTool builds the run-scoped delegation tool (§2.7).
func SpawnSubagentTool(sctx *SubagentCtx) ports.Tool {
	return ports.WrapTool(goai.NewTool("spawnSubagent",
		spawnDescription(sctx),
		func(ctx context.Context, in spawnInput) (string, error) {
			cfg := sctx.Ports.Config
			depth := sctx.Depth + 1
			if depth > cfg.SubagentMaxDepth {
				return jsonString(map[string]any{"error": fmt.Sprintf("Max subagent depth (%d) reached", cfg.SubagentMaxDepth)}), nil
			}
			// With profiles, an unknown name is ordinary bad input reported
			// to the model, never a crash (§2.7).
			profile := profileFor(sctx, in.Name)
			if len(sctx.Sub.Profiles) > 0 && profile == nil {
				return jsonString(map[string]any{"error": fmt.Sprintf(
					"Unknown subagent %q; use one of: %s", in.Name, strings.Join(profileNames(sctx), ", "))}), nil
			}
			release, err := sctx.Sem.Acquire(ctx)
			if err != nil {
				return "", err
			}
			defer release()

			io := sctx.IOCtx
			toolCallID := goai.ToolCallIDFromContext(ctx)
			// A nested run is a run (§2.9): same table, distinguished by depth
			// and a parent. Its id is also the agentId its messages carry.
			parent := sctx.AgentID
			if parent == "" {
				parent = sctx.Resume.RunID
			}
			rec := ports.NewRunRecord{
				ID: NewID(), ThreadID: sctx.ThreadID, ParentRunID: parent, Depth: depth,
				Agent: in.Name, Model: nestedModelName(sctx, profile, in.Model),
			}
			if cfg.RecordPayloads {
				// A nested run's "prompt" is the brief it was delegated (§2.7).
				rec.Prompt = capText(in.Instructions, cfg.PayloadCapChars)
				rec.RunState = sctx.State
			}
			run, err := sctx.Ports.Admin.Runs().Start(io, rec)
			if err != nil {
				return "", err
			}
			if toolCallID == "" {
				toolCallID = run.ID
			}
			if _, err := Publish(io, sctx.Ports, sctx.ThreadID, "SUBAGENT_STARTED", map[string]any{
				"agentId": run.ID, "name": in.Name, "depth": depth,
			}); err != nil {
				return "", err
			}
			descriptor := ports.NestedDescriptor{AgentID: run.ID, Name: in.Name, Model: rec.Model, Depth: depth}

			// This call is now the innermost thing waiting on any approval the
			// child raises (§2.7).
			frames := append([]HitlFrame{{AgentID: sctx.AgentID, ToolCallID: toolCallID, Nested: sctx.Descriptor}}, sctx.Frames...)
			instructions := in.Instructions
			outcome, err := RunNestedAgent(ctx, sctx, descriptor, &instructions, frames)
			if err == nil && outcome.Aborted {
				err = context.Canceled
			}
			if err != nil {
				cancelled := sctx.Aborted != nil && sctx.Aborted()
				if !cancelled {
					if st, _, _ := sctx.Ports.Kv.Get(io, StateKey(sctx.ThreadID)); st == string(ports.StateCancelled) {
						cancelled = true
					}
				}
				state := ports.StateFailed
				if cancelled {
					state = ports.StateCancelled
				}
				msg := err.Error()
				closeNested(io, sctx, run, nil, ports.RunPatch{State: &state, Error: &msg})
				// Carry the reason: a bare state tells an operator a child died
				// but not why.
				_, _ = Publish(io, sctx.Ports, sctx.ThreadID, "SUBAGENT_FAILED", map[string]any{
					"agentId": run.ID, "state": state, "error": msg,
				})
				// A user stop tears the whole run down (§2.1), so that one
				// keeps propagating.
				if cancelled {
					return "", err
				}
				// Anything else is reported TO THE PARENT as the delegation's
				// result, the same way an approved tool's failure is reported
				// to the model rather than thrown (§2.5).
				return jsonString(map[string]any{"agentId": run.ID, "error": msg}), nil
			}
			if outcome.Parked {
				// The child is suspended, not finished: leave its record
				// RUNNING and hand the parent the sentinel so its segment ends
				// too (§2.5). The child is re-entered on approval, from its own
				// persisted turns; it never restarts.
				return ParkedResult(toolCallID), nil
			}
			completed := ports.StateCompleted
			closeNested(io, sctx, run, outcome, ports.RunPatch{
				State: &completed, Result: MarshalPayload(map[string]any{"text": outcome.Text}),
			})
			_, _ = Publish(io, sctx.Ports, sctx.ThreadID, "SUBAGENT_COMPLETED", map[string]any{"agentId": run.ID})
			// The parent receives a capped result, keeping its own context small (§2.6)
			return jsonString(map[string]any{
				"agentId": run.ID, "result": capRunes(outcome.Text, cfg.SubagentResultCapChars),
			}), nil
		}))
}

func capRunes(s string, limit int) string {
	r := []rune(s)
	if limit > 0 && len(r) > limit {
		return string(r[:limit])
	}
	return s
}

// closeNested closes a nested run's record with the same detail a
// dispatched run gets (§2.9): how it ended, how long it took, what it cost.
func closeNested(ctx context.Context, sctx *SubagentCtx, run *ports.RunRecord, outcome *LoopOutcome, end ports.RunPatch) {
	endedAt := time.Now()
	end.EndedAt = &endedAt
	end.DurationMs = ports.Ptr(endedAt.Sub(run.StartedAt).Milliseconds())
	if outcome != nil {
		end.Steps = ports.Ptr(outcome.Steps)
		end.InputTokens = ports.Ptr(outcome.Attribution.InputTokens)
		end.CachedInputTokens = ports.Ptr(outcome.Attribution.CachedInputTokens)
		end.OutputTokens = ports.Ptr(outcome.Attribution.OutputTokens)
		end.TotalTokens = ports.Ptr(outcome.Attribution.TotalTokens)
	}
	_ = sctx.Ports.Admin.Runs().Patch(ctx, run.ID, end)
}

// nestedTools is the toolset a nested run sees (§2.7): the delegation
// config's extra tools, HITL-wrapped exactly like the parent's, plus nesting
// while depth allows. Default is spawnSubagent alone; destructive tools reach
// a child only when a workflow grants them.
func nestedTools(sctx *SubagentCtx, d ports.NestedDescriptor, frames []HitlFrame) []ports.Tool {
	child := *sctx
	child.Depth = d.Depth
	child.AgentID = d.AgentID
	child.Frames = frames
	desc := d
	child.Descriptor = &desc
	raw := append([]ports.Tool{}, nestedRawTools(sctx, &d)...)
	raw = append(raw, SpawnSubagentTool(&child))
	// A nested run's tools see the same state as its parent's (§2.10), and
	// publish on the same thread.
	return WithRunState(WithPublishEvent(sctx.Ports, sctx.ThreadID, WithHitl(sctx.Ports, sctx.ThreadID, raw, HitlCtx{
		Resume: sctx.Resume, AgentID: d.AgentID, Frames: frames, Nested: &desc,
	})), sctx.State)
}

// resolveNestedModel: the delegation tool lets the MODEL name the child's
// model, so an unknown registry key is ordinary bad input rather than a
// failure. The child falls back to the model its parent is already running
// on, which is resolvable by construction.
// The registry key comes back with the model: pricing is keyed by the key
// that was actually resolved, not the one the delegation asked for.
func resolveNestedModel(sctx *SubagentCtx, name string) (ports.ResolvedModel, string, error) {
	if m, err := sctx.Ports.ResolveModel(name); err == nil {
		return m, name, nil
	}
	m, err := sctx.Ports.ResolveModel(sctx.Resume.Model)
	return m, sctx.Resume.Model, err
}

// RunNestedAgent runs, or RE-ENTERS, a nested agent (§2.7).
//
// Its turns live in the thread's message log under its own AgentID, so a
// child that parked is resumed from exactly where it stopped rather than
// replayed from its brief. Replaying an LLM call is not a safe substitute:
// the model can take a different path and never make the call the human
// approved, and it re-pays for everything before the park.
//
// genCtx is the context the child's model calls run under (cancelled by a
// user stop); storage calls use the spawner's IOCtx.
func RunNestedAgent(genCtx context.Context, sctx *SubagentCtx, d ports.NestedDescriptor, instructions *string, frames []HitlFrame) (*LoopOutcome, error) {
	deps, threadID := sctx.Ports, sctx.ThreadID
	io := sctx.IOCtx
	if io == nil {
		io = context.WithoutCancel(genCtx)
	}
	// A nested run is a run: its model calls carry its own id (§2.9).
	genCtx = ContextWithRunID(genCtx, d.AgentID)

	persisted, err := deps.Storage.Messages.List(io, threadID, ports.AgentScope(d.AgentID))
	if err != nil {
		return nil, err
	}
	if len(persisted) == 0 {
		if instructions == nil {
			return nil, fmt.Errorf("nested run %s has no turns and no brief to seed from", d.AgentID)
		}
		// Isolated context (§2.7): the brief is the only input; parent history
		// is never forwarded.
		seed, err := deps.Storage.Messages.Append(io, threadID, ports.NewMessage{
			Role: ports.RoleUser, Content: TextContent(*instructions), AgentID: d.AgentID,
		})
		if err != nil {
			return nil, err
		}
		persisted = append(persisted, *seed)
	}

	model, modelKey, err := resolveNestedModel(sctx, d.Model)
	if err != nil {
		return nil, err
	}
	kind := sctx.Sub.Kind
	if kind == "" {
		kind = ports.KindStreamText
	}
	messages := RepairDanglingToolCalls(MessagesFromDTOs(persisted))
	if deps.Config.PromptCaching {
		// Stamped like the parent's (§2.6). A nested run re-sends its whole
		// brief and history on every step, exactly the shape caching is for.
		messages = MarkPromptCaching(messages)
	}
	// A profile brings its own persona and step cap (§2.7); the descriptor
	// carries the name, so a re-entry after an approval finds the same one.
	system := fmt.Sprintf("You are the %q subagent. Complete the task, then stop.", d.Name)
	var systemFn ports.SystemFunc
	var prepareStep ports.PrepareStepFunc
	maxSteps := deps.Config.SubagentMaxSteps
	if p := profileFor(sctx, d.Name); p != nil {
		if p.System != "" {
			system = p.System
		}
		systemFn = p.SystemFn
		prepareStep = p.PrepareStep
		if p.MaxSteps > 0 && p.MaxSteps < maxSteps {
			maxSteps = p.MaxSteps
		}
	}
	outcome, err := RunLoop(io, deps, sctx.Agent, threadID, LoopInput{
		AgentID: d.AgentID,
		// Its OWN run id, not its parent's: a nested run is a run (§2.7, §2.9).
		RunID:    d.AgentID,
		Kind:     kind,
		Model:    model.Instance(),
		Messages: messages,
		Tools:    nestedTools(sctx, d, frames),
		MaxSteps: maxSteps,
		GenCtx:   genCtx, Aborted: sctx.Aborted,
		ProviderOptions: sctx.ProviderOptions,
		TokenBudget:     sctx.TokenBudget,
		// Money is capped and billed at the RUN, not per child (§2.7, §4):
		// the cap is the parent's, and every call a child makes lands on the
		// parent's bill under its own AgentID.
		CostBudgetMicros:  sctx.CostBudgetMicros,
		BillingRunID:      sctx.BillingRunID,
		ModelKey:          modelKey,
		AgentName:         d.Name,
		System:            system,
		SystemFn:          systemFn,
		PrepareStep:       prepareStep,
		State:             sctx.State,
		CacheSystemPrompt: deps.Config.PromptCaching,
		OnChunk: func(chunk provider.StreamChunk) {
			// Namespaced into the shared thread event log → same multi-user pipeline (§2.2)
			_, _ = Publish(io, deps, threadID, "SUBAGENT_CHUNK", map[string]any{
				"agentId": d.AgentID, "chunk": ChunkPayload(chunk),
			})
		},
	}, sctx.Ledger)
	if err != nil {
		return nil, err
	}
	// Nothing to bill here: every call the child made recorded and priced its
	// own row as it happened, tagged with this child's AgentID (§4). The
	// run-wide ledger was advanced inside the loop too.
	return outcome, nil
}
