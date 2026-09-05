package core

import (
	"context"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// SpecDefaults are the spec-level defaults the engine reads.
type SpecDefaults struct {
	Model            string
	Subagents        *ports.SubagentsConfig
	TokenBudget      int
	CostBudgetMicros int64
	ProviderOptions  ports.ProviderOptions
}

// AgentArgs are the user's generation args: applied first, platform keys
// last (§3.1).
type AgentArgs struct {
	System      string
	SystemFn    ports.SystemFunc
	PrepareStep ports.PrepareStepFunc
	Tools       []ports.Tool
	Options     []goai.Option
	OnChunk     func(provider.StreamChunk)
	OnSettle    ports.SettleFunc
	OnFinish    func(ports.RunFinishInfo)
}

// RegisteredAgent is the registry entry behind a handle: the bound
// generation flavor, the spec-level defaults, and the user's generation args.
type RegisteredAgent struct {
	Name string
	Kind ports.AgentKind
	Spec SpecDefaults
	Args AgentArgs
	Sem  *Semaphore
}

// ScopeFn resolves the ports for one call, binding that call's state to
// storage (§2.10). A handle outlives many runs, so it cannot hold fixed ports.
type ScopeFn func(state ports.AgentRunState, runID string) ports.RuntimePorts

// Handle is an executor bound to a generation flavor and to the user's
// generation arguments (§3). Returned by the Create*Agent factories.
type Handle struct {
	agent *RegisteredAgent
	scope ScopeFn
	// lookup finds the other agents registered beside this one, by name.
	// Nil for a handle built outside a runtime.
	lookup func(name string) *RegisteredAgent
}

// Registry lets the handle find the OTHER agents registered beside it, by
// name. Stop needs it: the run it ends may belong to any of them, and the
// settle hook that closes that run's books is that agent's (§5.6). Without
// one, a stop settles only this handle's own runs.
func (h *Handle) Registry(lookup func(name string) *RegisteredAgent) { h.lookup = lookup }

// resolveAgent is the run record's agent name back to its registration.
func (h *Handle) resolveAgent(name string) *RegisteredAgent {
	if h.lookup != nil {
		if a := h.lookup(name); a != nil {
			return a
		}
	}
	if name == h.agent.Name {
		return h.agent
	}
	return nil
}

// Name is the handle's registry key.
func (h *Handle) Name() string { return h.agent.Name }

// Kind is the generation flavor.
func (h *Handle) Kind() ports.AgentKind { return h.agent.Kind }

// Agent exposes the registry entry.
func (h *Handle) Agent() *RegisteredAgent { return h.agent }

// Execute is worker-side only (§5.6). Returns OutcomeLockConflict when
// another worker owns the thread's run lock (nothing was executed) and
// OutcomeStale when a newer run has replaced this one (§2.1, §2.8).
func (h *Handle) Execute(ctx context.Context, input ExecuteInput) (ExecuteOutcome, error) {
	return Execute(ctx, h.scope(input.State, input.RunID), h.agent, input)
}

// ExecuteWithPolicy is Execute + the §2.8 failure policy.
func (h *Handle) ExecuteWithPolicy(ctx context.Context, input ExecuteInput, policy *Policy) error {
	return ExecuteWithPolicy(ctx, h.scope(input.State, input.RunID), h.agent, input, policy)
}

// Run persists the user message → state RUNNING → enqueues a job dispatched
// back to THIS handle.
func (h *Handle) Run(ctx context.Context, input ports.RunInput) (ports.RunResult, error) {
	return Run(ctx, h.scope(input.State, ""), h.agent, input)
}

// Stop is the platform stop (§2.1); works regardless of which agent's run
// is active. A run it ends with no worker holding it is settled here, by
// the agent that run belongs to (§5.6).
func (h *Handle) Stop(ctx context.Context, threadID string, state ports.AgentRunState) (ports.StopResult, error) {
	return StopRun(ctx, h.scope(state, ""), threadID, StopOptions{Agent: h.resolveAgent})
}

func newHandle(scope ScopeFn, agent *RegisteredAgent) *Handle {
	if agent.Sem == nil {
		agent.Sem = NewSemaphore(scope(nil, "").Config.SubagentMaxConcurrent)
	}
	return &Handle{agent: agent, scope: scope}
}

// NewStreamTextAgent registers a stream-text handle.
func NewStreamTextAgent(scope ScopeFn, spec ports.StreamTextAgentSpec) *Handle {
	return newHandle(scope, &RegisteredAgent{
		Name: spec.Name, Kind: ports.KindStreamText,
		Spec: SpecDefaults{Model: spec.Model, Subagents: spec.Subagents, TokenBudget: spec.TokenBudget,
			CostBudgetMicros: spec.CostBudgetMicros, ProviderOptions: spec.ProviderOptions},
		Args: AgentArgs{System: spec.System, SystemFn: spec.SystemFn, PrepareStep: spec.PrepareStep, Tools: spec.Tools, Options: spec.Options, OnChunk: spec.OnChunk, OnSettle: spec.OnSettle, OnFinish: spec.OnFinish},
	})
}

// NewGenerateTextAgent registers a generate-text handle.
func NewGenerateTextAgent(scope ScopeFn, spec ports.GenerateTextAgentSpec) *Handle {
	return newHandle(scope, &RegisteredAgent{
		Name: spec.Name, Kind: ports.KindGenerateText,
		Spec: SpecDefaults{Model: spec.Model, Subagents: spec.Subagents, TokenBudget: spec.TokenBudget,
			CostBudgetMicros: spec.CostBudgetMicros, ProviderOptions: spec.ProviderOptions},
		Args: AgentArgs{System: spec.System, SystemFn: spec.SystemFn, PrepareStep: spec.PrepareStep, Tools: spec.Tools, Options: spec.Options, OnSettle: spec.OnSettle, OnFinish: spec.OnFinish},
	})
}
