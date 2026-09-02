package core

import (
	"context"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// SpecDefaults are the spec-level defaults the engine reads.
type SpecDefaults struct {
	Model           string
	Subagents       *ports.SubagentsConfig
	TokenBudget     int
	ProviderOptions ports.ProviderOptions
}

// AgentArgs are the user's generation args: applied first, platform keys
// last (§3.1).
type AgentArgs struct {
	System   string
	Tools    []ports.Tool
	Options  []goai.Option
	OnChunk  func(provider.StreamChunk)
	OnFinish func(ports.RunFinishInfo)
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
// is active.
func (h *Handle) Stop(ctx context.Context, threadID string, state ports.AgentRunState) (ports.StopResult, error) {
	return Stop(ctx, h.scope(state, ""), threadID)
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
		Spec: SpecDefaults{Model: spec.Model, Subagents: spec.Subagents, TokenBudget: spec.TokenBudget, ProviderOptions: spec.ProviderOptions},
		Args: AgentArgs{System: spec.System, Tools: spec.Tools, Options: spec.Options, OnChunk: spec.OnChunk, OnFinish: spec.OnFinish},
	})
}

// NewGenerateTextAgent registers a generate-text handle.
func NewGenerateTextAgent(scope ScopeFn, spec ports.GenerateTextAgentSpec) *Handle {
	return newHandle(scope, &RegisteredAgent{
		Name: spec.Name, Kind: ports.KindGenerateText,
		Spec: SpecDefaults{Model: spec.Model, Subagents: spec.Subagents, TokenBudget: spec.TokenBudget, ProviderOptions: spec.ProviderOptions},
		Args: AgentArgs{System: spec.System, Tools: spec.Tools, Options: spec.Options, OnFinish: spec.OnFinish},
	})
}
