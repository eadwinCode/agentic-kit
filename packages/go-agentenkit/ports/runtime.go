package ports

import (
	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"
)

// RuntimePorts is the ports bundle: everything in core receives this and
// nothing else.
//
// Storage is the caller's implementation with THIS run's context already
// bound (§2.10). Admin is the platform's own operational store (§2.9).
type RuntimePorts struct {
	Storage BoundStorage
	Admin   AdminStore
	Bus     EventBus
	Queue   Queue
	Kv      Kv
	// ResolveModel is user-provided model resolution (§3.3): models can live
	// in any shape on the consumer side; the platform only sees ResolvedModel.
	ResolveModel func(modelName string) (ResolvedModel, error)
	Config       AgentConfig
}

// RuntimeOptions is what SetupAgentCore takes.
type RuntimeOptions struct {
	Storage Storage
	// Admin is where operational history goes (§2.9). Nil picks one from the
	// environment: Postgres when AGENTIC_KIT_ADMIN_DATABASE_URL is set, SQLite
	// on disk otherwise (both need a database/sql driver registered).
	Admin AdminStore
	Bus   EventBus
	Queue Queue
	Kv    Kv
	// ResolveModel turns a registry key into a provider instance and a
	// context window.
	ResolveModel func(modelName string) (ResolvedModel, error)
	// Config is nil for the defaults.
	Config *AgentConfig
}

// RunInput starts a run.
type RunInput struct {
	// ThreadID is empty to create a fresh thread first (§3.2).
	ThreadID string
	Prompt   string
	// State is carried through this whole run (§2.10): every storage call,
	// every tool, every nested run. Persisted on the dispatch.
	State AgentRunState
	// EditMessageID: edit + resend (§5.1). Replace this user message with
	// Prompt and drop every message after it, then answer again.
	EditMessageID string
	// Model overrides the spec default; resolved via ResolveModel.
	Model string
	// TokenBudget overrides the spec / config default (§2.1 safety cap).
	TokenBudget int
	// ProviderOptions merge over the spec default, per provider namespace.
	ProviderOptions ProviderOptions
}

// RunResult is what Run answers. Accepted false carries a reason in Error.
type RunResult struct {
	Accepted bool           `json:"accepted"`
	ThreadID string         `json:"threadId"`
	RunID    string         `json:"runId,omitempty"`
	State    ExecutionState `json:"state,omitempty"`
	Error    string         `json:"error,omitempty"`
}

// StopResult is what Stop answers.
type StopResult struct {
	Accepted bool   `json:"accepted"`
	Error    string `json:"error,omitempty"`
}

// DeleteThreadResult is what DeleteThread answers.
type DeleteThreadResult struct {
	Accepted bool   `json:"accepted"`
	Error    string `json:"error,omitempty"`
}

// RespondInput answers a parked approval (§2.5).
type RespondInput struct {
	ThreadID   string
	ToolCallID string
	Approved   bool
	Payload    any
	// State is for the storage calls answering makes (§2.10). The RESUMED
	// run rebuilds its own state from the park's ticket.
	State AgentRunState
}

// RespondResult is what Respond answers.
type RespondResult struct {
	Delivered bool   `json:"delivered"`
	Error     string `json:"error,omitempty"`
}

// ThreadUsage is what a thread has spent and how full its context is.
type ThreadUsage struct {
	Tokens  UsageTotals  `json:"tokens"`
	Context ContextUsage `json:"context"`
	Model   string       `json:"model"`
}

// ThreadSnapshot is the durable state used to hydrate a client before it
// starts live event replay.
type ThreadSnapshot struct {
	Thread   ThreadDTO    `json:"thread"`
	Messages []MessageDTO `json:"messages"`
	// Runs are the runs on this thread (§2.7), so a reconnecting client
	// rebuilds its subagent panel.
	Runs []RunRecord `json:"runs"`
	// LastEventSeq is the cursor for starting live replay.
	LastEventSeq int64 `json:"lastEventSeq"`
	// ActiveEvents are only the unfinished run's events.
	ActiveEvents []AgentEvent `json:"activeEvents"`
}

// RunFinishInfo is handed to a spec's OnFinish after the platform finalized
// the run.
type RunFinishInfo struct {
	ThreadID    string
	RunID       string
	State       ExecutionState
	StopReason  string
	TokensUsed  int
	Attribution UsageTotals
	Steps       int
}

// StreamTextAgentSpec describes a stream-text agent (§3.1). The platform
// owns the model, messages, prompt, MaxSteps and stop handling; everything
// else about generation is yours.
type StreamTextAgentSpec struct {
	// Name is the unique handle key and the queue dispatch key (§5).
	Name string
	// Model is the registry key for this agent's model.
	Model string
	// Subagents opts in to delegation (§2.7). Nil is off; an empty config
	// takes the defaults.
	Subagents *SubagentsConfig
	// TokenBudget is the default per-run budget (input + output). Zero is
	// unbounded apart from MaxSteps.
	TokenBudget int
	// ProviderOptions are this agent's defaults; the run input wins.
	ProviderOptions ProviderOptions
	// System is the static persona. Per-run system prompts are not a thing.
	System string
	Tools  []Tool
	// Options are extra goai options (temperature, hooks, retries, ...).
	// Platform-owned options are applied after these and win.
	Options []goai.Option
	// OnChunk fires per stream chunk, after the platform persisted it.
	OnChunk func(chunk provider.StreamChunk)
	// OnFinish fires once, after the platform finalized the run.
	OnFinish func(info RunFinishInfo)
}

// GenerateTextAgentSpec describes a generate-text agent (§3.1).
type GenerateTextAgentSpec struct {
	Name            string
	Model           string
	Subagents       *SubagentsConfig
	TokenBudget     int
	ProviderOptions ProviderOptions
	System          string
	Tools           []Tool
	Options         []goai.Option
	OnFinish        func(info RunFinishInfo)
}
