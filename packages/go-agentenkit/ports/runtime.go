package ports

import (
	"context"
	"log/slog"

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
	// Pricer puts a price on every model call before its usage row is
	// stored (§4). Nil stores every row unpriced.
	Pricer Pricer
	// Log is where the platform reports what it could not do without
	// failing the run, such as a usage row it failed to store.
	Log    *slog.Logger
	Config AgentConfig
}

// Pricer turns one model call into money (§4). The runtime calls it after
// every call, before the usage row is stored, so cost is part of the row
// rather than something a reader has to work out later.
//
// It runs on the run's own path: keep it fast and side-effect free. A price
// list lookup is the intended shape; a network call is not.
type Pricer interface {
	// Price returns nil, nil when it cannot price this call. The row is then
	// stored unpriced, and in a Chain the next pricer gets its turn. An
	// error is logged and treated the same way: an unpriceable call must
	// never fail a run.
	Price(ctx context.Context, usage NewUsage) (*Cost, error)
}

// PricerFunc adapts a plain function into a Pricer.
type PricerFunc func(ctx context.Context, usage NewUsage) (*Cost, error)

// Price implements Pricer.
func (f PricerFunc) Price(ctx context.Context, u NewUsage) (*Cost, error) { return f(ctx, u) }

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
	// Pricer prices every model call (§4). Nil records tokens only, and
	// every UsageTotals comes back with Unpriced above zero. See the
	// pricing package for the three that ship: a price table, a provider
	// receipt reader, and a chain of both.
	Pricer Pricer
	// Log is where the platform reports what it could not do without
	// failing the run. Nil uses slog.Default().
	Log *slog.Logger
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
	// CostBudgetMicros overrides the spec / config money cap (§4), in
	// millionths of the pricer's currency: 250_000 stops the run after
	// roughly $0.25. Needs a Pricer; without one nothing is ever priced and
	// the cap can never be reached.
	CostBudgetMicros int64
	// ProviderOptions merge over the spec default, per provider namespace.
	ProviderOptions ProviderOptions
	// RunID lets the caller name the run (§2.1). Empty mints one. A caller
	// that keys its own records (a workspace, a billing line) by run id can
	// open them before dispatch and know the worker will see the same id.
	// Reusing an id is refused, not silently re-run.
	RunID string
	// MaxSteps caps this run's round trips below the config's MaxSteps. Zero
	// keeps the config value; a larger value is clamped to it.
	MaxSteps int
	// Attachments are images the user sent with the prompt. They are stored
	// as image parts on the user message and reach the model natively.
	Attachments []Attachment
}

// Attachment is one image on a user turn: a URL the provider can fetch, or
// a data: URL.
type Attachment struct {
	URL       string `json:"url"`
	MediaType string `json:"mediaType,omitempty"`
}

// SystemFunc builds the system prompt for one step, with the run's state in
// hand (§3.1). Called once per step, so a prompt can read the project it is
// acting on. Keep the stable part first: a cached prefix is only a prefix
// while it does not move.
type SystemFunc func(ctx context.Context, threadID string, state AgentRunState) (string, error)

// PrepareStepFunc edits the prompt for one step, just before it is sent
// (§3.1): messages is the durable history the platform assembled (compacted,
// repaired, cache-stamped) and the return value is what the model sees. It
// is the place for context that must NOT be persisted — a screenshot the
// model should look at once, an editor snapshot — because anything appended
// here is gone on the next step unless it is appended again.
type PrepareStepFunc func(ctx context.Context, threadID string, state AgentRunState, messages []provider.Message) ([]provider.Message, error)

// SettleFunc runs after a run's last step and BEFORE its terminal
// STATE_CHANGE is written (§5.6): the place to commit what the run produced
// so every client sees it settled the moment the state flips. An error fails
// the run. A user stop reaches it with a cancelled ctx and Cancelled set;
// storage work in the hook should use context.WithoutCancel.
//
// It may run more than once for one run: a worker that dies inside it is
// redelivered. Keep it idempotent on RunID.
type SettleFunc func(ctx context.Context, info RunFinishInfo) error

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
	ThreadID   string
	RunID      string
	State      ExecutionState
	StopReason string
	TokensUsed int
	// Attribution is what THIS segment spent. A run that parked and resumed
	// finishes once, so this is the last segment, not the whole run.
	Attribution UsageTotals
	Steps       int
	// Cancelled is a user stop (§2.1).
	Cancelled bool
	// Error is why the run failed, when it did.
	Error string
	// Usage is the whole run's tokens AND money: every segment and every
	// nested run, read back with Total(threadID, UsageFilter{RunID: runID}).
	// Its Lines are the bill, one per agent and model, so a settle hook
	// charges in one pass without keeping its own tally (§4). Zero-valued
	// when the storage read failed; Unpriced above zero means some calls went
	// unpriced and CostMicros is a floor.
	Usage UsageTotals
	// UsageErr is set when the platform could not read the run's rows back.
	// Usage is then zero-valued, and a hook that bills from it should refuse
	// to settle rather than charge nothing: return the error from OnSettle
	// and the run fails instead of going free.
	UsageErr error
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
	// CostBudgetMicros is the default per-run money cap (§4), in millionths
	// of the pricer's currency. Zero is unbounded. Needs a Pricer: an
	// unpriced call spends no money and so can never exhaust it.
	CostBudgetMicros int64
	// ProviderOptions are this agent's defaults; the run input wins.
	ProviderOptions ProviderOptions
	// System is the static persona.
	System string
	// SystemFn builds the persona per step, with the run's state (§3.1). It
	// wins over System when set.
	SystemFn SystemFunc
	// PrepareStep edits the prompt per step, for ephemeral context. See
	// PrepareStepFunc.
	PrepareStep PrepareStepFunc
	Tools       []Tool
	// Options are extra goai options (temperature, hooks, retries, ...).
	// Platform-owned options are applied after these and win.
	Options []goai.Option
	// OnChunk fires per stream chunk, after the platform persisted it.
	OnChunk func(chunk provider.StreamChunk)
	// OnSettle runs after the last step and before the terminal state is
	// written; an error fails the run. See SettleFunc.
	OnSettle SettleFunc
	// OnFinish fires once, after the platform finalized the run.
	OnFinish func(info RunFinishInfo)
}

// GenerateTextAgentSpec describes a generate-text agent (§3.1).
type GenerateTextAgentSpec struct {
	Name             string
	Model            string
	Subagents        *SubagentsConfig
	TokenBudget      int
	CostBudgetMicros int64
	ProviderOptions  ProviderOptions
	System           string
	SystemFn         SystemFunc
	PrepareStep      PrepareStepFunc
	Tools            []Tool
	Options          []goai.Option
	OnSettle         SettleFunc
	OnFinish         func(info RunFinishInfo)
}
