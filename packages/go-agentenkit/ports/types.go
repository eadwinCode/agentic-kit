// Package ports holds the contract between the platform and the code around
// it: the four ports a caller implements (Storage, EventBus, Queue, Kv), the
// platform's own AdminStore, and every DTO that crosses those boundaries.
//
// In the TypeScript package these types are spread over core/types.ts,
// core/state.ts and ports/*.ts. Go cannot have an import cycle between
// core and ports, so the shared types live here, in the leaf package, and
// core imports them. The file names still follow the TypeScript ones.
package ports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"
)

// ExecutionState is the lifecycle of a thread. Durable truth lives in
// Storage.Threads; the kv copy (`agent:state:{threadId}`) is a hot cache the
// engine polls (§2.1, §3.4).
type ExecutionState string

const (
	StateIdle            ExecutionState = "IDLE"
	StateRunning         ExecutionState = "RUNNING"
	StateWaitingForInput ExecutionState = "WAITING_FOR_INPUT"
	StateCancelled       ExecutionState = "CANCELLED"
	StateCompleted       ExecutionState = "COMPLETED"
	StateFailed          ExecutionState = "FAILED"
)

// MessageRole is who produced a message.
type MessageRole string

const (
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleSystem    MessageRole = "system"
	RoleTool      MessageRole = "tool"
)

// AgentKind is the generation flavor of a handle / spawned subagent (§4).
type AgentKind string

const (
	KindStreamText   AgentKind = "stream-text"
	KindGenerateText AgentKind = "generate-text"
)

// ThreadDTO is one conversation thread.
type ThreadDTO struct {
	ID        string         `json:"id"`
	State     ExecutionState `json:"state"`
	Model     string         `json:"model"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

// MessageDTO is one persisted turn.
//
// Content is the message body as JSON, in the same shapes the TypeScript
// package stores (see core/messages.go): a JSON string for plain text, an
// array of parts for assistant and tool turns, or a CONTEXT_SUMMARY envelope
// (§2.6). Keeping the shape identical means a Go worker and a TypeScript
// worker can share one database.
type MessageDTO struct {
	ID       string `json:"id"`
	ThreadID string `json:"threadId"`
	// AgentID is the producing agent; empty = the main agent (§2.7).
	AgentID   string          `json:"agentId"`
	Role      MessageRole     `json:"role"`
	Content   json.RawMessage `json:"content"`
	CreatedAt time.Time       `json:"createdAt"`
}

// MarshalJSON writes an empty AgentID as null, which is what the TypeScript
// package (and the React client) use for the main agent.
func (m MessageDTO) MarshalJSON() ([]byte, error) {
	type alias MessageDTO
	out := struct {
		alias
		AgentID *string `json:"agentId"`
	}{alias: alias(m)}
	if m.AgentID != "" {
		out.AgentID = &m.AgentID
	}
	return json.Marshal(out)
}

// UnmarshalJSON accepts both null and a string for agentId.
func (m *MessageDTO) UnmarshalJSON(data []byte) error {
	type alias MessageDTO
	var in struct {
		alias
		AgentID *string `json:"agentId"`
	}
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	*m = MessageDTO(in.alias)
	if in.AgentID != nil {
		m.AgentID = *in.AgentID
	}
	return nil
}

// NewMessage is a turn about to be appended.
type NewMessage struct {
	AgentID string
	Role    MessageRole
	Content json.RawMessage
}

// AgentEvent is an append-only event log entry: the replay source for SSE
// (re)connects and the durable record of INPUT_REQUIRED (HITL) requests. Seq
// is assigned by the engine via Kv.Incr before append (§3.4). Seq 0 marks a
// bus-only notice that is never persisted.
type AgentEvent struct {
	ThreadID  string          `json:"threadId"`
	Seq       int64           `json:"seq"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"createdAt"`
}

// PayloadInto decodes the event payload into v.
func (e AgentEvent) PayloadInto(v any) error {
	if len(e.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(e.Payload, v)
}

// RunRecord is the durable record of ONE agent run (§2.9): when it started,
// how it ended, what it cost. Keyed by the run id the platform already mints
// to fence stale workers (§2.1). A thread accumulates many of these over its
// life; Thread.State only ever describes the latest one.
type RunRecord struct {
	// ID is the run id (§2.1) for a dispatched run; the nested run's id
	// otherwise, which is also the AgentID its messages and events carry.
	ID       string `json:"id"`
	ThreadID string `json:"threadId"`
	// ParentRunID is the run that spawned this one; empty for a dispatched run.
	ParentRunID string `json:"parentRunId,omitempty"`
	// Depth is 0 for the main agent, 1+ for nested.
	Depth int `json:"depth"`
	// Agent is the registered handle for a dispatched run; the delegation's
	// name for a nested one.
	Agent string         `json:"agent"`
	Model string         `json:"model"`
	State ExecutionState `json:"state"`
	// StopReason is 'completed' | 'cancelled' | 'token_budget' | 'max_steps',
	// set at finalize.
	StopReason string `json:"stopReason,omitempty"`
	// Error is why it failed, when it did.
	Error     string     `json:"error,omitempty"`
	StartedAt time.Time  `json:"startedAt"`
	EndedAt   *time.Time `json:"endedAt,omitempty"`
	// DurationMs is EndedAt - StartedAt, kept so a listing never recomputes
	// it. A parked run legitimately spans however long the human took (§2.5).
	DurationMs *int64 `json:"durationMs,omitempty"`
	// QueuedMs is the time between enqueue and a worker starting work (§2.8).
	QueuedMs *int64 `json:"queuedMs,omitempty"`
	// Steps is loop iterations completed, summed across every segment.
	Steps             int `json:"steps"`
	InputTokens       int `json:"inputTokens"`
	CachedInputTokens int `json:"cachedInputTokens"`
	OutputTokens      int `json:"outputTokens"`
	TotalTokens       int `json:"totalTokens"`
	// Attempts is the §2.8 redrive attempts consumed.
	Attempts int `json:"attempts"`
	// Prompt, TokenBudget and RunState are what the run was dispatched with
	// (§2.9). Present only when RecordPayloads is on. A nested run's prompt
	// is its brief.
	Prompt      string        `json:"prompt,omitempty"`
	TokenBudget *int          `json:"tokenBudget,omitempty"`
	RunState    AgentRunState `json:"runState,omitempty"`
	// ProviderOptions the run was dispatched with (§3.1), merged across
	// config, spec and input. Present only when RecordPayloads is on.
	ProviderOptions ProviderOptions `json:"providerOptions,omitempty"`
	// Result is a nested run's capped result, handed back to its parent (§2.7).
	Result json.RawMessage `json:"result,omitempty"`
}

// NewRunRecord opens a run record.
type NewRunRecord struct {
	ID              string
	ThreadID        string
	Agent           string
	Model           string
	Prompt          string
	TokenBudget     *int
	RunState        AgentRunState
	ProviderOptions ProviderOptions
	// Depth defaults to 0, a dispatched run.
	Depth       int
	ParentRunID string
}

// RunPatch is a partial update of a run record. A nil field is left alone.
type RunPatch struct {
	State             *ExecutionState
	StopReason        *string
	Error             *string
	EndedAt           *time.Time
	DurationMs        *int64
	QueuedMs          *int64
	Steps             *int
	InputTokens       *int
	CachedInputTokens *int
	OutputTokens      *int
	TotalTokens       *int
	Attempts          *int
	Result            json.RawMessage
}

// Ptr is a small helper for building a RunPatch.
func Ptr[T any](v T) *T { return &v }

// UsageTotals is cumulative token attribution across every run on a thread
// (§4). The platform records the counters; USD/credit pricing is a
// downstream concern computed over them.
type UsageTotals struct {
	// InputTokens are fresh (uncached) prompt tokens.
	InputTokens int `json:"inputTokens"`
	// CachedInputTokens are prompt tokens served from the provider's cache (§2.6).
	CachedInputTokens int `json:"cachedInputTokens"`
	OutputTokens      int `json:"outputTokens"`
	// TotalTokens is input + cached + output.
	TotalTokens int `json:"totalTokens"`
}

// Add sums another set of totals into this one.
func (u *UsageTotals) Add(o UsageTotals) {
	u.InputTokens += o.InputTokens
	u.CachedInputTokens += o.CachedInputTokens
	u.OutputTokens += o.OutputTokens
	u.TotalTokens += o.TotalTokens
}

// ContextUsage is how full the next run's prompt would be (§2.6). Token
// counts are the same rough estimate the engine itself compacts on.
type ContextUsage struct {
	UsedTokens      int `json:"usedTokens"`
	BudgetTokens    int `json:"budgetTokens"`
	CompactAtTokens int `json:"compactAtTokens"`
	Messages        int `json:"messages"`
}

// NewUsage is one recorded segment of spend.
type NewUsage struct {
	AgentID           string
	InputTokens       int
	CachedInputTokens int
	OutputTokens      int
	TotalTokens       int
}

// ProviderOptions are provider-specific options passed through to the
// provider by goai. Namespaced per provider; the platform never inspects them.
type ProviderOptions map[string]any

// MergeProviderOptions is a shallow per-provider merge: override wins over base.
func MergeProviderOptions(base, override ProviderOptions) ProviderOptions {
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}
	out := make(ProviderOptions, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

// RunJob is the dispatch ticket for the queue (§2.8). At-least-once;
// consumers must be idempotent. Agent resolves via AgentCore.GetAgent; when
// missing, the default handle executes. The JSON field names match the
// TypeScript package so a queue can carry jobs for either.
type RunJob struct {
	ThreadID string `json:"threadId"`
	Model    string `json:"model"`
	Agent    string `json:"agent,omitempty"`
	// RunID identifies THIS dispatch (§2.1). A thread has one live run at a
	// time and `agent:run:{threadId}` holds its id; a job whose id no longer
	// matches has been replaced and must not execute.
	RunID string `json:"runId,omitempty"`
	// EnqueuedAt is epoch milliseconds at enqueue (§2.9).
	EnqueuedAt int64 `json:"enqueuedAt,omitempty"`
	// State is the run's state (§2.10), so a worker rehydrates exactly what
	// the caller attached.
	State           AgentRunState   `json:"state,omitempty"`
	TokenBudget     int             `json:"tokenBudget,omitempty"`
	ProviderOptions ProviderOptions `json:"providerOptions,omitempty"`
}

// NestedDescriptor identifies a nested run well enough to re-enter its loop
// (§2.7). Persisted in the INPUT_REQUIRED payload.
type NestedDescriptor struct {
	AgentID string `json:"agentId"`
	Name    string `json:"name"`
	Model   string `json:"model"`
	Depth   int    `json:"depth"`
}

// ResumeInfo is everything needed to resume a parked HITL run segment (§2.5).
// Persisted inside the INPUT_REQUIRED event payload.
type ResumeInfo struct {
	Agent           string          `json:"agent"`
	Model           string          `json:"model"`
	RunID           string          `json:"runId,omitempty"`
	TokenBudget     int             `json:"tokenBudget,omitempty"`
	ProviderOptions ProviderOptions `json:"providerOptions,omitempty"`
	State           AgentRunState   `json:"state,omitempty"`
}

// ResolvedModel is a model identity after resolution: the real provider
// instance (created lazily per run) and the declared context window, which
// feeds the §2.6 compaction budget math.
type ResolvedModel struct {
	Instance      func() provider.LanguageModel
	ContextWindow int
}

// Tool is a goai tool plus the platform's own flags. RequiresConfirmation
// marks a destructive tool: the engine parks it behind an approval (§2.5)
// instead of executing it. Use WrapTool for a plain goai tool.
type Tool struct {
	goai.Tool
	RequiresConfirmation bool
}

// WrapTool lifts a plain goai tool into the platform's Tool.
func WrapTool(t goai.Tool) Tool { return Tool{Tool: t} }

// WrapTools lifts several goai tools.
func WrapTools(ts ...goai.Tool) []Tool {
	out := make([]Tool, 0, len(ts))
	for _, t := range ts {
		out = append(out, WrapTool(t))
	}
	return out
}

// SubagentsConfig is the opt-in subagent delegation config (§2.7): the
// platform constructs the run-scoped spawnSubagent tool itself.
type SubagentsConfig struct {
	// Kind is the generation flavor for spawned subagents. Default: stream-text.
	Kind AgentKind
	// Model is the registry key used when a delegation call omits one.
	Model string
	// Tools are merged into every spawned subagent's toolset (HITL-wrapped
	// identically to the parent's tools).
	Tools []Tool
}

// BillingCheck is what BillingPreCheck receives: the thread about to run,
// the run's state (§2.10), and a way to publish on the thread (a credit
// warning, a reset date) before the refusal reaches the caller.
type BillingCheck struct {
	ThreadID string
	State    AgentRunState
	// PublishEvent publishes a durable event on the thread; Notice for a
	// bus-only one.
	PublishEvent func(ctx context.Context, typ string, payload any, notice bool) (AgentEvent, error)
}

// AgentConfig tunes the platform. Build one with DefaultConfig and change
// what you need.
type AgentConfig struct {
	// HITLTTL is how long a parked HITL request stays answerable (§2.5).
	HITLTTL time.Duration
	// ReclaimGrace is the grace beyond the HITL TTL before orphan reclamation
	// may claim a thread (§2.5).
	ReclaimGrace time.Duration
	// MaxSteps is the round-trip ceiling per run (§2.1 safety cap).
	MaxSteps int
	// RunMaxAttempts is the queue redrive attempts before a run finalizes
	// FAILED (§2.8).
	RunMaxAttempts int
	// StopPoll is how often a running worker re-reads the stop signal (§2.1).
	StopPoll time.Duration
	// RunRedriveDelay is the delay before re-dispatching a job that found the
	// run lock still held by an OLDER run (§2.8).
	RunRedriveDelay time.Duration
	// TokenBudget is the default per-run token budget (input + output). Zero
	// means unbounded apart from MaxSteps.
	TokenBudget int
	// SubagentMaxDepth caps nesting (§2.7).
	SubagentMaxDepth int
	// SubagentMaxConcurrent caps concurrent subagents per run (§2.7).
	SubagentMaxConcurrent int
	// SubagentMaxSteps is the step ceiling per subagent run (§2.7).
	SubagentMaxSteps int
	// SubagentResultCapChars caps a subagent result handed to the parent.
	SubagentResultCapChars int
	// RecordPayloads records prompts, state, step text and tool payloads into
	// the operational store (§2.9). Turn it off when those carry anything
	// that should not sit in an operational database.
	RecordPayloads bool
	// PayloadCapChars caps each recorded value.
	PayloadCapChars int
	// ContextCeilingTokens is the universal context ceiling (§2.6).
	ContextCeilingTokens int
	// ContextOutputReserveTokens are reserved for the completion when compacting.
	ContextOutputReserveTokens int
	// CompactionTrigger: compact when the history estimate exceeds this share
	// of the budget (§2.6).
	CompactionTrigger float64
	// ContextTailShare is the share of the budget kept verbatim as the tail.
	ContextTailShare float64
	// CompactionModel is the registry key of the cheap model used to write
	// the context summary (§2.6).
	CompactionModel string
	// PromptCaching stamps cache breakpoints on the stable prompt prefix
	// (§2.6). Providers without prompt caching ignore the stamp.
	PromptCaching bool
	// NativeWindows are per-model native windows below the ceiling (§2.6).
	// A ContextWindow declared via ResolveModel wins over this table.
	NativeWindows map[string]int
	// RunLockLease is the lease for the per-thread run lock. Must exceed the
	// longest possible run segment (§2.8, §3.4).
	RunLockLease time.Duration
	// BillingPreCheck rejects a run before it starts (§4). Nil means no
	// check. The check can publish on the thread, so the refusal is visible
	// to every client: the platform also publishes RUN_REFUSED with the error.
	BillingPreCheck func(ctx context.Context, check BillingCheck) error
	// ProviderOptions are applied to EVERY run (§3.1). An agent spec
	// overrides this, and a run input overrides both, per provider namespace.
	ProviderOptions ProviderOptions
}

// DefaultConfig returns the defaults the TypeScript package ships with.
func DefaultConfig() AgentConfig {
	return AgentConfig{
		HITLTTL:                    15 * time.Minute,
		ReclaimGrace:               time.Minute,
		MaxSteps:                   25,
		RunMaxAttempts:             3,
		StopPoll:                   500 * time.Millisecond,
		RunRedriveDelay:            2 * time.Second,
		SubagentMaxDepth:           2,
		SubagentMaxConcurrent:      3,
		SubagentMaxSteps:           10,
		SubagentResultCapChars:     8_000,
		RecordPayloads:             true,
		PayloadCapChars:            2_000,
		ContextCeilingTokens:       265_000,
		ContextOutputReserveTokens: 16_000,
		CompactionTrigger:          0.8,
		ContextTailShare:           0.25,
		CompactionModel:            "gpt-4o-mini",
		PromptCaching:              true,
		RunLockLease:               30 * time.Minute,
	}
}

// ResolveConfig validates a config. A nil config means the defaults.
func ResolveConfig(partial *AgentConfig) (AgentConfig, error) {
	config := DefaultConfig()
	if partial != nil {
		config = *partial
	}
	if config.SubagentMaxSteps < 1 || config.SubagentMaxSteps > config.MaxSteps {
		// A subagent must never get a looser step ceiling than its parent run (§2.7)
		return config, fmt.Errorf(
			"invalid config: SubagentMaxSteps (%d) must be between 1 and MaxSteps (%d)",
			config.SubagentMaxSteps, config.MaxSteps)
	}
	if config.StopPoll < time.Millisecond {
		// The poll is the only thing that delivers a stop to a running worker (§2.1)
		return config, fmt.Errorf("invalid config: StopPoll (%s) must be at least 1ms", config.StopPoll)
	}
	if config.RunRedriveDelay < 0 {
		return config, fmt.Errorf("invalid config: RunRedriveDelay (%s) must not be negative", config.RunRedriveDelay)
	}
	if config.RunLockLease < time.Second {
		// The lease is the only thing that heals a crashed worker's lock (§3.4)
		return config, fmt.Errorf("invalid config: RunLockLease (%s) must be at least 1s", config.RunLockLease)
	}
	if config.CompactionModel == "" {
		return config, errors.New("invalid config: CompactionModel must be set")
	}
	return config, nil
}
