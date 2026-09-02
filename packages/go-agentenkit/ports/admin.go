package ports

import (
	"context"
	"encoding/json"
	"time"
)

// AdminStore is operational history: run records and step timings (§2.9).
//
// NOT a port a caller implements. The platform owns this data and stores it
// itself: SQLite in development, Postgres in production. Callers only ever
// read it back, through the runtime's Admin API.
//
// It carries no StorageContext. A run's state belongs to the caller's own
// data (§2.10); operational history is the platform's.
type AdminStore interface {
	Threads() AdminThreadStore
	Runs() RunStore
	Steps() StepStore
	// Close releases any handles. Development stores hold an open file.
	Close() error
}

// AdminThreadStore is the platform's OWN view of threads (§2.9): a copy of
// the few fields an operational view needs, never the caller's thread table.
type AdminThreadStore interface {
	// Upsert records a thread at its current state. Called on every transition.
	Upsert(ctx context.Context, t NewAdminThread) error
	CountByState(ctx context.Context) (map[ExecutionState]int, error)
	List(ctx context.Context, f AdminThreadFilter) ([]AdminThread, error)
}

// RunStore holds run records.
type RunStore interface {
	Start(ctx context.Context, r NewRunRecord) (*RunRecord, error)
	Patch(ctx context.Context, runID string, p RunPatch) error
	// Get returns nil, nil when the run is unknown.
	Get(ctx context.Context, runID string) (*RunRecord, error)
	// ListByThread returns every run on a thread, newest first, nested runs
	// included.
	ListByThread(ctx context.Context, threadID string) ([]RunRecord, error)
	List(ctx context.Context, f RunFilter) ([]RunRecord, error)
	CountByState(ctx context.Context) (map[ExecutionState]int, error)
}

// StepStore holds one row per completed loop iteration.
type StepStore interface {
	Record(ctx context.Context, s NewStepRecord) error
	// ListByRun returns a run's steps in order.
	ListByRun(ctx context.Context, runID string) ([]StepRecord, error)
	// ListByThread returns every step on a thread, oldest first.
	ListByThread(ctx context.Context, threadID string) ([]StepRecord, error)
}

// StepToolCall is one tool a step ran, with capped arguments and result.
type StepToolCall struct {
	ToolName string          `json:"toolName"`
	Args     json.RawMessage `json:"args"`
	Result   json.RawMessage `json:"result"`
}

// StepRecord is one completed loop iteration (§2.9).
type StepRecord struct {
	RunID string `json:"runId"`
	// ThreadID is denormalised so a thread's whole timeline is one query.
	ThreadID string `json:"threadId"`
	// AgentID is which stream ran it; empty is the main agent (§2.7).
	AgentID           string `json:"agentId"`
	Index             int    `json:"index"`
	DurationMs        int64  `json:"durationMs"`
	FinishReason      string `json:"finishReason"`
	InputTokens       int    `json:"inputTokens"`
	CachedInputTokens int    `json:"cachedInputTokens"`
	OutputTokens      int    `json:"outputTokens"`
	TotalTokens       int    `json:"totalTokens"`
	// Tools the step executed, by name.
	Tools []string `json:"tools"`
	// Text is what the step said, capped. Only with RecordPayloads.
	Text string `json:"text,omitempty"`
	// ToolCalls are the tools it ran, with arguments and results, each capped.
	// Note: tool arguments and results are operational data now. If yours
	// carry anything you would not want there, set RecordPayloads false.
	ToolCalls []StepToolCall `json:"toolCalls,omitempty"`
	At        time.Time      `json:"at"`
}

// NewStepRecord is a step about to be recorded. A zero At means now.
type NewStepRecord = StepRecord

// ThreadStart is what started a thread (§2.9): the first dispatched run's
// parameters, recorded once and never overwritten, so a listing can say who
// asked for what without opening the thread. Prompt, State and
// ProviderOptions are present only when RecordPayloads is on.
type ThreadStart struct {
	RunID           string          `json:"runId"`
	Agent           string          `json:"agent"`
	Model           string          `json:"model"`
	At              time.Time       `json:"at"`
	Prompt          string          `json:"prompt,omitempty"`
	TokenBudget     *int            `json:"tokenBudget,omitempty"`
	State           AgentRunState   `json:"state,omitempty"`
	ProviderOptions ProviderOptions `json:"providerOptions,omitempty"`
}

// AdminThread is the platform's view of one thread.
type AdminThread struct {
	ID          string         `json:"id"`
	State       ExecutionState `json:"state"`
	Model       string         `json:"model"`
	FirstSeenAt time.Time      `json:"firstSeenAt"`
	UpdatedAt   time.Time      `json:"updatedAt"`
	// StartedWith is the parameters that started it; nil for a thread seen
	// before this was recorded.
	StartedWith *ThreadStart `json:"startedWith,omitempty"`
}

// NewAdminThread is a thread transition to record. StartedWith is written on
// first sight only: a later upsert never replaces what started the thread.
type NewAdminThread struct {
	ID          string
	State       ExecutionState
	Model       string
	StartedWith *ThreadStart
}

// AdminThreadFilter narrows an admin thread listing.
type AdminThreadFilter struct {
	State []ExecutionState
	Since *time.Time
	// Limit defaults to 100 in the shipped stores.
	Limit int
}

// RunFilter narrows a run listing.
type RunFilter struct {
	// State: any of these; empty means all.
	State    []ExecutionState
	Agent    string
	ThreadID string
	Since    *time.Time
	Until    *time.Time
	// Limit: newest first. Implementations cap this; core passes a bounded value.
	Limit int
}
