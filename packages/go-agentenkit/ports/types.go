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
	StateIdle ExecutionState = "IDLE"
	// StateQueued is a run accepted and waiting for a worker (§2.8). It
	// becomes RUNNING the moment a worker picks the job up.
	StateQueued          ExecutionState = "QUEUED"
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
	Error string `json:"error,omitempty"`
	// EnqueuedAt is when the run was accepted and put on the queue (§2.8).
	// Nil on a nested run, which is never queued.
	EnqueuedAt *time.Time `json:"enqueuedAt,omitempty"`
	// StartedAt is when a worker first picked the run up. Until then it is
	// the enqueue time, so a listing sorted on it stays stable.
	StartedAt time.Time  `json:"startedAt"`
	EndedAt   *time.Time `json:"endedAt,omitempty"`
	// DurationMs is EndedAt - StartedAt, kept so a listing never recomputes
	// it. A parked run legitimately spans however long the human took (§2.5).
	DurationMs *int64 `json:"durationMs,omitempty"`
	// QueuedMs is the time between the latest enqueue and a worker starting
	// on it (§2.8): the first dispatch's wait, then a retry's or a resume's.
	QueuedMs *int64 `json:"queuedMs,omitempty"`
	// SettledAt is when the spec's OnSettle ran for this run (§5.6). A run
	// settles exactly once: a worker that ends it, or a stop that ends it
	// while no worker holds it. Unset until then.
	SettledAt *time.Time `json:"settledAt,omitempty"`
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
	// CostBudgetMicros and MaxSteps are the caps the run was dispatched with
	// (§4, §2.1), kept so a run re-dispatched from its record keeps them.
	CostBudgetMicros int64 `json:"costBudgetMicros,omitempty"`
	MaxSteps         int   `json:"maxSteps,omitempty"`
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
	// State the record opens in. Empty means RUNNING, which is what a nested
	// run is from its first step; a dispatched run opens QUEUED.
	State ExecutionState
	// EnqueuedAt is set on a dispatched run: when it was put on the queue.
	EnqueuedAt *time.Time
	// CostBudgetMicros and MaxSteps are the dispatched caps (§4, §2.1).
	CostBudgetMicros int64
	MaxSteps         int
}

// RunPatch is a partial update of a run record. A nil field is left alone.
type RunPatch struct {
	State      *ExecutionState
	StopReason *string
	Error      *string
	// StartedAt moves the start to the moment a worker picked the run up.
	StartedAt         *time.Time
	EndedAt           *time.Time
	DurationMs        *int64
	QueuedMs          *int64
	SettledAt         *time.Time
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

// UsageTotals is cumulative token AND money attribution across every run on
// a thread (§4). Tokens are what the provider reported; the money is what
// the Pricer put on each row as it was stored.
type UsageTotals struct {
	// InputTokens are fresh (uncached) prompt tokens.
	InputTokens int `json:"inputTokens"`
	// CachedInputTokens are prompt tokens served from the provider's cache (§2.6).
	CachedInputTokens int `json:"cachedInputTokens"`
	OutputTokens      int `json:"outputTokens"`
	// TotalTokens is input + cached + output.
	TotalTokens int `json:"totalTokens"`
	// CostMicros is the summed cost, in millionths of one Currency unit.
	// 1_000_000 is one dollar when Currency is "USD".
	CostMicros int64 `json:"costMicros"`
	// Currency is the unit CostMicros is in, empty when nothing was priced.
	// One deployment should price in ONE currency: these are summed, not
	// converted.
	Currency string `json:"currency,omitempty"`
	// Unpriced is how many calls had no cost, because no pricer answered for
	// them. Above zero, CostMicros is a floor and not the whole bill.
	Unpriced int `json:"unpriced"`
	// Lines is the same spend grouped by agent and model: one line per pair,
	// which is the shape a bill wants. Summing the lines gives the totals
	// above.
	Lines []UsageLine `json:"lines,omitempty"`
}

// UsageLine is one agent's spend on one model, summed over its calls (§4).
// This is the bill line a credit system charges for.
type UsageLine struct {
	// AgentID is empty for the main run, the nested run's id otherwise.
	AgentID   string `json:"agentId,omitempty"`
	AgentName string `json:"agentName,omitempty"`
	// Model is the registry key; ModelID the wire id the provider reported.
	Model                 string `json:"model,omitempty"`
	ModelID               string `json:"modelId,omitempty"`
	InputTokens           int    `json:"inputTokens"`
	CacheReadInputTokens  int    `json:"cacheReadInputTokens"`
	CacheWriteInputTokens int    `json:"cacheWriteInputTokens"`
	OutputTokens          int    `json:"outputTokens"`
	ReasoningTokens       int    `json:"reasoningTokens"`
	// Calls is how many model calls this line covers.
	Calls int `json:"calls"`
	// Estimated is how many of those had estimated tokens, because they were
	// cut off before the provider reported real ones.
	Estimated  int   `json:"estimated"`
	CostMicros int64 `json:"costMicros"`
}

// UsageAggregator sums usage rows into the shape Total must return: the four
// counters, the money, and one Line per agent and model.
//
// A storage adapter that can group in the database should do that instead.
// This is for the ones that cannot, and for anyone writing their own adapter:
// feed every matching row through Add and Totals gives back exactly what the
// port promises, lines in first-seen order.
type UsageAggregator struct {
	total UsageTotals
	index map[usageLineKey]int
}

type usageLineKey struct{ agentID, agentName, model, modelID string }

// Add books one call.
func (a *UsageAggregator) Add(u NewUsage) {
	if a.index == nil {
		a.index = map[usageLineKey]int{}
	}
	a.total.Add(u.Totals())

	key := usageLineKey{u.AgentID, u.AgentName, u.Model, u.ModelID}
	i, ok := a.index[key]
	if !ok {
		i = len(a.total.Lines)
		a.index[key] = i
		a.total.Lines = append(a.total.Lines, UsageLine{
			AgentID: u.AgentID, AgentName: u.AgentName, Model: u.Model, ModelID: u.ModelID,
		})
	}
	line := &a.total.Lines[i]
	line.InputTokens += u.InputTokens
	line.CacheReadInputTokens += u.CacheReadInputTokens
	line.CacheWriteInputTokens += u.CacheWriteInputTokens
	line.OutputTokens += u.OutputTokens
	line.ReasoningTokens += u.ReasoningTokens
	line.Calls++
	if u.Estimated {
		line.Estimated++
	}
	// Money is summed in ONE currency: the first one seen. A row priced in
	// another currency cannot be added to it, so it counts as unpriced and
	// the total stays a floor rather than a mix of units. Totals() above
	// already added the row's cost; take it back out here.
	if u.Cost != nil {
		if a.total.Currency == "" {
			a.total.Currency = u.Cost.Currency
		}
		if u.Cost.Currency == a.total.Currency {
			line.CostMicros += u.Cost.Micros
		} else {
			a.total.CostMicros -= u.Cost.Micros
			a.total.Unpriced++
		}
	}
}

// Totals is everything added so far.
func (a *UsageAggregator) Totals() UsageTotals { return a.total }

// UsageLineMerger rebuilds UsageTotals from grouped rows that a SQL adapter
// read GROUP BY agent, model AND currency. Grouping by currency is what keeps
// a sum honest; merging here is what keeps one agent's spend on one model a
// single line. Money is summed in the first currency seen; a group priced in
// another currency counts as unpriced instead of being added to it.
type UsageLineMerger struct {
	total UsageTotals
	index map[usageLineKey]int
}

// Add books one grouped row: its line, the currency its cost is in, its
// summed total tokens and how many of its calls were unpriced.
func (m *UsageLineMerger) Add(l UsageLine, currency string, totalTokens, unpriced int) {
	if m.index == nil {
		m.index = map[usageLineKey]int{}
	}
	m.total.InputTokens += l.InputTokens
	m.total.CachedInputTokens += l.CacheReadInputTokens
	m.total.OutputTokens += l.OutputTokens
	m.total.TotalTokens += totalTokens
	m.total.Unpriced += unpriced

	priced := l.Calls - unpriced
	if currency != "" && m.total.Currency == "" {
		m.total.Currency = currency
	}
	if currency != "" && currency != m.total.Currency {
		// Another unit: cannot be added to the total, so its calls are
		// reported as unpriced and the total stays a floor.
		m.total.Unpriced += priced
		l.CostMicros = 0
	}
	m.total.CostMicros += l.CostMicros

	key := usageLineKey{l.AgentID, l.AgentName, l.Model, l.ModelID}
	i, ok := m.index[key]
	if !ok {
		m.index[key] = len(m.total.Lines)
		m.total.Lines = append(m.total.Lines, l)
		return
	}
	line := &m.total.Lines[i]
	line.InputTokens += l.InputTokens
	line.CacheReadInputTokens += l.CacheReadInputTokens
	line.CacheWriteInputTokens += l.CacheWriteInputTokens
	line.OutputTokens += l.OutputTokens
	line.ReasoningTokens += l.ReasoningTokens
	line.Calls += l.Calls
	line.Estimated += l.Estimated
	line.CostMicros += l.CostMicros
}

// Totals is everything added so far.
func (m *UsageLineMerger) Totals() UsageTotals { return m.total }

// UsageFilter narrows a usage read (§4).
type UsageFilter struct {
	// RunID limits the read to one dispatched run, nested runs included.
	// Empty reads the whole thread.
	RunID string
}

// Add sums another set of totals into this one. The currency is taken from
// whichever side has one; mixing two is a pricing misconfiguration, not
// something to convert here.
func (u *UsageTotals) Add(o UsageTotals) {
	u.InputTokens += o.InputTokens
	u.CachedInputTokens += o.CachedInputTokens
	u.OutputTokens += o.OutputTokens
	u.TotalTokens += o.TotalTokens
	u.CostMicros += o.CostMicros
	u.Unpriced += o.Unpriced
	if u.Currency == "" {
		u.Currency = o.Currency
	}
}

// ContextUsage is how full the next run's prompt would be (§2.6). Token
// counts are the same rough estimate the engine itself compacts on.
type ContextUsage struct {
	UsedTokens      int `json:"usedTokens"`
	BudgetTokens    int `json:"budgetTokens"`
	CompactAtTokens int `json:"compactAtTokens"`
	Messages        int `json:"messages"`
}

// UsageOutcome is how the model call that produced a usage row ended.
type UsageOutcome string

const (
	// UsageFinished: the call ran to its finish and the provider reported
	// the counters itself.
	UsageFinished UsageOutcome = "finished"
	// UsageAborted: a user stop cut the call mid-stream, so no finish ever
	// arrived and the tokens are estimated.
	UsageAborted UsageOutcome = "aborted"
	// UsageErrored: the provider failed mid-call. Whatever it had already
	// streamed was still billed, so the row is kept.
	UsageErrored UsageOutcome = "error"
)

// UsageKind says which part of the platform made the call.
type UsageKind string

const (
	// KindStep is a step of an agent loop, main run or nested (§2.1, §2.7).
	KindStep UsageKind = "step"
	// KindCompaction is the summary the platform writes to keep the prompt
	// inside the model's window (§2.6). Nobody asked for it, so it is worth
	// being able to see what it costs on its own.
	KindCompaction UsageKind = "compaction"
)

// Cost is the money one model call cost.
type Cost struct {
	// Micros is millionths of one Currency unit: 1_000_000 is one dollar
	// when Currency is "USD". Integers, because money summed as float64
	// drifts.
	Micros int64 `json:"micros"`
	// Currency is an ISO-4217 code, "USD" for the shipped table pricer.
	Currency string `json:"currency"`
	// Source is where the number came from: "receipt" when the provider
	// computed it, "table" from a price list, "estimate" for anything a
	// pricer worked out itself. It rides along so a bill can be audited.
	Source string `json:"source"`
}

// NewUsage is ONE model call, recorded by the engine after every call: a
// step of the main run, a step of a nested run, a compaction pass, streamed
// or not, finished or cut short.
//
// It carries everything a Pricer needs to put a price on the call, so
// pricing never has to reach back into the run to find out what happened.
type NewUsage struct {
	// RunID is the DISPATCHED run this call belongs to (§2.9). A nested run's
	// calls carry their parent's run id, so one run's bill is one query.
	RunID string `json:"runId,omitempty"`
	// AgentID is whose stream made the call (§2.7): empty is the main run,
	// otherwise the nested run's id.
	AgentID string `json:"agentId,omitempty"`
	// AgentName is the registered handle for the main run, the delegation's
	// name for a nested one. What a bill line should say.
	AgentName string    `json:"agentName,omitempty"`
	Kind      UsageKind `json:"kind"`
	// Step is the 1-based iteration inside its loop; 0 for a compaction.
	Step int `json:"step"`

	// Model is the registry key the call was made with, e.g.
	// "claude-sonnet-4@high" — what a price list is usually keyed by.
	Model string `json:"model,omitempty"`
	// ModelID is the wire id the provider reported back, which can differ
	// from the key that asked for it (an alias, a dated snapshot).
	ModelID string `json:"modelId,omitempty"`

	InputTokens           int `json:"inputTokens"`
	CacheReadInputTokens  int `json:"cacheReadInputTokens"`
	CacheWriteInputTokens int `json:"cacheWriteInputTokens"`
	OutputTokens          int `json:"outputTokens"`
	// ReasoningTokens are thinking tokens. Most providers already count
	// these inside OutputTokens, so price them at zero unless yours bills
	// them separately (see the pricing package).
	ReasoningTokens int `json:"reasoningTokens"`

	Outcome UsageOutcome `json:"outcome"`
	// Estimated is true when no finish chunk arrived and the counters are the
	// platform's own estimate over what it does know: the prompt it sent and
	// the text that did stream, measured the same way compaction measures
	// context fill.
	Estimated bool `json:"estimated,omitempty"`

	// ProviderMetadata is whatever the provider attached to the finish: a
	// gateway receipt, a generation id, anything a Pricer wants to read.
	ProviderMetadata map[string]any `json:"providerMetadata,omitempty"`

	// Cost is filled by the Pricer before the row is stored. Nil means the
	// call went unpriced and any total over it is a floor.
	Cost *Cost `json:"cost,omitempty"`
}

// TotalTokens is input + cache reads + output: the counter the token budget
// is measured against, and the one UsageTotals carries.
//
// Cache WRITES and reasoning tokens are deliberately outside it. Cache
// writes are a separate line on the provider's bill, and reasoning tokens
// are usually already inside OutputTokens; adding either here would move a
// budget that callers have already tuned.
func (u NewUsage) TotalTokens() int {
	return u.InputTokens + u.CacheReadInputTokens + u.OutputTokens
}

// Totals is this one call as a UsageTotals, cost included.
func (u NewUsage) Totals() UsageTotals {
	t := UsageTotals{
		InputTokens: u.InputTokens, CachedInputTokens: u.CacheReadInputTokens,
		OutputTokens: u.OutputTokens, TotalTokens: u.TotalTokens(),
	}
	if u.Cost == nil {
		t.Unpriced = 1
		return t
	}
	t.CostMicros, t.Currency = u.Cost.Micros, u.Cost.Currency
	return t
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
	// EnqueuedAt is epoch milliseconds at THIS enqueue (§2.9): the queue
	// wait a worker measures on pickup.
	EnqueuedAt int64 `json:"enqueuedAt,omitempty"`
	// DispatchedAt is epoch milliseconds of the run's first dispatch. A
	// retry, redrive or resume carries it on, so the run keeps its place in
	// line instead of going to the back. Zero means "now".
	DispatchedAt int64 `json:"dispatchedAt,omitempty"`
	// Kind says why the job exists: a fresh dispatch, a retry, a resume, a
	// park expiry. See JobKind.
	Kind JobKind `json:"kind,omitempty"`
	// PartitionKey is the caller's tenant, opaque to the platform. A queue
	// that can spread its claims across partitions serves one tenant's
	// backlog without starving the others. Empty means one shared lane.
	PartitionKey string `json:"partitionKey,omitempty"`
	// State is the run's state (§2.10), so a worker rehydrates exactly what
	// the caller attached.
	State       AgentRunState `json:"state,omitempty"`
	TokenBudget int           `json:"tokenBudget,omitempty"`
	// CostBudgetMicros is the money cap for this run (§4), carried so the
	// worker enforces the same cap the caller asked for.
	CostBudgetMicros int64           `json:"costBudgetMicros,omitempty"`
	ProviderOptions  ProviderOptions `json:"providerOptions,omitempty"`
	// MaxSteps is the run's own step cap, when the caller set one (§2.1).
	MaxSteps int `json:"maxSteps,omitempty"`
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
	Agent string `json:"agent"`
	Model string `json:"model"`
	RunID string `json:"runId,omitempty"`
	// DispatchedAt is epoch milliseconds of the run's first dispatch, so a
	// resume keeps the run's place in line.
	DispatchedAt     int64           `json:"dispatchedAt,omitempty"`
	TokenBudget      int             `json:"tokenBudget,omitempty"`
	CostBudgetMicros int64           `json:"costBudgetMicros,omitempty"`
	ProviderOptions  ProviderOptions `json:"providerOptions,omitempty"`
	State            AgentRunState   `json:"state,omitempty"`
	MaxSteps         int             `json:"maxSteps,omitempty"`
}

// ResolvedModel is a model identity after resolution: the real provider
// instance (created lazily per run) and the declared context window, which
// feeds the §2.6 compaction budget math.
type ResolvedModel struct {
	Instance      func() provider.LanguageModel
	ContextWindow int
	// ModelID is the wire id this key resolves to, e.g.
	// "claude-sonnet-4-20250514" for the key "claude-sonnet-4@high". It goes
	// onto every usage row, so a price list keyed by wire ids can match one
	// (§4). Empty means the key IS the id.
	ModelID string
}

// WireID is the model id to record for a registry key: what ResolveModel
// declared, or the key itself when it declared nothing.
func (m ResolvedModel) WireID(key string) string {
	if m.ModelID != "" {
		return m.ModelID
	}
	return key
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
	// Profiles are named specialists (§2.7). When set, spawnSubagent must
	// name one of them: the child takes the profile's persona, model, tools
	// and step cap instead of the shared defaults above. The model still
	// writes the brief; the profile says who reads it.
	Profiles map[string]SubagentProfile
}

// SubagentProfile is one named specialist a run may delegate to.
type SubagentProfile struct {
	// Description is shown to the model beside the name, so it can choose.
	Description string
	// System is the child's static persona; SystemFn wins when set.
	System   string
	SystemFn SystemFunc
	// PrepareStep edits the child's prompt per step; see PrepareStepFunc.
	PrepareStep PrepareStepFunc
	// Model is the registry key; empty falls back to the config's Model,
	// then the parent's.
	Model string
	// Tools are the child's own set, HITL-wrapped like the parent's. They
	// replace the shared Tools above.
	Tools []Tool
	// MaxSteps caps the child's round trips; zero keeps SubagentMaxSteps.
	MaxSteps int
}

// BillingStage says when a BillingCheck runs.
type BillingStage string

const (
	// BillingAtDispatch runs inside Run, before anything is written. A
	// refusal here means the run never exists.
	BillingAtDispatch BillingStage = "dispatch"
	// BillingAtPickup runs when a worker takes the job, however long it
	// waited: a first dispatch, a retry, or a resume after an approval. A
	// refusal here fails the run. The check may also lower the run's caps
	// through Budget.
	BillingAtPickup BillingStage = "pickup"
)

// RunBudget is the money and step cap a segment runs with.
type RunBudget struct {
	// CostBudgetMicros is the money cap (§4); zero is unbounded.
	CostBudgetMicros int64
	// MaxSteps is the step cap; zero keeps the config's.
	MaxSteps int
}

// BillingCheck is what BillingPreCheck receives: the thread about to run,
// the run's state (§2.10), and a way to publish on the thread (a credit
// warning, a reset date) before the refusal reaches the caller.
type BillingCheck struct {
	ThreadID string
	// RunID is set at pickup; empty at dispatch, where the run has no id yet.
	RunID string
	State AgentRunState
	// Stage is when the check runs. See BillingStage.
	Stage BillingStage
	// Budget is the cap the job carries. At pickup the check may lower
	// either field and the segment runs with the lowered cap; raising is
	// ignored. Nil at dispatch.
	Budget *RunBudget
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
	// RunRetryBackoff is the delay before the first retry of a failed run
	// (§2.8). Each further retry waits twice as long, up to
	// RunRetryBackoffMax, with a little jitter so a fleet does not retry in
	// step. Zero retries at once.
	RunRetryBackoff time.Duration
	// RunRetryBackoffMax caps the retry delay. Zero means no cap.
	RunRetryBackoffMax time.Duration
	// StepTimeout bounds one model round trip, tools included. A step past
	// it fails like any other failed step and the run takes the retry
	// policy. Zero is no bound.
	StepTimeout time.Duration
	// SegmentTimeout bounds one worker segment: from pickup to finish or
	// park. A segment past it settles the run FAILED with a reason the
	// client can show, instead of holding the worker slot. Zero is no bound.
	SegmentTimeout time.Duration
	// MaxQueueWait is the longest a job may sit in the queue and still run.
	// A job picked up later than this is settled FAILED with the reason,
	// instead of executing work nobody is waiting for. Zero is no bound.
	MaxQueueWait time.Duration
	// MaxQueueDepth refuses a new run, before anything is written, once this
	// many jobs are ready and waiting (Queue.Stats). The refusal names
	// RefusedQueueFull so a host can answer 503 with a Retry-After. Zero is
	// no bound. A queue adapter may enforce its own cap on top.
	MaxQueueDepth int
	// TokenBudget is the default per-run token budget (input + output). Zero
	// means unbounded apart from MaxSteps.
	TokenBudget int
	// CostBudgetMicros is the default per-run money cap (§4), in millionths
	// of the pricer's currency: 250_000 is roughly a quarter of a dollar
	// when the pricer works in USD. Zero means unbounded. Checked between
	// steps, exactly like TokenBudget, so the step that crossed the line is
	// always kept in full.
	CostBudgetMicros int64
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
	// RunLockLease is the lease for the per-thread run lock (§2.8, §3.4). The
	// worker renews it every third of the lease while its segment runs, so
	// it only has to outlast a renewal gap plus a crash: an expired lock
	// means a dead worker and nothing else.
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
		RunLockLease:               2 * time.Minute,
		RunRetryBackoff:            5 * time.Second,
		RunRetryBackoffMax:         2 * time.Minute,
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
	if config.RunRetryBackoff < 0 || config.RunRetryBackoffMax < 0 {
		return config, errors.New("invalid config: RunRetryBackoff and RunRetryBackoffMax must not be negative")
	}
	if config.StepTimeout < 0 || config.SegmentTimeout < 0 || config.MaxQueueWait < 0 || config.MaxQueueDepth < 0 {
		return config, errors.New("invalid config: StepTimeout, SegmentTimeout, MaxQueueWait and MaxQueueDepth must not be negative")
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
