package core

import (
	"context"
	"sort"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Percentiles summarise a set of durations.
type Percentiles struct {
	P50 int64 `json:"p50"`
	P95 int64 `json:"p95"`
	Max int64 `json:"max"`
}

// RunStats summarise a window of runs (§2.9).
type RunStats struct {
	Total        int                          `json:"total"`
	ByState      map[ports.ExecutionState]int `json:"byState"`
	ByStopReason map[string]int               `json:"byStopReason"`
	Tokens       ports.UsageTotals            `json:"tokens"`
	// Duration is wall time from enqueue to finish, over runs that ended. A
	// parked run legitimately includes however long the human took (§2.5).
	Duration *Percentiles `json:"duration"`
	// Queued is time spent waiting for a worker: the backlog signal (§2.8).
	Queued *Percentiles `json:"queued"`
	Failed int          `json:"failed"`
}

// AdminOverview is the top of an operational view.
type AdminOverview struct {
	Runs RunStats `json:"runs"`
	// Threads by state, from the platform's own view (§2.9).
	Threads map[ports.ExecutionState]int `json:"threads"`
	// RunsByState is every run ever, by state, unbounded by the stats window.
	RunsByState map[ports.ExecutionState]int `json:"runsByState"`
	// Active are runs still in flight, newest first.
	Active []ports.RunRecord `json:"active"`
}

// ThreadSummary is a thread with its runs rolled up (§2.9).
type ThreadSummary struct {
	ID          string               `json:"id"`
	State       ports.ExecutionState `json:"state"`
	Model       string               `json:"model"`
	FirstSeenAt time.Time            `json:"firstSeenAt"`
	UpdatedAt   time.Time            `json:"updatedAt"`
	// Runs on this thread, nested ones included.
	Runs   int               `json:"runs"`
	Steps  int               `json:"steps"`
	Tokens ports.UsageTotals `json:"tokens"`
	// DurationMs is summed run durations. Not wall time: nested runs overlap
	// their parent.
	DurationMs int64 `json:"durationMs"`
	// Prompt is what started it: the first dispatched run's prompt.
	Prompt string `json:"prompt,omitempty"`
	// StartedWith is the parameters that started it, as recorded on first
	// sight (§2.9); falls back to the earliest dispatched run in the window.
	StartedWith *ports.ThreadStart `json:"startedWith,omitempty"`
}

// ThreadDetail is a thread opened up: its runs, and every step across them.
type ThreadDetail struct {
	Thread ThreadSummary      `json:"thread"`
	Runs   []ports.RunRecord  `json:"runs"`
	Steps  []ports.StepRecord `json:"steps"`
}

// RunDetail is everything about one run, assembled for a timeline view.
type RunDetail struct {
	Run   ports.RunRecord    `json:"run"`
	Steps []ports.StepRecord `json:"steps"`
	// Subagents are the nested runs spawned beneath it (§2.7).
	Subagents []ports.RunRecord `json:"subagents"`
	// Events are the run's events with CHUNKs stripped: the readable spine.
	Events []ports.AgentEvent `json:"events"`
}

func percentiles(values []int64) *Percentiles {
	if len(values) == 0 {
		return nil
	}
	s := append([]int64(nil), values...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(q float64) int64 { return s[min(len(s)-1, int(q*float64(len(s))))] }
	return &Percentiles{P50: at(0.5), P95: at(0.95), Max: s[len(s)-1]}
}

// Summarise rolls a set of runs into stats.
func Summarise(runs []ports.RunRecord) RunStats {
	out := RunStats{
		ByState: map[ports.ExecutionState]int{}, ByStopReason: map[string]int{}, Total: len(runs),
	}
	var durations, queued []int64
	for _, r := range runs {
		out.ByState[r.State]++
		if r.StopReason != "" {
			out.ByStopReason[r.StopReason]++
		}
		out.Tokens.Add(ports.UsageTotals{
			InputTokens: r.InputTokens, CachedInputTokens: r.CachedInputTokens,
			OutputTokens: r.OutputTokens, TotalTokens: r.TotalTokens,
		})
		if r.DurationMs != nil {
			durations = append(durations, *r.DurationMs)
		}
		if r.QueuedMs != nil {
			queued = append(queued, *r.QueuedMs)
		}
		if r.State == ports.StateFailed {
			out.Failed++
		}
	}
	out.Duration = percentiles(durations)
	out.Queued = percentiles(queued)
	return out
}

// defaultLimit bounds a dashboard read so "show me everything" can never
// become a table scan.
const defaultLimit = 200

// ListRuns lists runs with a bounded default limit.
func ListRuns(ctx context.Context, deps ports.RuntimePorts, filter ports.RunFilter) ([]ports.RunRecord, error) {
	if filter.Limit <= 0 {
		filter.Limit = defaultLimit
	}
	return deps.Admin.Runs().List(ctx, filter)
}

// StatsRange bounds a stats query.
type StatsRange struct {
	Since *time.Time
	Until *time.Time
	Limit int
}

// RunStatsFor computes stats over a window. Percentiles are computed here
// rather than pushed into the store, so a store only ever writes filters it
// can express in one indexed query.
func RunStatsFor(ctx context.Context, deps ports.RuntimePorts, r StatsRange) (RunStats, error) {
	limit := r.Limit
	if limit <= 0 {
		limit = 1_000
	}
	runs, err := ListRuns(ctx, deps, ports.RunFilter{Since: r.Since, Until: r.Until, Limit: limit})
	if err != nil {
		return RunStats{}, err
	}
	return Summarise(runs), nil
}

// Overview assembles the top of an operational view.
func Overview(ctx context.Context, deps ports.RuntimePorts, since *time.Time) (AdminOverview, error) {
	threads, err := deps.Admin.Threads().CountByState(ctx)
	if err != nil {
		return AdminOverview{}, err
	}
	runsByState, err := deps.Admin.Runs().CountByState(ctx)
	if err != nil {
		return AdminOverview{}, err
	}
	recent, err := ListRuns(ctx, deps, ports.RunFilter{Since: since, Limit: 1_000})
	if err != nil {
		return AdminOverview{}, err
	}
	active, err := deps.Admin.Runs().List(ctx, ports.RunFilter{
		State: []ports.ExecutionState{ports.StateRunning, ports.StateWaitingForInput}, Limit: 50,
	})
	if err != nil {
		return AdminOverview{}, err
	}
	return AdminOverview{Runs: Summarise(recent), Threads: threads, RunsByState: runsByState, Active: active}, nil
}

// ListSteps is a run's steps, in order (§2.9).
func ListSteps(ctx context.Context, deps ports.RuntimePorts, runID string) ([]ports.StepRecord, error) {
	return deps.Admin.Steps().ListByRun(ctx, runID)
}

func rollUp(t ports.AdminThread, runs []ports.RunRecord) ThreadSummary {
	out := ThreadSummary{ID: t.ID, State: t.State, Model: t.Model, FirstSeenAt: t.FirstSeenAt, UpdatedAt: t.UpdatedAt, Runs: len(runs)}
	var root *ports.RunRecord
	for i := range runs {
		r := &runs[i]
		out.Tokens.Add(ports.UsageTotals{
			InputTokens: r.InputTokens, CachedInputTokens: r.CachedInputTokens,
			OutputTokens: r.OutputTokens, TotalTokens: r.TotalTokens,
		})
		out.Steps += r.Steps
		if r.DurationMs != nil {
			out.DurationMs += *r.DurationMs
		}
		// The dispatched run is the one a person started; a nested run's
		// prompt is a brief the model wrote.
		if r.Depth == 0 && (root == nil || r.StartedAt.Before(root.StartedAt)) {
			root = r
		}
	}
	out.StartedWith = t.StartedWith
	if out.StartedWith == nil && root != nil {
		out.StartedWith = &ports.ThreadStart{
			RunID: root.ID, Agent: root.Agent, Model: root.Model, At: root.StartedAt,
			Prompt: root.Prompt, TokenBudget: root.TokenBudget, State: root.RunState, ProviderOptions: root.ProviderOptions,
		}
	}
	if out.StartedWith != nil {
		out.Prompt = out.StartedWith.Prompt
	}
	return out
}

// ListThreads lists threads with their runs rolled up, newest activity first
// (§2.9). One pass over the window's runs rather than a query per thread.
func ListThreads(ctx context.Context, deps ports.RuntimePorts, filter ports.AdminThreadFilter) ([]ThreadSummary, error) {
	if filter.Limit <= 0 {
		filter.Limit = defaultLimit
	}
	threads, err := deps.Admin.Threads().List(ctx, filter)
	if err != nil {
		return nil, err
	}
	runs, err := deps.Admin.Runs().List(ctx, ports.RunFilter{Since: filter.Since, Limit: 5_000})
	if err != nil {
		return nil, err
	}
	byThread := map[string][]ports.RunRecord{}
	for _, r := range runs {
		byThread[r.ThreadID] = append(byThread[r.ThreadID], r)
	}
	out := make([]ThreadSummary, 0, len(threads))
	for _, t := range threads {
		out = append(out, rollUp(t, byThread[t.ID]))
	}
	return out, nil
}

// GetThread opens one thread up: its runs and every step across them (§2.9).
// Nil when nothing was ever recorded for it.
func GetThread(ctx context.Context, deps ports.RuntimePorts, threadID string) (*ThreadDetail, error) {
	runs, err := deps.Admin.Runs().ListByThread(ctx, threadID)
	if err != nil {
		return nil, err
	}
	steps, err := deps.Admin.Steps().ListByThread(ctx, threadID)
	if err != nil {
		return nil, err
	}
	rows, err := deps.Admin.Threads().List(ctx, ports.AdminThreadFilter{Limit: 5_000})
	if err != nil {
		return nil, err
	}
	var thread *ports.AdminThread
	for i := range rows {
		if rows[i].ID == threadID {
			thread = &rows[i]
			break
		}
	}
	if thread == nil && len(runs) == 0 {
		return nil, nil
	}
	base := ports.AdminThread{ID: threadID, State: ports.StateIdle, Model: "unknown", FirstSeenAt: time.Now(), UpdatedAt: time.Now()}
	if thread != nil {
		base = *thread
	} else if len(runs) > 0 {
		base.State, base.Model = runs[0].State, runs[0].Model
		base.FirstSeenAt, base.UpdatedAt = runs[len(runs)-1].StartedAt, runs[0].StartedAt
	}
	return &ThreadDetail{Thread: rollUp(base, runs), Runs: runs, Steps: steps}, nil
}

// GetRun assembles one run for a timeline view. Nil when unknown.
func GetRun(ctx context.Context, deps ports.RuntimePorts, runID string) (*RunDetail, error) {
	run, err := deps.Admin.Runs().Get(ctx, runID)
	if err != nil || run == nil {
		return nil, err
	}
	steps, err := ListSteps(ctx, deps, runID)
	if err != nil {
		return nil, err
	}
	siblings, err := deps.Admin.Runs().ListByThread(ctx, run.ThreadID)
	if err != nil {
		return nil, err
	}
	events, err := deps.Storage.Events.ListSince(ctx, run.ThreadID, -1)
	if err != nil {
		return nil, err
	}
	detail := &RunDetail{Run: *run, Steps: steps, Subagents: []ports.RunRecord{}, Events: []ports.AgentEvent{}}
	for _, c := range siblings {
		if c.ParentRunID == runID {
			detail.Subagents = append(detail.Subagents, c) // its children: same table, by depth (§2.7)
		}
	}
	from := run.StartedAt
	for _, e := range events {
		if e.Type == "CHUNK" || e.Type == "SUBAGENT_CHUNK" {
			continue // the token firehose; a timeline wants the spine
		}
		if e.CreatedAt.Before(from) {
			continue
		}
		if run.EndedAt != nil && e.CreatedAt.After(*run.EndedAt) {
			continue
		}
		detail.Events = append(detail.Events, e)
	}
	return detail, nil
}
