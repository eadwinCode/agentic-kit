package core

import (
	"context"
	"errors"
	"slices"
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
	// Duration is wall time from pickup to finish, over runs that ended. A
	// parked run legitimately includes however long the human took (§2.5).
	Duration *Percentiles `json:"duration"`
	// Queued is time spent waiting for a worker: the backlog signal (§2.8).
	// A run still waiting counts with the time it has waited so far, so the
	// number rises while a backlog grows, not after it clears.
	Queued *Percentiles `json:"queued"`
	// Waiting is how many runs in the window are still QUEUED.
	Waiting int `json:"waiting"`
	Failed  int `json:"failed"`
	// Sampled is true when the window held more runs than the percentiles
	// were worked out over. The counts and token sums are exact either way;
	// the percentiles are then over the newest runs only.
	Sampled bool `json:"sampled,omitempty"`
}

// AdminOverview is the top of an operational view.
type AdminOverview struct {
	Runs RunStats `json:"runs"`
	// Threads by state, from the platform's own view (§2.9).
	Threads map[ports.ExecutionState]int `json:"threads"`
	// RunsByState is every run ever, by state, unbounded by the stats window.
	RunsByState map[ports.ExecutionState]int `json:"runsByState"`
	// ActiveTotal is every run queued, running or waiting on a human, from
	// RunsByState: the number to show, where Active is a bounded sample.
	ActiveTotal int `json:"activeTotal"`
	// Active are runs still in flight, newest first, at most ActiveLimit.
	Active []ports.RunRecord `json:"active"`
	// ActiveLimit is the cap on Active; when ActiveTotal is above it the
	// list is a sample.
	ActiveLimit int `json:"activeLimit"`
	// Queue is what the queue says about itself: ready, delayed, in flight,
	// dead, the oldest wait. Nil when the queue adapter cannot count.
	Queue *ports.QueueStats `json:"queue,omitempty"`
}

// ThreadSummary is a thread with its runs rolled up (§2.9).
type ThreadSummary struct {
	ID          string               `json:"id"`
	State       ports.ExecutionState `json:"state"`
	Model       string               `json:"model"`
	FirstSeenAt time.Time            `json:"firstSeenAt"`
	UpdatedAt   time.Time            `json:"updatedAt"`
	// Runs on this thread, nested ones included.
	Runs  int `json:"runs"`
	Steps int `json:"steps"`
	// Tokens are summed from the run records. In a list that is tokens only,
	// with no money; GetThread reads the thread's usage rows instead, so its
	// Tokens carry the cost too (§4).
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
	// Events are the run's entries in the thread record: each segment's
	// start and end, its parks and their answers, a refusal, a budget stop.
	// The readable spine; its steps are in Steps.
	Events []ports.AgentEvent `json:"events"`
	// Usage is what this run spent, nested runs included (§4): tokens, money,
	// and a line per agent and model. Read from the usage rows, so it is the
	// same number a bill is built from.
	Usage ports.UsageTotals `json:"usage"`
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
	now := time.Now()
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
		switch {
		case r.State == ports.StateQueued && r.EnqueuedAt != nil:
			// Still waiting: a live sample, so the percentile moves while the
			// backlog grows rather than once it has cleared.
			queued = append(queued, now.Sub(*r.EnqueuedAt).Milliseconds())
			out.Waiting++
		case r.QueuedMs != nil:
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
	return statsOver(ctx, deps, ports.RunFilter{Since: r.Since, Until: r.Until}, limit)
}

// statsOver is Summarise over the newest sample runs, with the counts and
// token sums taken exact from the store, whatever the window holds.
func statsOver(ctx context.Context, deps ports.RuntimePorts, f ports.RunFilter, sample int) (RunStats, error) {
	f.Limit = sample
	runs, err := ListRuns(ctx, deps, f)
	if err != nil {
		return RunStats{}, err
	}
	out := Summarise(runs)
	totals, err := deps.Admin.Runs().Totals(ctx, f)
	if err != nil {
		return RunStats{}, err
	}
	out.Total, out.ByState, out.ByStopReason = totals.Runs, totals.ByState, totals.ByStopReason
	out.Failed, out.Waiting = totals.ByState[ports.StateFailed], totals.ByState[ports.StateQueued]
	out.Tokens = ports.UsageTotals{
		InputTokens: totals.InputTokens, CachedInputTokens: totals.CachedInputTokens,
		OutputTokens: totals.OutputTokens, TotalTokens: totals.TotalTokens,
	}
	out.Sampled = totals.Runs > len(runs)
	return out, nil
}

// activeLimit caps the Active sample in an overview.
const activeLimit = 50

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
	recent, err := statsOver(ctx, deps, ports.RunFilter{Since: since}, 1_000)
	if err != nil {
		return AdminOverview{}, err
	}
	active, err := deps.Admin.Runs().List(ctx, ports.RunFilter{
		State: []ports.ExecutionState{ports.StateQueued, ports.StateRunning, ports.StateWaitingForInput}, Limit: activeLimit,
	})
	if err != nil {
		return AdminOverview{}, err
	}
	out := AdminOverview{
		Runs: recent, Threads: threads, RunsByState: runsByState, Active: active, ActiveLimit: activeLimit,
		ActiveTotal: runsByState[ports.StateQueued] + runsByState[ports.StateRunning] + runsByState[ports.StateWaitingForInput],
	}
	// The queue's own numbers ride the same response every dashboard
	// already fetches. A queue that cannot count leaves the field empty.
	if stats, err := deps.Queue.Stats(ctx); err == nil {
		out.Queue = &stats
	} else if !errors.Is(err, ports.ErrUnsupported) {
		Logger(deps).Warn("queue stats not read for the overview", "err", err)
	}
	return out, nil
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
// (§2.9). One read of those threads' runs rather than a query per thread.
func ListThreads(ctx context.Context, deps ports.RuntimePorts, filter ports.AdminThreadFilter) ([]ThreadSummary, error) {
	if filter.Limit <= 0 {
		filter.Limit = defaultLimit
	}
	threads, err := deps.Admin.Threads().List(ctx, filter)
	if err != nil {
		return nil, err
	}
	// The runs of exactly the threads listed, not the latest few thousand
	// overall: a busy system would otherwise roll some threads up short.
	ids := make([]string, 0, len(threads))
	for _, t := range threads {
		ids = append(ids, t.ID)
	}
	var runs []ports.RunRecord
	if len(ids) > 0 {
		if runs, err = deps.Admin.Runs().List(ctx, ports.RunFilter{ThreadIDs: ids, Limit: 100_000}); err != nil {
			return nil, err
		}
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
	thread, err := deps.Admin.Threads().Get(ctx, threadID)
	if err != nil {
		return nil, err
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
	summary := rollUp(base, runs)
	// The money lives on the usage rows, not the run records: read it from
	// there, the same number a bill is built from. The view still renders
	// with the records' tokens when the read fails.
	if usage, err := deps.Storage.Usage.Total(ctx, threadID, ports.UsageFilter{}); err != nil {
		Logger(deps).Error("thread usage not read", "thread", threadID, "err", err)
	} else {
		summary.Tokens = usage
	}
	return &ThreadDetail{Thread: summary, Runs: runs, Steps: steps}, nil
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
	events, err := runEvents(ctx, deps, run)
	if err != nil {
		return nil, err
	}
	// Spend per run, from the one place money lives (§4). Best effort: a run
	// view must still render when the usage read fails.
	usage, err := deps.Storage.Usage.Total(ctx, run.ThreadID, ports.UsageFilter{RunID: runID})
	if err != nil {
		Logger(deps).Error("run usage not read", "run", runID, "err", err)
		usage = ports.UsageTotals{}
	}
	detail := &RunDetail{Run: *run, Steps: steps, Subagents: []ports.RunRecord{}, Events: []ports.AgentEvent{}, Usage: usage}
	for _, c := range siblings {
		if c.ParentRunID == runID {
			detail.Subagents = append(detail.Subagents, c) // its children: same table, by depth (§2.7)
		}
	}
	detail.Events = append(detail.Events, events...)
	return detail, nil
}

// runEvents is a run's entries in the thread record. A thread written
// before entries named their run has none by id; its log is read by the
// run's time window instead, without the chunks.
func runEvents(ctx context.Context, deps ports.RuntimePorts, run *ports.RunRecord) ([]ports.AgentEvent, error) {
	own, err := deps.Storage.Events.List(ctx, run.ThreadID, ports.ThreadEventFilter{RunID: run.ID})
	if err != nil || len(own) > 0 {
		return own, err
	}
	all, err := deps.Storage.Events.ListSince(ctx, run.ThreadID, -1)
	if err != nil {
		return nil, err
	}
	var out []ports.AgentEvent
	for _, e := range all {
		if slices.Contains(StreamOnlyTypes, e.Type) || e.CreatedAt.Before(run.StartedAt) {
			continue
		}
		if run.EndedAt != nil && e.CreatedAt.After(*run.EndedAt) {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}
