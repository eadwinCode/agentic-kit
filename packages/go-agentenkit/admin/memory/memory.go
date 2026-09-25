// Package memory holds operational history in memory: tests, and a template.
// Loses everything on restart, which is exactly what the SQLite and Postgres
// stores exist to fix.
package memory

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Store is an in-memory AdminStore.
type Store struct {
	mu      sync.Mutex
	runs    map[string]*ports.RunRecord
	steps   []ports.StepRecord
	threads map[string]*ports.AdminThread
	// settleTokens is who holds each run's settle claim.
	settleTokens map[string]string
}

// New makes an empty store.
func New() *Store {
	return &Store{runs: map[string]*ports.RunRecord{}, threads: map[string]*ports.AdminThread{}, settleTokens: map[string]string{}}
}

func (s *Store) Threads() ports.AdminThreadStore { return threadStore{s} }
func (s *Store) Runs() ports.RunStore            { return runStore{s} }
func (s *Store) Steps() ports.StepStore          { return stepStore{s} }
func (s *Store) Close() error                    { return nil }

type threadStore struct{ s *Store }

func (t threadStore) Upsert(_ context.Context, n ports.NewAdminThread) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	now := time.Now()
	if prior, ok := t.s.threads[n.ID]; ok {
		prior.State, prior.Model, prior.UpdatedAt = n.State, n.Model, now
		if prior.StartedWith == nil {
			prior.StartedWith = n.StartedWith // first sight only
		}
		return nil
	}
	t.s.threads[n.ID] = &ports.AdminThread{ID: n.ID, State: n.State, Model: n.Model, FirstSeenAt: now, UpdatedAt: now, StartedWith: n.StartedWith}
	return nil
}

func (t threadStore) CountByState(context.Context) (map[ports.ExecutionState]int, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	out := map[ports.ExecutionState]int{}
	for _, th := range t.s.threads {
		out[th.State]++
	}
	return out, nil
}

func (t threadStore) List(_ context.Context, f ports.AdminThreadFilter) ([]ports.AdminThread, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	var rows []ports.AdminThread
	for _, th := range t.s.threads {
		if len(f.State) > 0 && !contains(f.State, th.State) {
			continue
		}
		if f.Since != nil && th.UpdatedAt.Before(*f.Since) {
			continue
		}
		rows = append(rows, *th)
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].UpdatedAt.After(rows[j].UpdatedAt) })
	return limit(rows, f.Limit), nil
}

func (t threadStore) Get(_ context.Context, threadID string) (*ports.AdminThread, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	th, ok := t.s.threads[threadID]
	if !ok {
		return nil, nil
	}
	copy := *th
	return &copy, nil
}

func (t threadStore) Delete(_ context.Context, threadID string) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	delete(t.s.threads, threadID)
	for id, r := range t.s.runs {
		if r.ThreadID == threadID {
			delete(t.s.runs, id)
			delete(t.s.settleTokens, id)
		}
	}
	kept := t.s.steps[:0]
	for _, st := range t.s.steps {
		if st.ThreadID != threadID {
			kept = append(kept, st)
		}
	}
	t.s.steps = kept
	return nil
}

func contains(states []ports.ExecutionState, s ports.ExecutionState) bool {
	for _, x := range states {
		if x == s {
			return true
		}
	}
	return false
}

func limit[T any](rows []T, n int) []T {
	if n <= 0 {
		n = 100
	}
	if len(rows) > n {
		return rows[:n]
	}
	return rows
}

type runStore struct{ s *Store }

func (r runStore) Start(_ context.Context, n ports.NewRunRecord) (*ports.RunRecord, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	state := n.State
	if state == "" {
		state = ports.StateRunning
	}
	rec := &ports.RunRecord{
		ID: n.ID, ThreadID: n.ThreadID, ParentRunID: n.ParentRunID, Depth: n.Depth,
		Agent: n.Agent, Model: n.Model, State: state, StartedAt: time.Now(),
		Prompt: n.Prompt, TokenBudget: n.TokenBudget, RunState: n.RunState, ProviderOptions: n.ProviderOptions,
		CostBudgetMicros: n.CostBudgetMicros, MaxSteps: n.MaxSteps,
	}
	if n.EnqueuedAt != nil {
		t := *n.EnqueuedAt
		rec.EnqueuedAt = &t
		rec.StartedAt = t
	}
	r.s.runs[rec.ID] = rec
	copy := *rec
	return &copy, nil
}

func (r runStore) Patch(_ context.Context, runID string, p ports.RunPatch) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	cur, ok := r.s.runs[runID]
	if !ok {
		return nil
	}
	applyPatch(cur, p)
	return nil
}

func applyPatch(cur *ports.RunRecord, p ports.RunPatch) {
	if p.State != nil {
		cur.State = *p.State
	}
	if p.StartedAt != nil {
		cur.StartedAt = *p.StartedAt
	}
	if p.StopReason != nil {
		cur.StopReason = *p.StopReason
	}
	if p.Error != nil {
		cur.Error = *p.Error
	}
	if p.EndedAt != nil {
		cur.EndedAt = p.EndedAt
	}
	if p.DurationMs != nil {
		cur.DurationMs = p.DurationMs
	}
	if p.QueuedMs != nil {
		cur.QueuedMs = p.QueuedMs
	}
	if p.SettledAt != nil {
		cur.SettledAt = p.SettledAt
	}
	if p.Steps != nil {
		cur.Steps = *p.Steps
	}
	if p.InputTokens != nil {
		cur.InputTokens = *p.InputTokens
	}
	if p.CachedInputTokens != nil {
		cur.CachedInputTokens = *p.CachedInputTokens
	}
	if p.OutputTokens != nil {
		cur.OutputTokens = *p.OutputTokens
	}
	if p.TotalTokens != nil {
		cur.TotalTokens = *p.TotalTokens
	}
	if p.Attempts != nil {
		cur.Attempts = *p.Attempts
	}
	if p.Result != nil {
		cur.Result = append(json.RawMessage(nil), p.Result...)
	}
}

func (r runStore) Get(_ context.Context, runID string) (*ports.RunRecord, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	cur, ok := r.s.runs[runID]
	if !ok {
		return nil, nil
	}
	copy := *cur
	return &copy, nil
}

func (r runStore) ListByThread(_ context.Context, threadID string) ([]ports.RunRecord, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var rows []ports.RunRecord
	for _, rec := range r.s.runs {
		if rec.ThreadID == threadID {
			rows = append(rows, *rec)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].StartedAt.After(rows[j].StartedAt) })
	return rows, nil
}

func (r runStore) List(_ context.Context, f ports.RunFilter) ([]ports.RunRecord, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var rows []ports.RunRecord
	for _, rec := range r.s.runs {
		if matches(rec, f) {
			rows = append(rows, *rec)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if !rows[i].StartedAt.Equal(rows[j].StartedAt) {
			return rows[i].StartedAt.After(rows[j].StartedAt)
		}
		return rows[i].ID > rows[j].ID
	})
	return limit(rows, f.Limit), nil
}

// matches is the RunFilter, Limit aside.
func matches(rec *ports.RunRecord, f ports.RunFilter) bool {
	switch {
	case len(f.State) > 0 && !contains(f.State, rec.State),
		f.Agent != "" && rec.Agent != f.Agent,
		f.ThreadID != "" && rec.ThreadID != f.ThreadID,
		len(f.ThreadIDs) > 0 && !slices.Contains(f.ThreadIDs, rec.ThreadID),
		f.Since != nil && rec.StartedAt.Before(*f.Since),
		f.Until != nil && rec.StartedAt.After(*f.Until),
		f.Unsettled && (rec.EndedAt == nil || rec.SettledAt != nil),
		f.Depth != nil && rec.Depth != *f.Depth:
		return false
	}
	if c := f.Before; c != nil && !(rec.StartedAt.Before(c.StartedAt) || rec.StartedAt.Equal(c.StartedAt) && rec.ID < c.ID) {
		return false
	}
	return true
}

func (r runStore) Increment(_ context.Context, runID string, d ports.RunDeltas) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if cur, ok := r.s.runs[runID]; ok {
		cur.Steps += d.Steps
		cur.InputTokens += d.InputTokens
		cur.CachedInputTokens += d.CachedInputTokens
		cur.OutputTokens += d.OutputTokens
		cur.TotalTokens += d.TotalTokens
	}
	return nil
}

func (r runStore) Totals(_ context.Context, f ports.RunFilter) (ports.RunTotals, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	f.Before = nil
	out := ports.RunTotals{ByState: map[ports.ExecutionState]int{}, ByStopReason: map[string]int{}}
	for _, rec := range r.s.runs {
		if !matches(rec, f) {
			continue
		}
		out.Runs++
		out.ByState[rec.State]++
		if rec.StopReason != "" {
			out.ByStopReason[rec.StopReason]++
		}
		out.Steps += rec.Steps
		out.InputTokens += rec.InputTokens
		out.CachedInputTokens += rec.CachedInputTokens
		out.OutputTokens += rec.OutputTokens
		out.TotalTokens += rec.TotalTokens
	}
	return out, nil
}

func (r runStore) ClaimSettle(_ context.Context, runID, token string, staleBefore time.Time) (bool, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	cur, ok := r.s.runs[runID]
	if !ok || cur.SettledAt != nil || cur.SettlingAt != nil && !cur.SettlingAt.Before(staleBefore) {
		return false, nil
	}
	now := time.Now()
	cur.SettlingAt = &now
	r.s.settleTokens[runID] = token
	return true, nil
}

func (r runStore) EndSettle(_ context.Context, runID, token string, settled bool) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	cur, ok := r.s.runs[runID]
	if !ok || r.s.settleTokens[runID] != token {
		return nil
	}
	delete(r.s.settleTokens, runID)
	cur.SettlingAt = nil
	if settled {
		now := time.Now()
		cur.SettledAt = &now
	}
	return nil
}

func (r runStore) CountByState(context.Context) (map[ports.ExecutionState]int, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	out := map[ports.ExecutionState]int{}
	for _, rec := range r.s.runs {
		out[rec.State]++
	}
	return out, nil
}

type stepStore struct{ s *Store }

func (st stepStore) Record(_ context.Context, n ports.NewStepRecord) error {
	st.s.mu.Lock()
	defer st.s.mu.Unlock()
	if n.At.IsZero() {
		n.At = time.Now()
	}
	st.s.steps = append(st.s.steps, n)
	return nil
}

func (st stepStore) ListByRun(_ context.Context, runID string) ([]ports.StepRecord, error) {
	st.s.mu.Lock()
	defer st.s.mu.Unlock()
	var rows []ports.StepRecord
	for _, s := range st.s.steps {
		if s.RunID == runID {
			rows = append(rows, s)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Index < rows[j].Index })
	return rows, nil
}

func (st stepStore) ListByThread(_ context.Context, threadID string) ([]ports.StepRecord, error) {
	st.s.mu.Lock()
	defer st.s.mu.Unlock()
	var rows []ports.StepRecord
	for _, s := range st.s.steps {
		if s.ThreadID == threadID {
			rows = append(rows, s)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].At.Before(rows[j].At) })
	return rows, nil
}
