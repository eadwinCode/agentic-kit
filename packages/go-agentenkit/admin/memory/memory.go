// Package memory holds operational history in memory: tests, and a template.
// Loses everything on restart, which is exactly what the SQLite and Postgres
// stores exist to fix.
package memory

import (
	"context"
	"encoding/json"
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
}

// New makes an empty store.
func New() *Store {
	return &Store{runs: map[string]*ports.RunRecord{}, threads: map[string]*ports.AdminThread{}}
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
		return nil
	}
	t.s.threads[n.ID] = &ports.AdminThread{ID: n.ID, State: n.State, Model: n.Model, FirstSeenAt: now, UpdatedAt: now}
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
	rec := &ports.RunRecord{
		ID: n.ID, ThreadID: n.ThreadID, ParentRunID: n.ParentRunID, Depth: n.Depth,
		Agent: n.Agent, Model: n.Model, State: ports.StateRunning, StartedAt: time.Now(),
		Prompt: n.Prompt, TokenBudget: n.TokenBudget, RunState: n.RunState,
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
		if len(f.State) > 0 && !contains(f.State, rec.State) {
			continue
		}
		if f.Agent != "" && rec.Agent != f.Agent {
			continue
		}
		if f.ThreadID != "" && rec.ThreadID != f.ThreadID {
			continue
		}
		if f.Since != nil && rec.StartedAt.Before(*f.Since) {
			continue
		}
		if f.Until != nil && rec.StartedAt.After(*f.Until) {
			continue
		}
		rows = append(rows, *rec)
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].StartedAt.After(rows[j].StartedAt) })
	return limit(rows, f.Limit), nil
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
