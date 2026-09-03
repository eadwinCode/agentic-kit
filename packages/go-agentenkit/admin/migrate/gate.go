package migrate

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Timeout is how long the background migration may take before it gives up.
// Generous: it is one pass of DDL, and a database slow enough to exceed this
// has a bigger problem than the schema.
const Timeout = 2 * time.Minute

// Gate is the "schema is ready" signal (§2.9). SetupAgentCore returns as soon
// as the store is OPEN, and the schema is brought up to date behind it, so a
// service starts at the same speed whether or not it has migrating to do. Any
// admin call made in that window waits here rather than meeting a table that
// does not exist yet.
type Gate struct {
	done chan struct{}
	err  error
}

// NewGate returns a gate nobody has opened yet.
func NewGate() *Gate { return &Gate{done: make(chan struct{})} }

// Open releases everything waiting, with the migration's outcome. Called once.
func (g *Gate) Open(err error) {
	g.err = err
	close(g.done)
}

// Wait blocks until the schema is ready, and reports what happened. It
// returns the caller's own error if they give up first.
func (g *Gate) Wait(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case <-g.done:
		return g.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start applies the migrations on a goroutine and returns the gate that
// speaks for them.
//
// The context is detached from the caller's: SetupAgentCore's own context is
// usually finished the moment it returns, and a migration cancelled half way
// through is exactly what nobody wants.
func Start(ctx context.Context, db *sql.DB, d Dialect, ms []Migration, log *slog.Logger) *Gate {
	gate := NewGate()
	go func() {
		bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), Timeout)
		defer cancel()
		err := Run(bg, db, d, ms)
		if err != nil && log != nil {
			// Loud, because everything downstream of this is silent: admin
			// writes are best effort, so a failed migration shows up as a
			// dashboard with nothing in it rather than as an error.
			log.Error("admin migrations failed", "dialect", d.Name, "err", err)
		}
		gate.Open(err)
	}()
	return gate
}

// Gated is an AdminStore that waits for the schema before it does anything.
//
// The wrapper lives here rather than in each store so that every admin store,
// including one added later, gets the same behaviour for free.
func Gated(inner ports.AdminStore, gate *Gate) ports.AdminStore {
	return gated{inner: inner, gate: gate}
}

type gated struct {
	inner ports.AdminStore
	gate  *Gate
}

func (g gated) Threads() ports.AdminThreadStore { return gatedThreads{g.inner.Threads(), g.gate} }
func (g gated) Runs() ports.RunStore            { return gatedRuns{g.inner.Runs(), g.gate} }
func (g gated) Steps() ports.StepStore          { return gatedSteps{g.inner.Steps(), g.gate} }
func (g gated) Close() error                    { return g.inner.Close() }

type gatedThreads struct {
	inner ports.AdminThreadStore
	gate  *Gate
}

func (t gatedThreads) Upsert(ctx context.Context, n ports.NewAdminThread) error {
	if err := t.gate.Wait(ctx); err != nil {
		return err
	}
	return t.inner.Upsert(ctx, n)
}
func (t gatedThreads) CountByState(ctx context.Context) (map[ports.ExecutionState]int, error) {
	if err := t.gate.Wait(ctx); err != nil {
		return nil, err
	}
	return t.inner.CountByState(ctx)
}
func (t gatedThreads) List(ctx context.Context, f ports.AdminThreadFilter) ([]ports.AdminThread, error) {
	if err := t.gate.Wait(ctx); err != nil {
		return nil, err
	}
	return t.inner.List(ctx, f)
}

type gatedRuns struct {
	inner ports.RunStore
	gate  *Gate
}

func (r gatedRuns) Start(ctx context.Context, n ports.NewRunRecord) (*ports.RunRecord, error) {
	if err := r.gate.Wait(ctx); err != nil {
		return nil, err
	}
	return r.inner.Start(ctx, n)
}
func (r gatedRuns) Patch(ctx context.Context, runID string, p ports.RunPatch) error {
	if err := r.gate.Wait(ctx); err != nil {
		return err
	}
	return r.inner.Patch(ctx, runID, p)
}
func (r gatedRuns) Get(ctx context.Context, runID string) (*ports.RunRecord, error) {
	if err := r.gate.Wait(ctx); err != nil {
		return nil, err
	}
	return r.inner.Get(ctx, runID)
}
func (r gatedRuns) ListByThread(ctx context.Context, threadID string) ([]ports.RunRecord, error) {
	if err := r.gate.Wait(ctx); err != nil {
		return nil, err
	}
	return r.inner.ListByThread(ctx, threadID)
}
func (r gatedRuns) List(ctx context.Context, f ports.RunFilter) ([]ports.RunRecord, error) {
	if err := r.gate.Wait(ctx); err != nil {
		return nil, err
	}
	return r.inner.List(ctx, f)
}
func (r gatedRuns) CountByState(ctx context.Context) (map[ports.ExecutionState]int, error) {
	if err := r.gate.Wait(ctx); err != nil {
		return nil, err
	}
	return r.inner.CountByState(ctx)
}

type gatedSteps struct {
	inner ports.StepStore
	gate  *Gate
}

func (s gatedSteps) Record(ctx context.Context, rec ports.NewStepRecord) error {
	if err := s.gate.Wait(ctx); err != nil {
		return err
	}
	return s.inner.Record(ctx, rec)
}
func (s gatedSteps) ListByRun(ctx context.Context, runID string) ([]ports.StepRecord, error) {
	if err := s.gate.Wait(ctx); err != nil {
		return nil, err
	}
	return s.inner.ListByRun(ctx, runID)
}
func (s gatedSteps) ListByThread(ctx context.Context, threadID string) ([]ports.StepRecord, error) {
	if err := s.gate.Wait(ctx); err != nil {
		return nil, err
	}
	return s.inner.ListByThread(ctx, threadID)
}
