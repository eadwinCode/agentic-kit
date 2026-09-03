// Package postgres holds operational history in Postgres (§2.9): the
// production store, reached through AGENTIC_KIT_ADMIN_DATABASE_URL. Point it
// at its own database or the one you already have; the agentic_ prefix
// keeps them apart. The schema is the TypeScript package's.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Open opens a Postgres connection with whichever database/sql driver the
// process has registered: "pgx" (github.com/jackc/pgx/v5/stdlib) or
// "postgres" (github.com/lib/pq).
func Open(url string) (*sql.DB, error) {
	drivers := sql.Drivers()
	for _, name := range []string{"pgx", "postgres"} {
		if slices.Contains(drivers, name) {
			return sql.Open(name, url)
		}
	}
	return nil, errors.New("no Postgres driver registered: import github.com/jackc/pgx/v5/stdlib or github.com/lib/pq, " +
		"or construct the store with your own *sql.DB")
}

// Schema is the DDL, split per statement. Safe to run twice.
var Schema = []string{
	`CREATE TABLE IF NOT EXISTS agentic_threads (
	   id TEXT PRIMARY KEY, state TEXT NOT NULL, model TEXT NOT NULL,
	   "firstSeenAt" TIMESTAMPTZ NOT NULL DEFAULT now(),
	   "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT now(), "startedWith" JSONB)`,
	`ALTER TABLE agentic_threads ADD COLUMN IF NOT EXISTS "startedWith" JSONB`,
	`CREATE INDEX IF NOT EXISTS agentic_threads_state ON agentic_threads(state, "updatedAt")`,
	`CREATE TABLE IF NOT EXISTS agentic_runs (
	   id TEXT PRIMARY KEY, "threadId" TEXT NOT NULL, "parentRunId" TEXT,
	   depth INT NOT NULL DEFAULT 0, agent TEXT NOT NULL, model TEXT NOT NULL,
	   state TEXT NOT NULL DEFAULT 'RUNNING', "stopReason" TEXT, error TEXT,
	   "startedAt" TIMESTAMPTZ NOT NULL DEFAULT now(), "endedAt" TIMESTAMPTZ,
	   "durationMs" INT, "queuedMs" INT,
	   attempts INT NOT NULL DEFAULT 0, steps INT NOT NULL DEFAULT 0,
	   "inputTokens" INT NOT NULL DEFAULT 0, "cachedInputTokens" INT NOT NULL DEFAULT 0,
	   "outputTokens" INT NOT NULL DEFAULT 0, "totalTokens" INT NOT NULL DEFAULT 0,
	   result JSONB, prompt TEXT, "tokenBudget" INT, "runState" JSONB, "providerOptions" JSONB)`,
	`ALTER TABLE agentic_runs ADD COLUMN IF NOT EXISTS "providerOptions" JSONB`,
	`ALTER TABLE agentic_runs ADD COLUMN IF NOT EXISTS prompt TEXT`,
	`ALTER TABLE agentic_runs ADD COLUMN IF NOT EXISTS "tokenBudget" INT`,
	`ALTER TABLE agentic_runs ADD COLUMN IF NOT EXISTS "runState" JSONB`,
	`CREATE INDEX IF NOT EXISTS agentic_runs_thread ON agentic_runs("threadId", "startedAt" DESC)`,
	`CREATE INDEX IF NOT EXISTS agentic_runs_state ON agentic_runs(state, "startedAt" DESC)`,
	`CREATE INDEX IF NOT EXISTS agentic_runs_parent ON agentic_runs("parentRunId")`,
	`CREATE TABLE IF NOT EXISTS agentic_steps (
	   "runId" TEXT NOT NULL, "threadId" TEXT, "agentId" TEXT, "index" INT NOT NULL,
	   "durationMs" INT NOT NULL, "finishReason" TEXT NOT NULL,
	   "inputTokens" INT NOT NULL DEFAULT 0, "cachedInputTokens" INT NOT NULL DEFAULT 0,
	   "outputTokens" INT NOT NULL DEFAULT 0, "totalTokens" INT NOT NULL DEFAULT 0,
	   "costMicros" BIGINT NOT NULL DEFAULT 0, currency TEXT,
	   tools JSONB, text TEXT, "toolCalls" JSONB,
	   at TIMESTAMPTZ NOT NULL DEFAULT now())`,
	`ALTER TABLE agentic_steps ADD COLUMN IF NOT EXISTS text TEXT`,
	`ALTER TABLE agentic_steps ADD COLUMN IF NOT EXISTS "threadId" TEXT`,
	`CREATE INDEX IF NOT EXISTS agentic_steps_thread ON agentic_steps("threadId", at)`,
	`ALTER TABLE agentic_steps ADD COLUMN IF NOT EXISTS "toolCalls" JSONB`,
	`ALTER TABLE agentic_steps ADD COLUMN IF NOT EXISTS "costMicros" BIGINT NOT NULL DEFAULT 0`,
	`ALTER TABLE agentic_steps ADD COLUMN IF NOT EXISTS currency TEXT`,
	`CREATE INDEX IF NOT EXISTS agentic_steps_run ON agentic_steps("runId", "index")`,
}

// Store is an AdminStore over Postgres.
type Store struct{ db *sql.DB }

// Connect creates the tables if they are missing, then returns the store.
// SetupAgentCore awaits its store precisely so this can fail at startup.
func Connect(ctx context.Context, db *sql.DB) (*Store, error) {
	for _, stmt := range Schema {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("postgres admin schema: %w", err)
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Threads() ports.AdminThreadStore { return threadStore{s.db} }
func (s *Store) Runs() ports.RunStore            { return runStore{s.db} }
func (s *Store) Steps() ports.StepStore          { return stepStore{s.db} }
func (s *Store) Close() error                    { return s.db.Close() }

func nullStr(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

// args builds a positional parameter list.
type args struct{ vals []any }

func (a *args) add(v any) string {
	a.vals = append(a.vals, v)
	return fmt.Sprintf("$%d", len(a.vals))
}

func (a *args) in(states []ports.ExecutionState) string {
	parts := make([]string, 0, len(states))
	for _, s := range states {
		parts = append(parts, a.add(string(s)))
	}
	return "(" + strings.Join(parts, ",") + ")"
}

type threadStore struct{ db *sql.DB }

func (t threadStore) Upsert(ctx context.Context, n ports.NewAdminThread) error {
	var started any
	if n.StartedWith != nil {
		b, _ := json.Marshal(n.StartedWith)
		started = string(b)
	}
	_, err := t.db.ExecContext(ctx,
		`INSERT INTO agentic_threads (id, state, model, "startedWith") VALUES ($1, $2, $3, $4)
		 ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, model = EXCLUDED.model, "updatedAt" = now(),
		   "startedWith" = COALESCE(agentic_threads."startedWith", EXCLUDED."startedWith")`,
		n.ID, string(n.State), n.Model, started)
	return err
}

func countByState(ctx context.Context, db *sql.DB, table string) (map[ports.ExecutionState]int, error) {
	rows, err := db.QueryContext(ctx, `SELECT state, COUNT(*)::int FROM `+table+` GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[ports.ExecutionState]int{}
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[ports.ExecutionState(state)] = n
	}
	return out, rows.Err()
}

func (t threadStore) CountByState(ctx context.Context) (map[ports.ExecutionState]int, error) {
	return countByState(ctx, t.db, "agentic_threads")
}

func (t threadStore) List(ctx context.Context, f ports.AdminThreadFilter) ([]ports.AdminThread, error) {
	var a args
	var where []string
	if len(f.State) > 0 {
		where = append(where, `state IN `+a.in(f.State))
	}
	if f.Since != nil {
		where = append(where, `"updatedAt" >= `+a.add(*f.Since))
	}
	q := `SELECT id, state, model, "firstSeenAt", "updatedAt", "startedWith" FROM agentic_threads`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	q += ` ORDER BY "updatedAt" DESC LIMIT ` + a.add(limit)
	rows, err := t.db.QueryContext(ctx, q, a.vals...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.AdminThread
	for rows.Next() {
		var th ports.AdminThread
		var started []byte
		if err := rows.Scan(&th.ID, &th.State, &th.Model, &th.FirstSeenAt, &th.UpdatedAt, &started); err != nil {
			return nil, err
		}
		if len(started) > 0 {
			var s ports.ThreadStart
			if json.Unmarshal(started, &s) == nil {
				th.StartedWith = &s
			}
		}
		out = append(out, th)
	}
	return out, rows.Err()
}

type runStore struct{ db *sql.DB }

const runCols = `id, "threadId", "parentRunId", depth, agent, model, state, "stopReason", error, "startedAt", "endedAt",
	"durationMs", "queuedMs", attempts, steps, "inputTokens", "cachedInputTokens", "outputTokens", "totalTokens",
	result, prompt, "tokenBudget", "runState", "providerOptions"`

func scanRun(row interface{ Scan(...any) error }) (*ports.RunRecord, error) {
	var r ports.RunRecord
	var parent, stop, errMsg, prompt sql.NullString
	var ended sql.NullTime
	var duration, queued, budget sql.NullInt64
	var result, runState, providerOptions []byte
	if err := row.Scan(&r.ID, &r.ThreadID, &parent, &r.Depth, &r.Agent, &r.Model, &r.State, &stop, &errMsg,
		&r.StartedAt, &ended, &duration, &queued, &r.Attempts, &r.Steps, &r.InputTokens, &r.CachedInputTokens,
		&r.OutputTokens, &r.TotalTokens, &result, &prompt, &budget, &runState, &providerOptions); err != nil {
		return nil, err
	}
	r.ParentRunID, r.StopReason, r.Error, r.Prompt = parent.String, stop.String, errMsg.String, prompt.String
	if ended.Valid {
		t := ended.Time
		r.EndedAt = &t
	}
	if duration.Valid {
		r.DurationMs = ports.Ptr(duration.Int64)
	}
	if queued.Valid {
		r.QueuedMs = ports.Ptr(queued.Int64)
	}
	if budget.Valid {
		r.TokenBudget = ports.Ptr(int(budget.Int64))
	}
	if len(result) > 0 {
		r.Result = json.RawMessage(result)
	}
	if len(runState) > 0 {
		_ = json.Unmarshal(runState, &r.RunState)
	}
	if len(providerOptions) > 0 {
		_ = json.Unmarshal(providerOptions, &r.ProviderOptions)
	}
	return &r, nil
}

func (r runStore) Start(ctx context.Context, n ports.NewRunRecord) (*ports.RunRecord, error) {
	var budget sql.NullInt64
	if n.TokenBudget != nil {
		budget = sql.NullInt64{Int64: int64(*n.TokenBudget), Valid: true}
	}
	var runState, providerOptions []byte
	if n.RunState != nil {
		runState, _ = json.Marshal(n.RunState)
	}
	if n.ProviderOptions != nil {
		providerOptions, _ = json.Marshal(n.ProviderOptions)
	}
	rec, err := scanRun(r.db.QueryRowContext(ctx,
		`INSERT INTO agentic_runs (id, "threadId", "parentRunId", depth, agent, model, state, prompt, "tokenBudget", "runState", "providerOptions")
		 VALUES ($1, $2, $3, $4, $5, $6, 'RUNNING', $7, $8, $9, $10) RETURNING `+runCols,
		n.ID, n.ThreadID, nullStr(n.ParentRunID), n.Depth, n.Agent, n.Model, nullStr(n.Prompt), budget, nullBytes(runState), nullBytes(providerOptions)))
	return rec, err
}

func nullBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

func (r runStore) Patch(ctx context.Context, runID string, p ports.RunPatch) error {
	var a args
	var sets []string
	set := func(col string, v any) { sets = append(sets, col+" = "+a.add(v)) }
	if p.State != nil {
		set("state", string(*p.State))
	}
	if p.StopReason != nil {
		set(`"stopReason"`, *p.StopReason)
	}
	if p.Error != nil {
		set("error", *p.Error)
	}
	if p.EndedAt != nil {
		set(`"endedAt"`, *p.EndedAt)
	}
	if p.DurationMs != nil {
		set(`"durationMs"`, *p.DurationMs)
	}
	if p.QueuedMs != nil {
		set(`"queuedMs"`, *p.QueuedMs)
	}
	if p.Steps != nil {
		set("steps", *p.Steps)
	}
	if p.InputTokens != nil {
		set(`"inputTokens"`, *p.InputTokens)
	}
	if p.CachedInputTokens != nil {
		set(`"cachedInputTokens"`, *p.CachedInputTokens)
	}
	if p.OutputTokens != nil {
		set(`"outputTokens"`, *p.OutputTokens)
	}
	if p.TotalTokens != nil {
		set(`"totalTokens"`, *p.TotalTokens)
	}
	if p.Attempts != nil {
		set("attempts", *p.Attempts)
	}
	if p.Result != nil {
		set("result", string(p.Result))
	}
	if len(sets) == 0 {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `UPDATE agentic_runs SET `+strings.Join(sets, ", ")+` WHERE id = `+a.add(runID), a.vals...)
	return err
}

func (r runStore) Get(ctx context.Context, runID string) (*ports.RunRecord, error) {
	rec, err := scanRun(r.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM agentic_runs WHERE id = $1`, runID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return rec, err
}

func (r runStore) query(ctx context.Context, q string, vals ...any) ([]ports.RunRecord, error) {
	rows, err := r.db.QueryContext(ctx, q, vals...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.RunRecord
	for rows.Next() {
		rec, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

func (r runStore) ListByThread(ctx context.Context, threadID string) ([]ports.RunRecord, error) {
	return r.query(ctx, `SELECT `+runCols+` FROM agentic_runs WHERE "threadId" = $1 ORDER BY "startedAt" DESC`, threadID)
}

func (r runStore) List(ctx context.Context, f ports.RunFilter) ([]ports.RunRecord, error) {
	var a args
	var where []string
	if len(f.State) > 0 {
		where = append(where, `state IN `+a.in(f.State))
	}
	if f.Agent != "" {
		where = append(where, `agent = `+a.add(f.Agent))
	}
	if f.ThreadID != "" {
		where = append(where, `"threadId" = `+a.add(f.ThreadID))
	}
	if f.Since != nil {
		where = append(where, `"startedAt" >= `+a.add(*f.Since))
	}
	if f.Until != nil {
		where = append(where, `"startedAt" <= `+a.add(*f.Until))
	}
	q := `SELECT ` + runCols + ` FROM agentic_runs`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	q += ` ORDER BY "startedAt" DESC LIMIT ` + a.add(limit)
	return r.query(ctx, q, a.vals...)
}

func (r runStore) CountByState(ctx context.Context) (map[ports.ExecutionState]int, error) {
	return countByState(ctx, r.db, "agentic_runs")
}

type stepStore struct{ db *sql.DB }

func (s stepStore) Record(ctx context.Context, n ports.NewStepRecord) error {
	at := n.At
	if at.IsZero() {
		at = time.Now()
	}
	tools, _ := json.Marshal(n.Tools)
	var toolCalls any
	if n.ToolCalls != nil {
		b, _ := json.Marshal(n.ToolCalls)
		toolCalls = string(b)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO agentic_steps ("runId", "threadId", "agentId", "index", "durationMs", "finishReason",
		   "inputTokens", "cachedInputTokens", "outputTokens", "totalTokens", tools, text, "toolCalls", "costMicros", currency, at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		n.RunID, nullStr(n.ThreadID), nullStr(n.AgentID), n.Index, n.DurationMs, n.FinishReason,
		n.InputTokens, n.CachedInputTokens, n.OutputTokens, n.TotalTokens, string(tools), nullStr(n.Text), toolCalls,
		n.CostMicros, nullStr(n.Currency), at)
	return err
}

const stepCols = `"runId", "threadId", "agentId", "index", "durationMs", "finishReason", "inputTokens", "cachedInputTokens", "outputTokens", "totalTokens", tools, text, "toolCalls", "costMicros", currency, at`

func (s stepStore) query(ctx context.Context, q string, vals ...any) ([]ports.StepRecord, error) {
	rows, err := s.db.QueryContext(ctx, q, vals...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.StepRecord
	for rows.Next() {
		var st ports.StepRecord
		var threadID, agentID, text, currency sql.NullString
		var tools, toolCalls []byte
		if err := rows.Scan(&st.RunID, &threadID, &agentID, &st.Index, &st.DurationMs, &st.FinishReason,
			&st.InputTokens, &st.CachedInputTokens, &st.OutputTokens, &st.TotalTokens, &tools, &text, &toolCalls,
			&st.CostMicros, &currency, &st.At); err != nil {
			return nil, err
		}
		st.ThreadID, st.AgentID, st.Text, st.Currency = threadID.String, agentID.String, text.String, currency.String
		st.Tools = []string{}
		if len(tools) > 0 {
			_ = json.Unmarshal(tools, &st.Tools)
		}
		if len(toolCalls) > 0 {
			_ = json.Unmarshal(toolCalls, &st.ToolCalls)
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s stepStore) ListByRun(ctx context.Context, runID string) ([]ports.StepRecord, error) {
	return s.query(ctx, `SELECT `+stepCols+` FROM agentic_steps WHERE "runId" = $1 ORDER BY "index"`, runID)
}

func (s stepStore) ListByThread(ctx context.Context, threadID string) ([]ports.StepRecord, error) {
	return s.query(ctx, `SELECT `+stepCols+` FROM agentic_steps WHERE "threadId" = $1 ORDER BY at`, threadID)
}
