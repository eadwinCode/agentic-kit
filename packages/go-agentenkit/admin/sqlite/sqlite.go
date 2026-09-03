// Package sqlite holds operational history in SQLite (§2.9): the development
// store. Tables are prefixed agentic_ because this may well share a file
// with the caller's own schema; nothing here should collide with a table
// they own. The schema is the TypeScript package's.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Schema is the DDL, split per statement.
var Schema = []string{
	`CREATE TABLE IF NOT EXISTS agentic_runs (
	  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, parentRunId TEXT,
	  depth INTEGER NOT NULL DEFAULT 0, agent TEXT NOT NULL, model TEXT NOT NULL,
	  state TEXT NOT NULL DEFAULT 'RUNNING', stopReason TEXT, error TEXT,
	  startedAt INTEGER NOT NULL, endedAt INTEGER, durationMs INTEGER, queuedMs INTEGER,
	  attempts INTEGER NOT NULL DEFAULT 0, steps INTEGER NOT NULL DEFAULT 0,
	  inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
	  outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
	  result TEXT, prompt TEXT, tokenBudget INTEGER, runState TEXT, providerOptions TEXT)`,
	`CREATE INDEX IF NOT EXISTS agentic_runs_thread ON agentic_runs(threadId, startedAt)`,
	`CREATE INDEX IF NOT EXISTS agentic_runs_state ON agentic_runs(state, startedAt)`,
	`CREATE INDEX IF NOT EXISTS agentic_runs_parent ON agentic_runs(parentRunId)`,
	`CREATE TABLE IF NOT EXISTS agentic_threads (
	  id TEXT PRIMARY KEY, state TEXT NOT NULL, model TEXT NOT NULL,
	  firstSeenAt INTEGER NOT NULL, updatedAt INTEGER NOT NULL, startedWith TEXT)`,
	`CREATE INDEX IF NOT EXISTS agentic_threads_state ON agentic_threads(state, updatedAt)`,
	`CREATE TABLE IF NOT EXISTS agentic_steps (
	  runId TEXT NOT NULL, threadId TEXT, agentId TEXT, "index" INTEGER NOT NULL,
	  durationMs INTEGER NOT NULL, finishReason TEXT NOT NULL,
	  inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
	  outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
	  tools TEXT, text TEXT, toolCalls TEXT, at INTEGER NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS agentic_steps_run ON agentic_steps(runId, "index")`,
	`CREATE INDEX IF NOT EXISTS agentic_steps_thread ON agentic_steps(threadId, at)`,
}

// Store is an AdminStore over SQLite.
type Store struct{ db *sql.DB }

// New creates the tables if missing and returns the store. It owns the
// handle from here on; Close closes it.
func New(db *sql.DB) (*Store, error) {
	for _, stmt := range Schema {
		if _, err := db.Exec(stmt); err != nil {
			return nil, fmt.Errorf("sqlite admin schema: %w", err)
		}
	}
	// CREATE TABLE IF NOT EXISTS never adds a column to a database that
	// already exists, so newer fields are added separately.
	if err := addMissing(db, "agentic_steps", map[string]string{
		"text": "TEXT", "toolCalls": "TEXT", "threadId": "TEXT",
	}); err != nil {
		return nil, err
	}
	if err := addMissing(db, "agentic_runs", map[string]string{
		"prompt": "TEXT", "tokenBudget": "INTEGER", "runState": "TEXT", "providerOptions": "TEXT",
	}); err != nil {
		return nil, err
	}
	if err := addMissing(db, "agentic_threads", map[string]string{"startedWith": "TEXT"}); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func addMissing(db *sql.DB, table string, cols map[string]string) error {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		have[name] = true
	}
	rows.Close()
	for col, typ := range cols {
		if !have[col] {
			if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, col, typ)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) Threads() ports.AdminThreadStore { return threadStore{s.db} }
func (s *Store) Runs() ports.RunStore            { return runStore{s.db} }
func (s *Store) Steps() ports.StepStore          { return stepStore{s.db} }
func (s *Store) Close() error                    { return s.db.Close() }

func ms(t time.Time) int64     { return t.UnixMilli() }
func fromMs(n int64) time.Time { return time.UnixMilli(n) }
func nullStr(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
func placeholders(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }

type threadStore struct{ db *sql.DB }

func (t threadStore) Upsert(ctx context.Context, n ports.NewAdminThread) error {
	now := ms(time.Now())
	var started sql.NullString
	if n.StartedWith != nil {
		b, _ := json.Marshal(n.StartedWith)
		started = sql.NullString{String: string(b), Valid: true}
	}
	// firstSeenAt and startedWith survive an update; the rest is overwritten.
	_, err := t.db.ExecContext(ctx,
		`INSERT INTO agentic_threads (id,state,model,firstSeenAt,updatedAt,startedWith) VALUES (?,?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET state = excluded.state, model = excluded.model, updatedAt = excluded.updatedAt,
		   startedWith = COALESCE(agentic_threads.startedWith, excluded.startedWith)`,
		n.ID, string(n.State), n.Model, now, now, started)
	return err
}

func countByState(ctx context.Context, db *sql.DB, table string) (map[ports.ExecutionState]int, error) {
	rows, err := db.QueryContext(ctx, `SELECT state, COUNT(*) FROM `+table+` GROUP BY state`)
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
	var where []string
	var args []any
	if len(f.State) > 0 {
		where = append(where, `state IN (`+placeholders(len(f.State))+`)`)
		for _, s := range f.State {
			args = append(args, string(s))
		}
	}
	if f.Since != nil {
		where = append(where, `updatedAt >= ?`)
		args = append(args, ms(*f.Since))
	}
	q := `SELECT id, state, model, firstSeenAt, updatedAt, startedWith FROM agentic_threads`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)
	rows, err := t.db.QueryContext(ctx, q+` ORDER BY updatedAt DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.AdminThread
	for rows.Next() {
		var th ports.AdminThread
		var first, updated int64
		var started sql.NullString
		if err := rows.Scan(&th.ID, &th.State, &th.Model, &first, &updated, &started); err != nil {
			return nil, err
		}
		th.FirstSeenAt, th.UpdatedAt = fromMs(first), fromMs(updated)
		if started.Valid {
			var s ports.ThreadStart
			if json.Unmarshal([]byte(started.String), &s) == nil {
				th.StartedWith = &s
			}
		}
		out = append(out, th)
	}
	return out, rows.Err()
}

type runStore struct{ db *sql.DB }

const runCols = `id, threadId, parentRunId, depth, agent, model, state, stopReason, error, startedAt, endedAt,
	durationMs, queuedMs, attempts, steps, inputTokens, cachedInputTokens, outputTokens, totalTokens,
	result, prompt, tokenBudget, runState, providerOptions`

func scanRun(row interface{ Scan(...any) error }) (*ports.RunRecord, error) {
	var r ports.RunRecord
	var parent, stop, errMsg, result, prompt, runState, providerOptions sql.NullString
	var started int64
	var ended, duration, queued, budget sql.NullInt64
	if err := row.Scan(&r.ID, &r.ThreadID, &parent, &r.Depth, &r.Agent, &r.Model, &r.State, &stop, &errMsg,
		&started, &ended, &duration, &queued, &r.Attempts, &r.Steps, &r.InputTokens, &r.CachedInputTokens,
		&r.OutputTokens, &r.TotalTokens, &result, &prompt, &budget, &runState, &providerOptions); err != nil {
		return nil, err
	}
	r.ParentRunID, r.StopReason, r.Error, r.Prompt = parent.String, stop.String, errMsg.String, prompt.String
	r.StartedAt = fromMs(started)
	if ended.Valid {
		t := fromMs(ended.Int64)
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
	if result.Valid {
		r.Result = json.RawMessage(result.String)
	}
	if runState.Valid {
		_ = json.Unmarshal([]byte(runState.String), &r.RunState)
	}
	if providerOptions.Valid {
		_ = json.Unmarshal([]byte(providerOptions.String), &r.ProviderOptions)
	}
	return &r, nil
}

func (r runStore) Start(ctx context.Context, n ports.NewRunRecord) (*ports.RunRecord, error) {
	started := ms(time.Now())
	var budget sql.NullInt64
	if n.TokenBudget != nil {
		budget = sql.NullInt64{Int64: int64(*n.TokenBudget), Valid: true}
	}
	var runState, providerOptions sql.NullString
	if n.RunState != nil {
		b, _ := json.Marshal(n.RunState)
		runState = sql.NullString{String: string(b), Valid: true}
	}
	if n.ProviderOptions != nil {
		b, _ := json.Marshal(n.ProviderOptions)
		providerOptions = sql.NullString{String: string(b), Valid: true}
	}
	if _, err := r.db.ExecContext(ctx,
		`INSERT INTO agentic_runs (id,threadId,parentRunId,depth,agent,model,state,startedAt,prompt,tokenBudget,runState,providerOptions)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		n.ID, n.ThreadID, nullStr(n.ParentRunID), n.Depth, n.Agent, n.Model, string(ports.StateRunning), started,
		nullStr(n.Prompt), budget, runState, providerOptions); err != nil {
		return nil, err
	}
	return r.Get(ctx, n.ID)
}

func (r runStore) Patch(ctx context.Context, runID string, p ports.RunPatch) error {
	var cols []string
	var vals []any
	set := func(col string, v any) {
		cols = append(cols, col+" = ?")
		vals = append(vals, v)
	}
	if p.State != nil {
		set("state", string(*p.State))
	}
	if p.StopReason != nil {
		set("stopReason", *p.StopReason)
	}
	if p.Error != nil {
		set("error", *p.Error)
	}
	if p.EndedAt != nil {
		set("endedAt", ms(*p.EndedAt))
	}
	if p.DurationMs != nil {
		set("durationMs", *p.DurationMs)
	}
	if p.QueuedMs != nil {
		set("queuedMs", *p.QueuedMs)
	}
	if p.Steps != nil {
		set("steps", *p.Steps)
	}
	if p.InputTokens != nil {
		set("inputTokens", *p.InputTokens)
	}
	if p.CachedInputTokens != nil {
		set("cachedInputTokens", *p.CachedInputTokens)
	}
	if p.OutputTokens != nil {
		set("outputTokens", *p.OutputTokens)
	}
	if p.TotalTokens != nil {
		set("totalTokens", *p.TotalTokens)
	}
	if p.Attempts != nil {
		set("attempts", *p.Attempts)
	}
	if p.Result != nil {
		set("result", string(p.Result))
	}
	if len(cols) == 0 {
		return nil
	}
	vals = append(vals, runID)
	_, err := r.db.ExecContext(ctx, `UPDATE agentic_runs SET `+strings.Join(cols, ", ")+` WHERE id = ?`, vals...)
	return err
}

func (r runStore) Get(ctx context.Context, runID string) (*ports.RunRecord, error) {
	rec, err := scanRun(r.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM agentic_runs WHERE id = ?`, runID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return rec, err
}

func (r runStore) query(ctx context.Context, q string, args ...any) ([]ports.RunRecord, error) {
	rows, err := r.db.QueryContext(ctx, q, args...)
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
	return r.query(ctx, `SELECT `+runCols+` FROM agentic_runs WHERE threadId = ? ORDER BY startedAt DESC, rowid DESC`, threadID)
}

func (r runStore) List(ctx context.Context, f ports.RunFilter) ([]ports.RunRecord, error) {
	var where []string
	var args []any
	if len(f.State) > 0 {
		where = append(where, `state IN (`+placeholders(len(f.State))+`)`)
		for _, s := range f.State {
			args = append(args, string(s))
		}
	}
	if f.Agent != "" {
		where = append(where, `agent = ?`)
		args = append(args, f.Agent)
	}
	if f.ThreadID != "" {
		where = append(where, `threadId = ?`)
		args = append(args, f.ThreadID)
	}
	if f.Since != nil {
		where = append(where, `startedAt >= ?`)
		args = append(args, ms(*f.Since))
	}
	if f.Until != nil {
		where = append(where, `startedAt <= ?`)
		args = append(args, ms(*f.Until))
	}
	q := `SELECT ` + runCols + ` FROM agentic_runs`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)
	return r.query(ctx, q+` ORDER BY startedAt DESC, rowid DESC LIMIT ?`, args...)
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
	var toolCalls sql.NullString
	if n.ToolCalls != nil {
		b, _ := json.Marshal(n.ToolCalls)
		toolCalls = sql.NullString{String: string(b), Valid: true}
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO agentic_steps (runId,threadId,agentId,"index",durationMs,finishReason,inputTokens,cachedInputTokens,outputTokens,totalTokens,tools,text,toolCalls,at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		n.RunID, n.ThreadID, nullStr(n.AgentID), n.Index, n.DurationMs, n.FinishReason,
		n.InputTokens, n.CachedInputTokens, n.OutputTokens, n.TotalTokens,
		string(tools), nullStr(n.Text), toolCalls, ms(at))
	return err
}

const stepCols = `runId, threadId, agentId, "index", durationMs, finishReason, inputTokens, cachedInputTokens, outputTokens, totalTokens, tools, text, toolCalls, at`

func (s stepStore) query(ctx context.Context, q string, args ...any) ([]ports.StepRecord, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.StepRecord
	for rows.Next() {
		var st ports.StepRecord
		var threadID, agentID, tools, text, toolCalls sql.NullString
		var at int64
		if err := rows.Scan(&st.RunID, &threadID, &agentID, &st.Index, &st.DurationMs, &st.FinishReason,
			&st.InputTokens, &st.CachedInputTokens, &st.OutputTokens, &st.TotalTokens, &tools, &text, &toolCalls, &at); err != nil {
			return nil, err
		}
		st.ThreadID, st.AgentID, st.Text, st.At = threadID.String, agentID.String, text.String, fromMs(at)
		st.Tools = []string{}
		if tools.Valid {
			_ = json.Unmarshal([]byte(tools.String), &st.Tools)
		}
		if toolCalls.Valid {
			_ = json.Unmarshal([]byte(toolCalls.String), &st.ToolCalls)
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s stepStore) ListByRun(ctx context.Context, runID string) ([]ports.StepRecord, error) {
	return s.query(ctx, `SELECT `+stepCols+` FROM agentic_steps WHERE runId = ? ORDER BY "index"`, runID)
}

func (s stepStore) ListByThread(ctx context.Context, threadID string) ([]ports.StepRecord, error) {
	return s.query(ctx, `SELECT `+stepCols+` FROM agentic_steps WHERE threadId = ? ORDER BY at, rowid`, threadID)
}
