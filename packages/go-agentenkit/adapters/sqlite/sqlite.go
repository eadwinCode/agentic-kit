// Package sqlite holds a complete Storage over SQLite (§3.2): every table the
// platform needs, created on construction. A working agent platform with no
// infrastructure to stand up first.
//
// The package never imports a driver itself (§3.4). Hand it any *sql.DB
// opened with a SQLite driver, or call Open, which picks whichever
// database/sql driver the process has registered ("sqlite" from
// modernc.org/sqlite, or "sqlite3" from github.com/mattn/go-sqlite3).
//
// The schema is the same one the TypeScript SqliteStorage creates, so both
// runtimes can read one file.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Open opens a SQLite file with whichever driver the process has. Pass
// ":memory:" for a database that lives only as long as the process.
func Open(filename string) (*sql.DB, error) {
	drivers := sql.Drivers()
	for _, name := range []string{"sqlite", "sqlite3"} {
		if slices.Contains(drivers, name) {
			db, err := sql.Open(name, filename)
			if err != nil {
				return nil, err
			}
			// SQLite serialises writers anyway, and an in-memory database is
			// per connection: one connection is the only correct pool size.
			db.SetMaxOpenConns(1)
			return db, nil
		}
	}
	return nil, errors.New("no SQLite driver registered: import modernc.org/sqlite or github.com/mattn/go-sqlite3, " +
		"or construct the store with your own *sql.DB")
}

// Schema is the DDL, split per statement.
var Schema = []string{
	`CREATE TABLE IF NOT EXISTS threads (
	  id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT 'New Thread',
	  state TEXT NOT NULL DEFAULT 'IDLE', model TEXT NOT NULL DEFAULT 'gpt-4o',
	  createdAt INTEGER NOT NULL, updatedAt INTEGER NOT NULL)`,
	`CREATE TABLE IF NOT EXISTS messages (
	  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, agentId TEXT,
	  role TEXT NOT NULL, content TEXT NOT NULL, createdAt INTEGER NOT NULL,
	  seq INTEGER NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS messages_thread ON messages(threadId, seq)`,
	`CREATE TABLE IF NOT EXISTS events (
	  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, seq INTEGER NOT NULL,
	  type TEXT NOT NULL, payload TEXT, createdAt INTEGER NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS events_thread_seq ON events(threadId, seq)`,
	`CREATE INDEX IF NOT EXISTS events_thread_type ON events(threadId, type, seq)`,
	// One row per MODEL CALL (§4), not per run segment. cachedInputTokens
	// holds cache READS, keeping the column that was already there meaning
	// what it always meant; cache writes are their own column beside it.
	// A NULL costMicros is an unpriced call, which is not the same as one
	// that cost nothing.
	`CREATE TABLE IF NOT EXISTS usage (
	  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, runId TEXT, agentId TEXT, agentName TEXT,
	  kind TEXT NOT NULL DEFAULT 'step', step INTEGER NOT NULL DEFAULT 0,
	  model TEXT, modelId TEXT,
	  inputTokens INTEGER NOT NULL, cachedInputTokens INTEGER NOT NULL,
	  cacheWriteInputTokens INTEGER NOT NULL DEFAULT 0,
	  outputTokens INTEGER NOT NULL, reasoningTokens INTEGER NOT NULL DEFAULT 0,
	  totalTokens INTEGER NOT NULL,
	  outcome TEXT NOT NULL DEFAULT 'finished', estimated INTEGER NOT NULL DEFAULT 0,
	  providerMetadata TEXT,
	  costMicros INTEGER, costCurrency TEXT, costSource TEXT,
	  createdAt INTEGER NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS usage_thread ON usage(threadId)`,
	`CREATE INDEX IF NOT EXISTS usage_run ON usage(runId, createdAt)`,
}

// usageColumns are the columns added after the first release. CREATE TABLE
// IF NOT EXISTS never adds a column to a database that already exists, so a
// store upgraded in place gets them here instead (§4).
var usageColumns = map[string]string{
	"runId": "TEXT", "agentName": "TEXT", "kind": "TEXT NOT NULL DEFAULT 'step'",
	"step": "INTEGER NOT NULL DEFAULT 0", "model": "TEXT", "modelId": "TEXT",
	"cacheWriteInputTokens": "INTEGER NOT NULL DEFAULT 0",
	"reasoningTokens":       "INTEGER NOT NULL DEFAULT 0",
	"outcome":               "TEXT NOT NULL DEFAULT 'finished'",
	"estimated":             "INTEGER NOT NULL DEFAULT 0",
	"providerMetadata":      "TEXT",
	"costMicros":            "INTEGER", "costCurrency": "TEXT", "costSource": "TEXT",
}

// addMissing adds any of cols the table does not have yet.
func addMissing(db *sql.DB, table string, cols map[string]string) error {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		have[name] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for col, typ := range cols {
		if have[col] {
			continue
		}
		if _, err := db.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + col + ` ` + typ); err != nil {
			return fmt.Errorf("sqlite storage schema: add %s.%s: %w", table, col, err)
		}
	}
	return nil
}

// Storage is a Storage over SQLite.
type Storage struct{ db *sql.DB }

// New creates the tables if missing and returns the storage.
func New(db *sql.DB) (*Storage, error) {
	for _, stmt := range Schema {
		if _, err := db.Exec(stmt); err != nil {
			return nil, fmt.Errorf("sqlite storage schema: %w", err)
		}
	}
	if err := addMissing(db, "usage", usageColumns); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS usage_run ON usage(runId, createdAt)`); err != nil {
		return nil, fmt.Errorf("sqlite storage schema: %w", err)
	}
	return &Storage{db: db}, nil
}

func (s *Storage) Threads() ports.ThreadStore   { return threads{s.db} }
func (s *Storage) Messages() ports.MessageStore { return messages{s.db} }
func (s *Storage) Events() ports.EventStore     { return events{s.db} }
func (s *Storage) Usage() ports.UsageStore      { return usage{s.db} }

func ms(t time.Time) int64            { return t.UnixMilli() }
func fromMs(n int64) time.Time        { return time.UnixMilli(n) }
func nullStr(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

type threads struct{ db *sql.DB }

func scanThread(row interface{ Scan(...any) error }) (*ports.ThreadDTO, error) {
	var t ports.ThreadDTO
	var created, updated int64
	if err := row.Scan(&t.ID, &t.State, &t.Model, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	t.CreatedAt, t.UpdatedAt = fromMs(created), fromMs(updated)
	return &t, nil
}

func (t threads) Get(ctx context.Context, threadID string, _ ports.StorageContext) (*ports.ThreadDTO, error) {
	return scanThread(t.db.QueryRowContext(ctx, `SELECT id, state, model, createdAt, updatedAt FROM threads WHERE id = ?`, threadID))
}

func (t threads) Create(ctx context.Context, init ports.ThreadInit, _ ports.StorageContext) (*ports.ThreadDTO, error) {
	model := init.Model
	if model == "" {
		model = core.DefaultModel
	}
	now := time.Now()
	id := core.NewID()
	if _, err := t.db.ExecContext(ctx,
		`INSERT INTO threads (id,title,state,model,createdAt,updatedAt) VALUES (?,?,?,?,?,?)`,
		id, "New Thread", string(ports.StateIdle), model, ms(now), ms(now)); err != nil {
		return nil, err
	}
	return &ports.ThreadDTO{ID: id, State: ports.StateIdle, Model: model, CreatedAt: fromMs(ms(now)), UpdatedAt: fromMs(ms(now))}, nil
}

func (t threads) List(ctx context.Context, _ ports.StorageContext) ([]ports.ThreadDTO, error) {
	rows, err := t.db.QueryContext(ctx, `SELECT id, state, model, createdAt, updatedAt FROM threads ORDER BY updatedAt DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.ThreadDTO
	for rows.Next() {
		th, err := scanThread(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *th)
	}
	return out, rows.Err()
}

func (t threads) SetState(ctx context.Context, threadID string, state ports.ExecutionState, _ ports.StorageContext) error {
	_, err := t.db.ExecContext(ctx, `UPDATE threads SET state = ?, updatedAt = ? WHERE id = ?`, string(state), ms(time.Now()), threadID)
	return err
}

func (t threads) Delete(ctx context.Context, threadID string, _ ports.StorageContext) error {
	res, err := t.db.ExecContext(ctx, `DELETE FROM threads WHERE id = ?`, threadID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("unknown thread %s", threadID)
	}
	// No FK cascade here: the cascade is spelled out, so a caller can read
	// exactly what is removed.
	for _, table := range []string{"messages", "events", "usage"} {
		if _, err := t.db.ExecContext(ctx, `DELETE FROM `+table+` WHERE threadId = ?`, threadID); err != nil {
			return err
		}
	}
	return nil
}

func (t threads) ClaimState(ctx context.Context, threadID string, from, to ports.ExecutionState, _ ports.StorageContext) (bool, error) {
	// The §3.4 compare-and-set: one conditional UPDATE, so exactly one caller can win.
	res, err := t.db.ExecContext(ctx, `UPDATE threads SET state = ?, updatedAt = ? WHERE id = ? AND state = ?`,
		string(to), ms(time.Now()), threadID, string(from))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type messages struct{ db *sql.DB }

func scanMessage(row interface{ Scan(...any) error }) (*ports.MessageDTO, error) {
	var m ports.MessageDTO
	var agentID sql.NullString
	var content string
	var created int64
	if err := row.Scan(&m.ID, &m.ThreadID, &agentID, &m.Role, &content, &created); err != nil {
		return nil, err
	}
	m.AgentID = agentID.String
	m.Content = json.RawMessage(content)
	m.CreatedAt = fromMs(created)
	return &m, nil
}

func (m messages) Append(ctx context.Context, threadID string, msg ports.NewMessage, _ ports.StorageContext) (*ports.MessageDTO, error) {
	now := time.Now()
	id := core.NewID()
	content := msg.Content
	if len(content) == 0 {
		content = json.RawMessage("null")
	}
	// An explicit seq keeps insertion order stable: several messages land
	// inside the same millisecond, so createdAt alone cannot order them.
	if _, err := m.db.ExecContext(ctx,
		`INSERT INTO messages (id,threadId,agentId,role,content,createdAt,seq)
		 VALUES (?,?,?,?,?,?,(SELECT COALESCE(MAX(seq),0)+1 FROM messages WHERE threadId = ?))`,
		id, threadID, nullStr(msg.AgentID), string(msg.Role), string(content), ms(now), threadID); err != nil {
		return nil, err
	}
	return &ports.MessageDTO{ID: id, ThreadID: threadID, AgentID: msg.AgentID, Role: msg.Role, Content: content, CreatedAt: fromMs(ms(now))}, nil
}

func (m messages) List(ctx context.Context, threadID string, scope *ports.MessageScope, _ ports.StorageContext) ([]ports.MessageDTO, error) {
	q := `SELECT id, threadId, agentId, role, content, createdAt FROM messages WHERE threadId = ?`
	args := []any{threadID}
	switch {
	case scope == nil:
	case scope.AgentID == "":
		q += ` AND agentId IS NULL`
	default:
		q += ` AND agentId = ?`
		args = append(args, scope.AgentID)
	}
	rows, err := m.db.QueryContext(ctx, q+` ORDER BY seq`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.MessageDTO
	for rows.Next() {
		row, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *row)
	}
	return out, rows.Err()
}

func (m messages) DeleteFrom(ctx context.Context, threadID, messageID string, _ ports.StorageContext) (int, error) {
	var seq int64
	err := m.db.QueryRowContext(ctx, `SELECT seq FROM messages WHERE id = ? AND threadId = ?`, messageID, threadID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	res, err := m.db.ExecContext(ctx, `DELETE FROM messages WHERE threadId = ? AND seq >= ?`, threadID, seq)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

type events struct{ db *sql.DB }

func scanEvent(row interface{ Scan(...any) error }) (*ports.AgentEvent, error) {
	var e ports.AgentEvent
	var payload sql.NullString
	var created int64
	if err := row.Scan(&e.ThreadID, &e.Seq, &e.Type, &payload, &created); err != nil {
		return nil, err
	}
	if payload.Valid {
		e.Payload = json.RawMessage(payload.String)
	} else {
		e.Payload = json.RawMessage("null")
	}
	e.CreatedAt = fromMs(created)
	return &e, nil
}

func (e events) query(ctx context.Context, q string, args ...any) ([]ports.AgentEvent, error) {
	rows, err := e.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.AgentEvent
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ev)
	}
	return out, rows.Err()
}

const eventCols = `threadId, seq, type, payload, createdAt`

func (e events) Append(ctx context.Context, threadID string, ev ports.AgentEvent, _ ports.StorageContext) error {
	_, err := e.db.ExecContext(ctx, `INSERT INTO events (id,threadId,seq,type,payload,createdAt) VALUES (?,?,?,?,?,?)`,
		core.NewID(), threadID, ev.Seq, ev.Type, string(core.MarshalPayload(ev.Payload)), ms(ev.CreatedAt))
	return err
}

func (e events) ListSince(ctx context.Context, threadID string, sinceSeq int64, _ ports.StorageContext) ([]ports.AgentEvent, error) {
	return e.query(ctx, `SELECT `+eventCols+` FROM events WHERE threadId = ? AND seq > ? ORDER BY seq`, threadID, sinceSeq)
}

func (e events) Latest(ctx context.Context, threadID, typ string, _ ports.StorageContext) (*ports.AgentEvent, error) {
	rows, err := e.query(ctx, `SELECT `+eventCols+` FROM events WHERE threadId = ? AND type = ? ORDER BY seq DESC LIMIT 1`, threadID, typ)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return &rows[0], nil
}

func (e events) ListByType(ctx context.Context, threadID, typ string, _ ports.StorageContext) ([]ports.AgentEvent, error) {
	return e.query(ctx, `SELECT `+eventCols+` FROM events WHERE threadId = ? AND type = ? ORDER BY seq`, threadID, typ)
}

type usage struct{ db *sql.DB }

// usageGroup is the grouped read Total does: one row per agent and model,
// which is exactly one UsageLine plus the two figures that only make sense
// on the whole total.
const usageGroup = `SELECT COALESCE(agentId,''), COALESCE(agentName,''), COALESCE(model,''), COALESCE(modelId,''),
	  COALESCE(SUM(inputTokens),0), COALESCE(SUM(cachedInputTokens),0), COALESCE(SUM(cacheWriteInputTokens),0),
	  COALESCE(SUM(outputTokens),0), COALESCE(SUM(reasoningTokens),0), COALESCE(SUM(totalTokens),0),
	  COUNT(*), COALESCE(SUM(estimated),0),
	  COALESCE(SUM(costMicros),0), MAX(costCurrency),
	  COALESCE(SUM(CASE WHEN costMicros IS NULL THEN 1 ELSE 0 END),0)
	FROM usage WHERE threadId = ?`

func (u usage) Record(ctx context.Context, threadID string, n ports.NewUsage, _ ports.StorageContext) error {
	var meta any
	if len(n.ProviderMetadata) > 0 {
		raw, err := json.Marshal(n.ProviderMetadata)
		if err != nil {
			return fmt.Errorf("usage provider metadata: %w", err)
		}
		meta = string(raw)
	}
	var micros, currency, source any
	if n.Cost != nil {
		micros, currency, source = n.Cost.Micros, n.Cost.Currency, n.Cost.Source
	}
	kind := n.Kind
	if kind == "" {
		kind = ports.KindStep
	}
	outcome := n.Outcome
	if outcome == "" {
		outcome = ports.UsageFinished
	}
	_, err := u.db.ExecContext(ctx,
		`INSERT INTO usage (id,threadId,runId,agentId,agentName,kind,step,model,modelId,
		  inputTokens,cachedInputTokens,cacheWriteInputTokens,outputTokens,reasoningTokens,
		  totalTokens,outcome,estimated,providerMetadata,costMicros,costCurrency,costSource,createdAt)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		core.NewID(), threadID, nullStr(n.RunID), nullStr(n.AgentID), nullStr(n.AgentName),
		string(kind), n.Step, nullStr(n.Model), nullStr(n.ModelID),
		n.InputTokens, n.CacheReadInputTokens, n.CacheWriteInputTokens, n.OutputTokens, n.ReasoningTokens,
		n.TotalTokens(), string(outcome), n.Estimated, meta, micros, currency, source, ms(time.Now()))
	return err
}

func (u usage) Total(ctx context.Context, threadID string, f ports.UsageFilter, _ ports.StorageContext) (ports.UsageTotals, error) {
	q, args := usageGroup, []any{threadID}
	if f.RunID != "" {
		q += ` AND runId = ?`
		args = append(args, f.RunID)
	}
	q += ` GROUP BY agentId, agentName, model, modelId ORDER BY MIN(createdAt)`
	rows, err := u.db.QueryContext(ctx, q, args...)
	if err != nil {
		return ports.UsageTotals{}, err
	}
	defer rows.Close()

	var t ports.UsageTotals
	for rows.Next() {
		var l ports.UsageLine
		var currency sql.NullString
		var totalTokens, unpriced int
		if err := rows.Scan(&l.AgentID, &l.AgentName, &l.Model, &l.ModelID,
			&l.InputTokens, &l.CacheReadInputTokens, &l.CacheWriteInputTokens,
			&l.OutputTokens, &l.ReasoningTokens, &totalTokens,
			&l.Calls, &l.Estimated, &l.CostMicros, &currency, &unpriced); err != nil {
			return ports.UsageTotals{}, err
		}
		t.InputTokens += l.InputTokens
		t.CachedInputTokens += l.CacheReadInputTokens
		t.OutputTokens += l.OutputTokens
		t.TotalTokens += totalTokens
		t.CostMicros += l.CostMicros
		t.Unpriced += unpriced
		if t.Currency == "" {
			t.Currency = currency.String
		}
		t.Lines = append(t.Lines, l)
	}
	return t, rows.Err()
}
