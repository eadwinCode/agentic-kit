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
	`CREATE TABLE IF NOT EXISTS usage (
	  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, agentId TEXT,
	  inputTokens INTEGER NOT NULL, cachedInputTokens INTEGER NOT NULL,
	  outputTokens INTEGER NOT NULL, totalTokens INTEGER NOT NULL, createdAt INTEGER NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS usage_thread ON usage(threadId)`,
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

func (u usage) Record(ctx context.Context, threadID string, n ports.NewUsage, _ ports.StorageContext) error {
	_, err := u.db.ExecContext(ctx,
		`INSERT INTO usage (id,threadId,agentId,inputTokens,cachedInputTokens,outputTokens,totalTokens,createdAt) VALUES (?,?,?,?,?,?,?,?)`,
		core.NewID(), threadID, nullStr(n.AgentID), n.InputTokens, n.CachedInputTokens, n.OutputTokens, n.TotalTokens, ms(time.Now()))
	return err
}

func (u usage) Total(ctx context.Context, threadID string, _ ports.StorageContext) (ports.UsageTotals, error) {
	var t ports.UsageTotals
	err := u.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(inputTokens),0), COALESCE(SUM(cachedInputTokens),0),
		        COALESCE(SUM(outputTokens),0), COALESCE(SUM(totalTokens),0) FROM usage WHERE threadId = ?`, threadID).
		Scan(&t.InputTokens, &t.CachedInputTokens, &t.OutputTokens, &t.TotalTokens)
	return t, err
}
