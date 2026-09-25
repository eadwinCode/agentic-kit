// Package postgres holds a complete Storage over Postgres (§3.2): the
// production storage adapter, the Go stand-in for the TypeScript package's
// PrismaStorage.
//
// It owns its tables, prefixed agentenkit_ by default because a production
// database is usually shared with the caller's own schema. Hand it any
// *sql.DB opened with a Postgres driver; the package never imports one.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/migrate"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Option tunes New.
type Option func(*Storage)

// WithPrefix sets the table prefix. Default: "agentenkit_".
func WithPrefix(prefix string) Option { return func(s *Storage) { s.prefix = prefix } }

// Storage is a Storage over Postgres.
type Storage struct {
	db     *sql.DB
	prefix string
}

// New creates the tables if they are missing, then returns the storage.
func New(ctx context.Context, db *sql.DB, opts ...Option) (*Storage, error) {
	s := &Storage{db: db, prefix: "agentenkit_"}
	for _, o := range opts {
		o(s)
	}
	if err := migrateSchema(ctx, db, s.prefix, "storage", []migrate.Migration{
		migrate.NewMigration("storage_0001_init", s.schema()...),
		// One event per seq on a thread: a counter that restarted must fail
		// its write, never land a second event under a seq clients already
		// have. A log from before this check may hold duplicates; the index
		// is then left off, said so, and tried again on the next start.
		optional(migrate.NewMigration("storage_0002_events_seq_unique",
			`CREATE UNIQUE INDEX IF NOT EXISTS `+s.t("events_thread_seq_unique")+` ON `+s.t("events")+`("threadId", seq)`)),
		// The same for messages, so two appends that raced to one seq cannot
		// both land (Append retries the loser).
		optional(migrate.NewMigration("storage_0003_messages_seq_unique",
			`CREATE UNIQUE INDEX IF NOT EXISTS `+s.t("messages_thread_seq_unique")+` ON `+s.t("messages")+`("threadId", seq)`)),
	}); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Storage) t(name string) string { return s.prefix + name }

func (s *Storage) schema() []string {
	p := s.prefix
	return []string{
		`CREATE TABLE IF NOT EXISTS ` + p + `threads (
		   id TEXT PRIMARY KEY, state TEXT NOT NULL DEFAULT 'IDLE', model TEXT NOT NULL DEFAULT 'gpt-4o',
		   "createdAt" TIMESTAMPTZ NOT NULL DEFAULT now(), "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT now())`,
		`CREATE TABLE IF NOT EXISTS ` + p + `messages (
		   id TEXT PRIMARY KEY, "threadId" TEXT NOT NULL REFERENCES ` + p + `threads(id) ON DELETE CASCADE,
		   "agentId" TEXT, role TEXT NOT NULL, content JSONB NOT NULL, seq BIGINT NOT NULL,
		   "createdAt" TIMESTAMPTZ NOT NULL DEFAULT now())`,
		`CREATE INDEX IF NOT EXISTS ` + p + `messages_thread ON ` + p + `messages("threadId", seq)`,
		`CREATE TABLE IF NOT EXISTS ` + p + `events (
		   id TEXT PRIMARY KEY, "threadId" TEXT NOT NULL REFERENCES ` + p + `threads(id) ON DELETE CASCADE,
		   seq BIGINT NOT NULL, type TEXT NOT NULL, payload JSONB, "createdAt" TIMESTAMPTZ NOT NULL DEFAULT now())`,
		`CREATE INDEX IF NOT EXISTS ` + p + `events_thread_seq ON ` + p + `events("threadId", seq)`,
		`CREATE INDEX IF NOT EXISTS ` + p + `events_thread_type ON ` + p + `events("threadId", type, seq)`,
		// One row per MODEL CALL (§4), not per run segment. "cachedInputTokens"
		// holds cache READS, keeping the column that was already there meaning
		// what it always meant; cache writes are their own column beside it. A
		// NULL "costMicros" is an unpriced call, which is not the same as one
		// that cost nothing.
		`CREATE TABLE IF NOT EXISTS ` + p + `usage (
		   id TEXT PRIMARY KEY, "threadId" TEXT NOT NULL REFERENCES ` + p + `threads(id) ON DELETE CASCADE,
		   "agentId" TEXT, "inputTokens" INT NOT NULL, "cachedInputTokens" INT NOT NULL,
		   "outputTokens" INT NOT NULL, "totalTokens" INT NOT NULL, "createdAt" TIMESTAMPTZ NOT NULL DEFAULT now())`,
		`CREATE INDEX IF NOT EXISTS ` + p + `usage_thread ON ` + p + `usage("threadId")`,
		// Added after the first release. CREATE TABLE IF NOT EXISTS never adds
		// a column to a table that already exists, so these go on separately
		// and a store upgraded in place picks them up.
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS "runId" TEXT`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS "agentName" TEXT`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'step'`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS step INT NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS model TEXT`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS "modelId" TEXT`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS "cacheWriteInputTokens" INT NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS "reasoningTokens" INT NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS outcome TEXT NOT NULL DEFAULT 'finished'`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS estimated BOOLEAN NOT NULL DEFAULT false`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS "providerMetadata" JSONB`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS "costMicros" BIGINT`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS "costCurrency" TEXT`,
		`ALTER TABLE ` + p + `usage ADD COLUMN IF NOT EXISTS "costSource" TEXT`,
		`CREATE INDEX IF NOT EXISTS ` + p + `usage_run ON ` + p + `usage("runId", "createdAt")`,
		// The thread's current run, for ThreadTransition's compare-and-set.
		`ALTER TABLE ` + p + `threads ADD COLUMN IF NOT EXISTS "runId" TEXT`,
	}
}

func (s *Storage) Threads() ports.ThreadStore   { return threads{s} }
func (s *Storage) Messages() ports.MessageStore { return messages{s} }
func (s *Storage) Events() ports.EventStore     { return events{s} }
func (s *Storage) Usage() ports.UsageStore      { return usage{s} }

func nullStr(v string) sql.NullString { return sql.NullString{String: v, Valid: v != ""} }

type threads struct{ s *Storage }

const threadCols = `id, state, model, "createdAt", "updatedAt"`

func scanThread(row interface{ Scan(...any) error }) (*ports.ThreadDTO, error) {
	var t ports.ThreadDTO
	if err := row.Scan(&t.ID, &t.State, &t.Model, &t.CreatedAt, &t.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

func (t threads) Get(ctx context.Context, threadID string, _ ports.StorageContext) (*ports.ThreadDTO, error) {
	return scanThread(t.s.db.QueryRowContext(ctx, `SELECT `+threadCols+` FROM `+t.s.t("threads")+` WHERE id = $1`, threadID))
}

func (t threads) Create(ctx context.Context, init ports.ThreadInit, _ ports.StorageContext) (*ports.ThreadDTO, error) {
	model := init.Model
	if model == "" {
		model = core.DefaultModel
	}
	return scanThread(t.s.db.QueryRowContext(ctx,
		`INSERT INTO `+t.s.t("threads")+` (id, state, model) VALUES ($1, 'IDLE', $2) RETURNING `+threadCols,
		core.NewID(), model))
}

func (t threads) List(ctx context.Context, _ ports.StorageContext) ([]ports.ThreadDTO, error) {
	rows, err := t.s.db.QueryContext(ctx, `SELECT `+threadCols+` FROM `+t.s.t("threads")+` ORDER BY "updatedAt" DESC`)
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
	_, err := t.s.db.ExecContext(ctx, `UPDATE `+t.s.t("threads")+` SET state = $1, "updatedAt" = now() WHERE id = $2`, string(state), threadID)
	return err
}

func (t threads) Delete(ctx context.Context, threadID string, _ ports.StorageContext) error {
	// One delete: the schema cascades to messages, events and usage.
	res, err := t.s.db.ExecContext(ctx, `DELETE FROM `+t.s.t("threads")+` WHERE id = $1`, threadID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("unknown thread %s", threadID)
	}
	return nil
}

func (t threads) ClaimState(ctx context.Context, threadID string, from, to ports.ExecutionState, _ ports.StorageContext) (bool, error) {
	// Single conditional UPDATE: the atomicity contract (§3.4)
	res, err := t.s.db.ExecContext(ctx,
		`UPDATE `+t.s.t("threads")+` SET state = $1, "updatedAt" = now() WHERE id = $2 AND state = $3`,
		string(to), threadID, string(from))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (t threads) Transition(ctx context.Context, threadID string, tr ports.ThreadTransition, _ ports.StorageContext) (bool, error) {
	if len(tr.From) == 0 {
		return false, nil
	}
	// Single conditional UPDATE: the atomicity contract (§3.4). A thread
	// with no run recorded yet (from before the column) matches any run.
	args := []any{string(tr.To), threadID, tr.RunID, tr.NewRunID}
	in := make([]string, len(tr.From))
	for i, s := range tr.From {
		args = append(args, string(s))
		in[i] = fmt.Sprintf("$%d", len(args))
	}
	res, err := t.s.db.ExecContext(ctx,
		`UPDATE `+t.s.t("threads")+` SET state = $1, "updatedAt" = now(), "runId" = COALESCE(NULLIF($4, ''), "runId")
		 WHERE id = $2 AND state IN (`+strings.Join(in, ", ")+`)
		   AND ($3 = '' OR "runId" IS NULL OR "runId" = $3)`, args...)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type messages struct{ s *Storage }

const messageCols = `id, "threadId", "agentId", role, content, "createdAt"`

func scanMessage(row interface{ Scan(...any) error }) (*ports.MessageDTO, error) {
	var m ports.MessageDTO
	var agentID sql.NullString
	var content []byte
	if err := row.Scan(&m.ID, &m.ThreadID, &agentID, &m.Role, &content, &m.CreatedAt); err != nil {
		return nil, err
	}
	m.AgentID = agentID.String
	m.Content = json.RawMessage(content)
	return &m, nil
}

func (m messages) Append(ctx context.Context, threadID string, msg ports.NewMessage, _ ports.StorageContext) (*ports.MessageDTO, error) {
	content := msg.Content
	if len(content) == 0 {
		content = json.RawMessage("null")
	}
	// An explicit seq keeps insertion order stable inside one millisecond.
	// Under READ COMMITTED two appends could read the same MAX, so appends to
	// one thread take its row lock first and go one at a time; the unique
	// index is the backstop, and a clash that still gets through is retried.
	for attempt := 0; ; attempt++ {
		out, err := m.append(ctx, threadID, msg.AgentID, string(msg.Role), string(content))
		if err == nil || attempt == 4 || !isUniqueViolation(err) {
			return out, err
		}
	}
}

func (m messages) append(ctx context.Context, threadID, agentID, role, content string) (*ports.MessageDTO, error) {
	tx, err := m.s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT 1 FROM `+m.s.t("threads")+` WHERE id = $1 FOR UPDATE`, threadID); err != nil {
		return nil, err
	}
	out, err := scanMessage(tx.QueryRowContext(ctx,
		`INSERT INTO `+m.s.t("messages")+` (id, "threadId", "agentId", role, content, seq)
		 VALUES ($1, $2, $3, $4, $5, (SELECT COALESCE(MAX(seq),0)+1 FROM `+m.s.t("messages")+` WHERE "threadId" = $2))
		 RETURNING `+messageCols,
		core.NewID(), threadID, nullStr(agentID), role, content))
	if err != nil {
		return nil, err
	}
	return out, tx.Commit()
}

// isUniqueViolation reports a unique-index clash (SQLSTATE 23505), from any
// Postgres driver.
func isUniqueViolation(err error) bool {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState() == "23505"
	}
	return strings.Contains(err.Error(), "23505") || strings.Contains(err.Error(), "duplicate key")
}

func (m messages) List(ctx context.Context, threadID string, scope *ports.MessageScope, _ ports.StorageContext) ([]ports.MessageDTO, error) {
	q := `SELECT ` + messageCols + ` FROM ` + m.s.t("messages") + ` WHERE "threadId" = $1`
	vals := []any{threadID}
	switch {
	case scope == nil:
	case scope.AgentID == "":
		q += ` AND "agentId" IS NULL`
	default:
		q += ` AND "agentId" = $2`
		vals = append(vals, scope.AgentID)
	}
	rows, err := m.s.db.QueryContext(ctx, q+` ORDER BY seq`, vals...)
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
	res, err := m.s.db.ExecContext(ctx,
		`DELETE FROM `+m.s.t("messages")+` WHERE "threadId" = $1
		   AND seq >= (SELECT seq FROM `+m.s.t("messages")+` WHERE id = $2 AND "threadId" = $1)`, threadID, messageID)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

type events struct{ s *Storage }

const eventCols = `"threadId", seq, type, payload, "createdAt"`

func (e events) query(ctx context.Context, q string, vals ...any) ([]ports.AgentEvent, error) {
	rows, err := e.s.db.QueryContext(ctx, q, vals...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.AgentEvent
	for rows.Next() {
		var ev ports.AgentEvent
		var payload []byte
		if err := rows.Scan(&ev.ThreadID, &ev.Seq, &ev.Type, &payload, &ev.CreatedAt); err != nil {
			return nil, err
		}
		ev.Payload = json.RawMessage(payload)
		if len(ev.Payload) == 0 {
			ev.Payload = json.RawMessage("null")
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (e events) Append(ctx context.Context, threadID string, ev ports.AgentEvent, _ ports.StorageContext) error {
	_, err := e.s.db.ExecContext(ctx,
		`INSERT INTO `+e.s.t("events")+` (id, "threadId", seq, type, payload, "createdAt") VALUES ($1,$2,$3,$4,$5,$6)`,
		core.NewID(), threadID, ev.Seq, ev.Type, string(core.MarshalPayload(ev.Payload)), ev.CreatedAt)
	return err
}

func (e events) ListSince(ctx context.Context, threadID string, sinceSeq int64, _ ports.StorageContext) ([]ports.AgentEvent, error) {
	return e.query(ctx, `SELECT `+eventCols+` FROM `+e.s.t("events")+` WHERE "threadId" = $1 AND seq > $2 ORDER BY seq`, threadID, sinceSeq)
}

func (e events) Latest(ctx context.Context, threadID, typ string, _ ports.StorageContext) (*ports.AgentEvent, error) {
	rows, err := e.query(ctx, `SELECT `+eventCols+` FROM `+e.s.t("events")+` WHERE "threadId" = $1 AND type = $2 ORDER BY seq DESC LIMIT 1`, threadID, typ)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return &rows[0], nil
}

func (e events) ListByType(ctx context.Context, threadID, typ string, _ ports.StorageContext) ([]ports.AgentEvent, error) {
	return e.query(ctx, `SELECT `+eventCols+` FROM `+e.s.t("events")+` WHERE "threadId" = $1 AND type = $2 ORDER BY seq`, threadID, typ)
}

type usage struct{ s *Storage }

// usageGroup is the grouped read Total does: one row per agent and model,
// which is exactly one UsageLine plus the two figures that only make sense on
// the whole total.
func (u usage) group() string {
	return `SELECT COALESCE("agentId",''), COALESCE("agentName",''), COALESCE(model,''), COALESCE("modelId",''),
	  COALESCE(SUM("inputTokens"),0)::int, COALESCE(SUM("cachedInputTokens"),0)::int,
	  COALESCE(SUM("cacheWriteInputTokens"),0)::int, COALESCE(SUM("outputTokens"),0)::int,
	  COALESCE(SUM("reasoningTokens"),0)::int, COALESCE(SUM("totalTokens"),0)::int,
	  COUNT(*)::int, COALESCE(SUM(CASE WHEN estimated THEN 1 ELSE 0 END),0)::int,
	  COALESCE(SUM("costMicros"),0)::bigint, COALESCE("costCurrency",''),
	  COALESCE(SUM(CASE WHEN "costMicros" IS NULL THEN 1 ELSE 0 END),0)::int
	FROM ` + u.s.t("usage") + ` WHERE "threadId" = $1`
}

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
	_, err := u.s.db.ExecContext(ctx,
		`INSERT INTO `+u.s.t("usage")+` (id, "threadId", "runId", "agentId", "agentName", kind, step,
		   model, "modelId", "inputTokens", "cachedInputTokens", "cacheWriteInputTokens",
		   "outputTokens", "reasoningTokens", "totalTokens", outcome, estimated,
		   "providerMetadata", "costMicros", "costCurrency", "costSource")
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`,
		core.NewID(), threadID, nullStr(n.RunID), nullStr(n.AgentID), nullStr(n.AgentName),
		string(kind), n.Step, nullStr(n.Model), nullStr(n.ModelID),
		n.InputTokens, n.CacheReadInputTokens, n.CacheWriteInputTokens, n.OutputTokens, n.ReasoningTokens,
		n.TotalTokens(), string(outcome), n.Estimated, meta, micros, currency, source)
	return err
}

func (u usage) Total(ctx context.Context, threadID string, f ports.UsageFilter, _ ports.StorageContext) (ports.UsageTotals, error) {
	q, args := u.group(), []any{threadID}
	if f.RunID != "" {
		q += ` AND "runId" = $2`
		args = append(args, f.RunID)
	}
	// Grouped by currency as well, so a group never sums two units; the
	// groups of one agent and model are merged back into one line below.
	q += ` GROUP BY "agentId", "agentName", model, "modelId", "costCurrency" ORDER BY MIN("createdAt")`
	rows, err := u.s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return ports.UsageTotals{}, err
	}
	defer rows.Close()

	var merge ports.UsageLineMerger
	for rows.Next() {
		var l ports.UsageLine
		var currency string
		var totalTokens, unpriced int
		if err := rows.Scan(&l.AgentID, &l.AgentName, &l.Model, &l.ModelID,
			&l.InputTokens, &l.CacheReadInputTokens, &l.CacheWriteInputTokens,
			&l.OutputTokens, &l.ReasoningTokens, &totalTokens,
			&l.Calls, &l.Estimated, &l.CostMicros, &currency, &unpriced); err != nil {
			return ports.UsageTotals{}, err
		}
		merge.Add(l, currency, totalTokens, unpriced)
	}
	return merge.Totals(), rows.Err()
}
