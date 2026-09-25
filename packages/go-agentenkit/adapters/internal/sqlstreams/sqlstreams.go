// Package sqlstreams is RunStreams over two SQL tables, shared by the
// SQLite and Postgres adapters. <prefix>streams has one row per stream: who
// it belongs to, whether it is closed, its end event and when it expires.
// <prefix>stream_events has one row per event, keyed by an ever-increasing
// integer that is the offset. The TS SQLite adapter uses the same tables.
package sqlstreams

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Dialect is what differs between the databases.
type Dialect struct {
	// Placeholder is the n-th (1-based) bind parameter: "?" or "$n".
	Placeholder func(n int) string
	// Time turns a time into the value the expiresAt column holds, and Now
	// is the SQL for the current time in that form.
	Time func(t time.Time) any
	Now  string
	// ForUpdate locks the stream row inside a transaction ("" where the
	// whole database is locked anyway).
	ForUpdate string
	// Notify, when set, tells other processes that a stream changed. It
	// runs inside the write's transaction.
	Notify func(ctx context.Context, tx *sql.Tx, streamID string) error
}

// Streams implements ports.RunStreams.
type Streams struct {
	DB      *sql.DB
	D       Dialect
	Streams string // table names, prefixed
	Events  string
	// Poll is the longest a reader waits before reading again, news or
	// not: the floor under a missed wake-up from another process.
	Poll time.Duration

	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
	swept   time.Time
}

var _ ports.RunStreams = (*Streams)(nil)

// Schema is the DDL for the two tables. idType is the type of an
// ever-increasing integer key, timeType the expiresAt column's.
func Schema(streams, events, idType, boolType, timeType string) []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS ` + streams + ` (
		   id TEXT PRIMARY KEY, "threadId" TEXT NOT NULL, "runId" TEXT NOT NULL,
		   closed ` + boolType + ` NOT NULL, "endEvent" TEXT, "expiresAt" ` + timeType + ` NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS ` + streams + `_expires ON ` + streams + `("expiresAt")`,
		`CREATE INDEX IF NOT EXISTS ` + streams + `_thread ON ` + streams + `("threadId")`,
		`CREATE TABLE IF NOT EXISTS ` + events + ` (
		   pos ` + idType + `, "streamId" TEXT NOT NULL, event TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS ` + events + `_stream ON ` + events + `("streamId", pos)`,
	}
}

func (s *Streams) p(n int) string { return s.D.Placeholder(n) }

// Wake wakes this process's readers of a stream. The Postgres adapter also
// calls it for a NOTIFY from another process.
func (s *Streams) Wake(streamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.waiters[streamID] {
		close(ch)
	}
	delete(s.waiters, streamID)
}

// arm registers a wake-up for a stream before the reader reads, so an
// append that lands between the read and the wait still wakes it.
func (s *Streams) arm(streamID string) chan struct{} {
	ch := make(chan struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waiters == nil {
		s.waiters = map[string]map[chan struct{}]struct{}{}
	}
	if s.waiters[streamID] == nil {
		s.waiters[streamID] = map[chan struct{}]struct{}{}
	}
	s.waiters[streamID][ch] = struct{}{}
	return ch
}

func (s *Streams) disarm(streamID string, ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w := s.waiters[streamID]; w != nil {
		delete(w, ch)
		if len(w) == 0 {
			delete(s.waiters, streamID)
		}
	}
}

// wait waits for a wake-up on ch, the poll interval, or ctx.
func (s *Streams) wait(ctx context.Context, ch chan struct{}) {
	timer := time.NewTimer(s.Poll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-ch:
	case <-timer.C:
	}
}

// sweep deletes streams past their expiry, 500 a pass and at most once a
// minute, so one pass never holds a lock for long.
func (s *Streams) sweep(ctx context.Context) error {
	s.mu.Lock()
	if time.Since(s.swept) < time.Minute {
		s.mu.Unlock()
		return nil
	}
	s.swept = time.Now()
	s.mu.Unlock()
	rows, err := s.DB.QueryContext(ctx,
		`SELECT id FROM `+s.Streams+` WHERE "expiresAt" <= `+s.D.Now+` LIMIT 500`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if err := s.remove(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Streams) remove(ctx context.Context, streamID string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+s.Events+` WHERE "streamId" = `+s.p(1), streamID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+s.Streams+` WHERE id = `+s.p(1), streamID); err != nil {
		return err
	}
	if s.D.Notify != nil {
		if err := s.D.Notify(ctx, tx, streamID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Streams) Open(ctx context.Context, streamID string, meta ports.StreamMeta, ttl time.Duration) error {
	if err := s.sweep(ctx); err != nil {
		return err
	}
	// A row past its expiry is gone: replace it rather than reopen it.
	if _, err := s.DB.ExecContext(ctx,
		`DELETE FROM `+s.Streams+` WHERE id = `+s.p(1)+` AND "expiresAt" <= `+s.D.Now, streamID); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx,
		`INSERT INTO `+s.Streams+` (id, "threadId", "runId", closed, "expiresAt")
		 VALUES (`+s.p(1)+`, `+s.p(2)+`, `+s.p(3)+`, false, `+s.p(4)+`)
		 ON CONFLICT (id) DO NOTHING`,
		streamID, meta.ThreadID, meta.RunID, s.D.Time(time.Now().Add(ttl)))
	return err
}

// lock reads the stream's row inside tx: whether it is live and closed.
func (s *Streams) lock(ctx context.Context, tx *sql.Tx, streamID string) (live, closed bool, err error) {
	err = tx.QueryRowContext(ctx,
		`SELECT closed, "expiresAt" > `+s.D.Now+` FROM `+s.Streams+` WHERE id = `+s.p(1)+s.D.ForUpdate,
		streamID).Scan(&closed, &live)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	return live, closed, err
}

func (s *Streams) insert(ctx context.Context, tx *sql.Tx, streamID string, events []ports.StreamEvent) ([]string, error) {
	var sb strings.Builder
	args := []any{streamID}
	sb.WriteString(`INSERT INTO ` + s.Events + ` ("streamId", event) VALUES `)
	for i, e := range events {
		b, err := ports.EncodeStreamEvent(e)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		args = append(args, string(b))
		sb.WriteString("(" + s.p(1) + ", " + s.p(len(args)) + ")")
	}
	sb.WriteString(" RETURNING pos")
	rows, err := tx.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pos []int64
	for rows.Next() {
		var p int64
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		pos = append(pos, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// The rows took their keys in the order they were listed.
	sort.Slice(pos, func(i, j int) bool { return pos[i] < pos[j] })
	out := make([]string, len(pos))
	for i, p := range pos {
		out[i] = strconv.FormatInt(p, 10)
	}
	return out, nil
}

func (s *Streams) Append(ctx context.Context, streamID string, events []ports.StreamEvent) ([]string, error) {
	if len(events) == 0 {
		return nil, nil
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	live, closed, err := s.lock(ctx, tx, streamID)
	if err != nil {
		return nil, err
	}
	if !live {
		return nil, ports.ErrStreamGone
	}
	if closed {
		return nil, ports.ErrStreamClosed
	}
	offsets, err := s.insert(ctx, tx, streamID, events)
	if err != nil {
		return nil, err
	}
	if s.D.Notify != nil {
		if err := s.D.Notify(ctx, tx, streamID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.Wake(streamID)
	return offsets, nil
}

func (s *Streams) Close(ctx context.Context, streamID string, end ports.StreamEnd, grace time.Duration) error {
	b, err := ports.EncodeStreamEvent(end)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	live, closed, err := s.lock(ctx, tx, streamID)
	if err != nil {
		return err
	}
	if !live {
		return ports.ErrStreamGone
	}
	if closed {
		return nil
	}
	if _, err := s.insert(ctx, tx, streamID, []ports.StreamEvent{end}); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE `+s.Streams+` SET closed = true, "endEvent" = `+s.p(1)+`, "expiresAt" = `+s.p(2)+` WHERE id = `+s.p(3),
		string(b), s.D.Time(time.Now().Add(grace)), streamID); err != nil {
		return err
	}
	if s.D.Notify != nil {
		if err := s.D.Notify(ctx, tx, streamID); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.Wake(streamID)
	return nil
}

func (s *Streams) Delete(ctx context.Context, streamID string) error {
	if err := s.remove(ctx, streamID); err != nil {
		return err
	}
	s.Wake(streamID)
	return nil
}

type page struct {
	meta   ports.StreamMeta
	closed bool
	end    ports.StreamEnd
	items  []ports.StreamItem
}

func (s *Streams) page(ctx context.Context, streamID, after string) (*page, error) {
	var (
		p       page
		endJSON sql.NullString
		live    bool
	)
	err := s.DB.QueryRowContext(ctx,
		`SELECT "threadId", "runId", closed, "endEvent", "expiresAt" > `+s.D.Now+` FROM `+s.Streams+` WHERE id = `+s.p(1),
		streamID).Scan(&p.meta.ThreadID, &p.meta.RunID, &p.closed, &endJSON, &live)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !live) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if p.closed && endJSON.Valid {
		e, err := ports.DecodeStreamEvent([]byte(endJSON.String))
		if err != nil {
			return nil, err
		}
		p.end, _ = e.(ports.StreamEnd)
	}
	from, _ := strconv.ParseInt(after, 10, 64)
	rows, err := s.DB.QueryContext(ctx,
		`SELECT pos, event FROM `+s.Events+` WHERE "streamId" = `+s.p(1)+` AND pos > `+s.p(2)+` ORDER BY pos`,
		streamID, from)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			pos int64
			raw string
		)
		if err := rows.Scan(&pos, &raw); err != nil {
			return nil, err
		}
		e, err := ports.DecodeStreamEvent([]byte(raw))
		if err != nil {
			return nil, err
		}
		p.items = append(p.items, ports.StreamItem{Offset: strconv.FormatInt(pos, 10), Event: e})
	}
	return &p, rows.Err()
}

func (s *Streams) Read(ctx context.Context, streamID, after string) iter.Seq2[ports.StreamItem, error] {
	return func(yield func(ports.StreamItem, error) bool) {
		cursor := after
		for {
			if ctx.Err() != nil {
				return
			}
			ch := s.arm(streamID)
			p, err := s.page(ctx, streamID, cursor)
			if err != nil || p == nil || len(p.items) > 0 {
				s.disarm(streamID, ch)
			}
			if err != nil {
				if ctx.Err() == nil {
					yield(ports.StreamItem{}, err)
				}
				return
			}
			if p == nil {
				yield(ports.StreamItem{}, ports.ErrStreamGone)
				return
			}
			for _, item := range p.items {
				if !yield(item, nil) {
					return
				}
				cursor = item.Offset
				if ports.IsStreamEnd(item.Event) {
					return
				}
			}
			// Read from past the end item: nothing more will ever come.
			if p.closed && len(p.items) == 0 {
				s.disarm(streamID, ch)
				return
			}
			if len(p.items) == 0 {
				s.wait(ctx, ch)
				s.disarm(streamID, ch)
			}
		}
	}
}

func (s *Streams) Snapshot(ctx context.Context, streamID, after string) (*ports.StreamSnapshot, error) {
	p, err := s.page(ctx, streamID, after)
	if err != nil || p == nil {
		return nil, err
	}
	return &ports.StreamSnapshot{Meta: p.meta, Items: p.items, End: p.end}, nil
}
