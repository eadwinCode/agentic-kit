package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Kv is a Kv over SQLite, for a local setup whose storage is durable too.
//
// The event sequence counter (§3.4) lives in the Kv. With an in-memory Kv
// beside a SQLite Storage, a restart resets the counter while the log keeps
// its numbers, so new events reuse old sequence numbers and every client's
// cursor drops them. Keeping the Kv in the same file as the log removes
// that failure entirely: one file, one lifetime.
//
// Expiry is enforced on read. Incr and SET NX are single statements, so
// concurrent callers never collide on a counter or a lock.
type Kv struct{ db *sql.DB }

// KvSchema is the DDL for the Kv table.
var KvSchema = []string{
	`CREATE TABLE IF NOT EXISTS kv (
	  key TEXT PRIMARY KEY, value TEXT NOT NULL, expiresAt INTEGER)`,
}

// NewKv creates the table if missing and returns the Kv.
func NewKv(db *sql.DB) (*Kv, error) {
	for _, stmt := range KvSchema {
		if _, err := db.Exec(stmt); err != nil {
			return nil, fmt.Errorf("sqlite kv schema: %w", err)
		}
	}
	return &Kv{db: db}, nil
}

func nowMs() int64 { return time.Now().UnixMilli() }

func (k *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := k.db.QueryRowContext(ctx,
		`SELECT value FROM kv WHERE key = ? AND (expiresAt IS NULL OR expiresAt > ?)`, key, nowMs()).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func (k *Kv) Set(ctx context.Context, key, value string, opts ports.SetOptions) (bool, error) {
	var expires sql.NullInt64
	if opts.Expiry > 0 {
		expires = sql.NullInt64{Int64: nowMs() + opts.Expiry.Milliseconds(), Valid: true}
	}
	if !opts.OnlyIfNotExists {
		_, err := k.db.ExecContext(ctx,
			`INSERT INTO kv (key, value, expiresAt) VALUES (?, ?, ?)
			 ON CONFLICT(key) DO UPDATE SET value = excluded.value, expiresAt = excluded.expiresAt`,
			key, value, expires)
		return err == nil, err
	}
	// SET NX: one statement, so exactly one caller can win (§3.4). An expired
	// row counts as absent.
	res, err := k.db.ExecContext(ctx,
		`INSERT INTO kv (key, value, expiresAt) VALUES (?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value, expiresAt = excluded.expiresAt
		 WHERE kv.expiresAt IS NOT NULL AND kv.expiresAt <= ?`,
		key, value, expires, nowMs())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (k *Kv) Del(ctx context.Context, key string) error {
	_, err := k.db.ExecContext(ctx, `DELETE FROM kv WHERE key = ?`, key)
	return err
}

func (k *Kv) Incr(ctx context.Context, key string) (int64, error) {
	// An expired counter starts again from 1; a live one advances. The
	// expiry, if any, is kept, like Redis INCR.
	var value string
	err := k.db.QueryRowContext(ctx,
		`INSERT INTO kv (key, value, expiresAt) VALUES (?, '1', NULL)
		 ON CONFLICT(key) DO UPDATE SET
		   value = CASE WHEN kv.expiresAt IS NOT NULL AND kv.expiresAt <= ?2 THEN '1'
		                ELSE CAST(CAST(kv.value AS INTEGER) + 1 AS TEXT) END,
		   expiresAt = CASE WHEN kv.expiresAt IS NOT NULL AND kv.expiresAt <= ?2 THEN NULL ELSE kv.expiresAt END
		 RETURNING value`, key, nowMs()).Scan(&value)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}
