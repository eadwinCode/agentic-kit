package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Kv is a Kv over Postgres: the hot state, the run locks, the HITL handoff
// keys and the per-thread seq counters (§3.4), in one table beside the
// storage tables so a single database runs the whole platform.
//
// Expiry is enforced on read. SET NX and Incr are single statements, so two
// workers can never both win a lock or both take the same seq.
type Kv struct {
	db    *sql.DB
	table string
}

// NewKv creates the table if it is missing and returns the Kv. The prefix
// matches the storage's (default "agentenkit_").
func NewKv(ctx context.Context, db *sql.DB, opts ...Option) (*Kv, error) {
	s := &Storage{prefix: "agentenkit_"}
	for _, o := range opts {
		o(s)
	}
	k := &Kv{db: db, table: s.prefix + "kv"}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS ` + k.table + ` (
		   key TEXT PRIMARY KEY, value TEXT NOT NULL, "expiresAt" TIMESTAMPTZ)`,
		`CREATE INDEX IF NOT EXISTS ` + k.table + `_expires ON ` + k.table + `("expiresAt") WHERE "expiresAt" IS NOT NULL`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("postgres kv schema: %w", err)
		}
	}
	return k, nil
}

func (k *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := k.db.QueryRowContext(ctx,
		`SELECT value FROM `+k.table+` WHERE key = $1 AND ("expiresAt" IS NULL OR "expiresAt" > now())`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func expiry(d time.Duration) sql.NullTime {
	if d <= 0 {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: time.Now().Add(d), Valid: true}
}

func (k *Kv) Set(ctx context.Context, key, value string, opts ports.SetOptions) (bool, error) {
	if !opts.OnlyIfNotExists {
		_, err := k.db.ExecContext(ctx,
			`INSERT INTO `+k.table+` (key, value, "expiresAt") VALUES ($1, $2, $3)
			 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "expiresAt" = EXCLUDED."expiresAt"`,
			key, value, expiry(opts.Expiry))
		return err == nil, err
	}
	// SET NX in one statement (§3.4): the insert wins on a missing key, the
	// update wins only over an expired row, and a live row updates nothing.
	res, err := k.db.ExecContext(ctx,
		`INSERT INTO `+k.table+` AS kv (key, value, "expiresAt") VALUES ($1, $2, $3)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "expiresAt" = EXCLUDED."expiresAt"
		 WHERE kv."expiresAt" IS NOT NULL AND kv."expiresAt" <= now()`,
		key, value, expiry(opts.Expiry))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (k *Kv) Del(ctx context.Context, key string) error {
	_, err := k.db.ExecContext(ctx, `DELETE FROM `+k.table+` WHERE key = $1`, key)
	return err
}

func (k *Kv) Incr(ctx context.Context, key string) (int64, error) {
	// Atomic: the increment happens inside the conflict clause. An expired
	// counter starts again from 1 and loses its expiry, like Redis INCR.
	var value string
	err := k.db.QueryRowContext(ctx,
		`INSERT INTO `+k.table+` AS kv (key, value, "expiresAt") VALUES ($1, '1', NULL)
		 ON CONFLICT (key) DO UPDATE SET
		   value = CASE WHEN kv."expiresAt" IS NOT NULL AND kv."expiresAt" <= now() THEN '1'
		                ELSE (kv.value::bigint + 1)::text END,
		   "expiresAt" = CASE WHEN kv."expiresAt" IS NOT NULL AND kv."expiresAt" <= now() THEN NULL ELSE kv."expiresAt" END
		 RETURNING value`, key).Scan(&value)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

// DeleteExpired drops rows past their expiry. Expiry is already enforced on
// read, so this is housekeeping: call it from a periodic job.
func (k *Kv) DeleteExpired(ctx context.Context) (int64, error) {
	res, err := k.db.ExecContext(ctx, `DELETE FROM `+k.table+` WHERE "expiresAt" IS NOT NULL AND "expiresAt" <= now()`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
