package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/migrate"
)

// migrateSchema brings a component's tables up to date, once (§3.4). The
// storage, the kv and the queue each list their steps; this runs the ones
// the prefix's ledger (<prefix>migrations) does not have yet, in one
// transaction, under the same advisory lock the admin store migrates under.
//
// Before this, every process ran its CREATE and ALTER TABLE statements on
// every start. An ALTER TABLE takes a lock on the whole table even when the
// column is already there, so a fleet restarting stalled every write behind
// it. Now a start that has nothing to do reads the ledger and moves on.
func migrateSchema(ctx context.Context, db *sql.DB, prefix, what string, steps []migrate.Migration) error {
	d := migrate.Postgres
	ledger := prefix + "migrations"
	d.Ledger = `CREATE TABLE IF NOT EXISTS ` + ledger + ` (
	  version TEXT PRIMARY KEY, checksum TEXT NOT NULL,
	  "appliedAt" TIMESTAMPTZ NOT NULL DEFAULT now())`
	d.Insert = `INSERT INTO ` + ledger + ` (version, checksum) VALUES ($1, $2)`
	d.Applied = `SELECT version, checksum FROM ` + ledger
	d.Skipped = func(version string, err error) {
		slog.Warn("postgres "+what+" schema step skipped; tried again on the next start", "version", version, "err", err)
	}
	if err := migrate.Run(ctx, db, d, steps); err != nil {
		return fmt.Errorf("postgres %s schema: %w", what, err)
	}
	return nil
}

// optional marks a step that may fail on rows it cannot change.
func optional(m migrate.Migration) migrate.Migration {
	m.Optional = true
	return m
}
