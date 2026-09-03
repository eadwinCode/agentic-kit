// Package admin picks the operational store SetupAgentCore uses when none is
// configured (§2.9).
package admin

import (
	"context"
	"log/slog"
	"os"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	postgresadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/postgres"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// DefaultAdminDB is where the default store writes when no Postgres URL is set.
const DefaultAdminDB = "agentic-kit-admin.sqlite"

// OpenDefaultAdminStore is the store SetupAgentCore uses when none is
// configured (§2.9).
//
// AGENTIC_KIT_ADMIN_DATABASE_URL selects Postgres: point it at its own
// database or the one you already have, the agentic_ prefix keeps them
// apart. Without it, SQLite on disk at AGENTIC_KIT_ADMIN_DB.
//
// Like the TypeScript package, it uses whichever driver the process has:
// register a database/sql driver (modernc.org/sqlite or mattn/go-sqlite3;
// pgx/stdlib or lib/pq) by importing it.
//
// The connection is opened eagerly, so a store that cannot be opened fails at
// startup rather than losing every run record. The SCHEMA is brought up to
// date behind it (§2.9): a service starts at the same speed whether or not it
// has migrating to do, and the first admin call waits for the schema rather
// than meeting a table that is not there yet.
func OpenDefaultAdminStore(ctx context.Context, log *slog.Logger) (ports.AdminStore, error) {
	if url := os.Getenv("AGENTIC_KIT_ADMIN_DATABASE_URL"); url != "" {
		db, err := postgresadmin.Open(url)
		if err != nil {
			return nil, err
		}
		return postgresadmin.ConnectWithLogger(ctx, db, log)
	}
	file := os.Getenv("AGENTIC_KIT_ADMIN_DB")
	if file == "" {
		file = DefaultAdminDB
	}
	db, err := sqlite.Open(file)
	if err != nil {
		return nil, err
	}
	return sqliteadmin.NewContext(ctx, db, log)
}
