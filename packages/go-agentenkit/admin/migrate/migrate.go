// Package migrate applies the platform's OWN schema to the admin store (§2.9),
// from numbered .sql files.
//
// Only the admin store is migrated here. A caller's Storage is their database
// and their schema; the platform never touches it.
//
// The rules, in one place:
//
//   - A migration is a file named "NNNN_what_it_does.sql". Filename order is
//     apply order.
//   - A file that has been released is never edited. Its checksum is recorded
//     when it runs, and a later mismatch is an error, because a database that
//     no longer matches the code is exactly the thing an operational store is
//     supposed to tell you about.
//   - 0001 is the schema as it stood before this package existed, written so
//     that running it against a database already holding those tables changes
//     nothing. That is what lets an existing install pick the migrator up
//     without a separate baseline step, and what repairs one that stopped on
//     an older release.
package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

// Migration is one versioned step, named by its file without the extension.
type Migration struct {
	Version  string
	SQL      string
	Checksum string
}

// Load reads every .sql file in dir, in filename order. Nested directories
// are ignored: one flat directory per dialect keeps apply order obvious.
func Load(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("migrations in %s: %w", dir, err)
	}
	out := make([]Migration, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		raw, err := fs.ReadFile(fsys, dir+"/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("migration %s: %w", e.Name(), err)
		}
		// Hashed with carriage returns stripped: a Windows checkout can
		// rewrite line endings, and a checksum that changed with the
		// platform would reject every upgrade on it.
		body := strings.ReplaceAll(string(raw), "\r\n", "\n")
		sum := sha256.Sum256([]byte(body))
		out = append(out, Migration{
			Version:  strings.TrimSuffix(e.Name(), ".sql"),
			SQL:      body,
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Dialect is the little that differs between databases.
type Dialect struct {
	Name string
	// Ledger creates the table recording what has run.
	Ledger string
	// Insert records one applied migration, with two bind parameters.
	Insert string
	// Lock is run first inside each migration's transaction so two workers
	// starting together cannot both apply the same file. Empty for a
	// database that serialises writers by itself.
	Lock string
	// Placeholder is how this database spells a bind parameter.
	Placeholder string
	// Repair runs once BEFORE the migration files, inside their transaction. It exists for the one
	// thing portable SQL cannot express: SQLite has no ADD COLUMN IF NOT
	// EXISTS, so a database left behind by an older release needs a PRAGMA
	// check instead.
	//
	// Before, not after, because the baseline creates indexes over columns
	// that such a database may not have yet — and an index on a column that
	// does not exist is an error, not a no-op.
	//
	// It must be idempotent, and it must tolerate a database where none of
	// the tables exist: on a fresh one it runs before anything is created.
	Repair func(context.Context, Execer) error
}

// Execer is the part of a database handle a repair step needs. Both *sql.DB
// and *sql.Tx satisfy it.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// SQLite needs no lock: the driver holds one connection and the database
// takes one writer at a time.
var SQLite = Dialect{
	Name: "sqlite",
	Ledger: `CREATE TABLE IF NOT EXISTS agentic_migrations (
	  version TEXT PRIMARY KEY, checksum TEXT NOT NULL, appliedAt INTEGER NOT NULL)`,
	Insert: `INSERT INTO agentic_migrations (version, checksum, appliedAt)
	         VALUES (?, ?, CAST(strftime('%s','now') AS INTEGER) * 1000)`,
	Placeholder: "?",
}

// Postgres takes a transaction-scoped advisory lock, so several workers
// starting at once queue rather than racing on the same DDL. The key is
// arbitrary and constant: it only has to be the same in every process.
var Postgres = Dialect{
	Name: "postgres",
	Ledger: `CREATE TABLE IF NOT EXISTS agentic_migrations (
	  version TEXT PRIMARY KEY, checksum TEXT NOT NULL,
	  "appliedAt" TIMESTAMPTZ NOT NULL DEFAULT now())`,
	Insert: `INSERT INTO agentic_migrations (version, checksum) VALUES ($1, $2)`,
	// The key is arbitrary and constant; it only has to be the same in every
	// process migrating this database.
	Lock:        `SELECT pg_advisory_xact_lock(4171939288)`,
	Placeholder: "$1",
}

// Run applies every migration the ledger does not already carry, in order.
// Safe to call from several processes at once.
//
// The whole run is ONE transaction, taken under the dialect's lock: the
// ledger, the repair step and every file commit together or not at all. Both
// databases roll DDL back with the transaction, so a run that fails part way
// leaves nothing behind to confuse the next attempt.
//
// The lock has to cover the ledger too. Postgres's CREATE TABLE IF NOT EXISTS
// is not atomic against another session creating the same table: both see it
// missing, both create it, and one gets a duplicate-type error. Two workers
// starting together is the normal case here, so that is not a rare race.
//
// One consequence worth knowing: a migration that cannot run inside a
// transaction (CREATE INDEX CONCURRENTLY, say) does not belong in a file here.
func Run(ctx context.Context, db *sql.DB, d Dialect, ms []Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s migrations: %w", d.Name, err)
	}
	defer func() { _ = tx.Rollback() }()

	if d.Lock != "" {
		if _, err := tx.ExecContext(ctx, d.Lock); err != nil {
			return fmt.Errorf("%s migrations: lock: %w", d.Name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, d.Ledger); err != nil {
		return fmt.Errorf("%s migrations: ledger: %w", d.Name, err)
	}
	if d.Repair != nil {
		if err := d.Repair(ctx, tx); err != nil {
			return fmt.Errorf("%s migrations: repair: %w", d.Name, err)
		}
	}

	applied, err := appliedVersions(ctx, tx)
	if err != nil {
		return fmt.Errorf("%s migrations: %w", d.Name, err)
	}
	for _, m := range ms {
		if sum, ok := applied[m.Version]; ok {
			if sum != m.Checksum {
				return fmt.Errorf(
					"%s migrations: %s changed after it was applied: the database no longer matches the code",
					d.Name, m.Version)
			}
			continue
		}
		for _, stmt := range Statements(m.SQL) {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("%s migrations: %s: %w (in: %s)", d.Name, m.Version, err, firstLine(stmt))
			}
		}
		if _, err := tx.ExecContext(ctx, d.Insert, m.Version, m.Checksum); err != nil {
			return fmt.Errorf("%s migrations: %s: %w", d.Name, m.Version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s migrations: %w", d.Name, err)
	}
	return nil
}

func appliedVersions(ctx context.Context, tx *sql.Tx) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT version, checksum FROM agentic_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var version, checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, err
		}
		out[version] = checksum
	}
	return out, rows.Err()
}

// Statements splits a migration into the statements to run one at a time,
// because neither driver reliably takes several at once.
//
// Line comments are stripped BEFORE the split, so a semicolon inside one
// cannot cut a statement in half. A semicolon inside a string literal still
// would: migrations must not contain one.
func Statements(sqlText string) []string {
	var body strings.Builder
	for _, line := range strings.Split(sqlText, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		body.WriteString(line)
		body.WriteString("\n")
	}
	out := []string{}
	for _, stmt := range strings.Split(body.String(), ";") {
		if trimmed := strings.TrimSpace(stmt); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func firstLine(stmt string) string {
	if i := strings.IndexByte(stmt, '\n'); i >= 0 {
		return strings.TrimSpace(stmt[:i]) + " …"
	}
	return stmt
}
