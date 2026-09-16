package migrate_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"testing/fstest"

	_ "modernc.org/sqlite"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/migrate"
	sqliteadmin "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/sqlite"
)

func open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	// One connection: an in-memory database is per connection.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func run(t *testing.T, db *sql.DB, ms []migrate.Migration) error {
	t.Helper()
	return migrate.Run(context.Background(), db, migrate.SQLite, ms)
}

func versions(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT version FROM agentic_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

func load(t *testing.T, files map[string]string) []migrate.Migration {
	t.Helper()
	fsys := fstest.MapFS{}
	for name, body := range files {
		fsys["m/"+name] = &fstest.MapFile{Data: []byte(body)}
	}
	ms, err := migrate.Load(fsys, "m")
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

func TestRun_AppliesInFilenameOrderAndRecordsEach(t *testing.T) {
	db := open(t)
	ms := load(t, map[string]string{
		"0002_second.sql": `CREATE TABLE b (id TEXT);`,
		"0001_first.sql":  `CREATE TABLE a (id TEXT);`,
		"notes.md":        `not a migration`,
	})
	if len(ms) != 2 {
		t.Fatalf("loaded %d migrations, want 2 (the .md is not one)", len(ms))
	}
	if ms[0].Version != "0001_first" {
		t.Fatalf("apply order is filename order: got %s first", ms[0].Version)
	}
	if err := run(t, db, ms); err != nil {
		t.Fatal(err)
	}
	got := versions(t, db)
	if len(got) != 2 || got[0] != "0001_first" || got[1] != "0002_second" {
		t.Fatalf("ledger: %v", got)
	}
}

func TestRun_IsIdempotent(t *testing.T) {
	db := open(t)
	// Deliberately NOT idempotent SQL: running it twice would fail. Only the
	// ledger stops that, which is the point.
	ms := load(t, map[string]string{"0001_init.sql": `CREATE TABLE a (id TEXT);`})
	for i := range 3 {
		if err := run(t, db, ms); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	if got := versions(t, db); len(got) != 1 {
		t.Fatalf("recorded %v, want one row however often it runs", got)
	}
}

func TestRun_RollsBackAFileThatFailsPartWay(t *testing.T) {
	db := open(t)
	ms := load(t, map[string]string{
		"0001_bad.sql": "CREATE TABLE a (id TEXT);\nTHIS IS NOT SQL;",
	})
	if err := run(t, db, ms); err == nil {
		t.Fatal("want an error from the broken statement")
	}
	// The whole run is one transaction, so a failure leaves a clean slate:
	// not the table the first statement made, and not the ledger either. The
	// next attempt starts from scratch rather than from half a migration.
	if _, err := db.Query(`SELECT * FROM a`); err == nil {
		t.Fatal("the first statement was not rolled back")
	}
	if _, err := db.Query(`SELECT * FROM agentic_migrations`); err == nil {
		t.Fatal("the ledger outlived the transaction that made it")
	}
	// And the next attempt, with the file fixed, works.
	if err := run(t, db, load(t, map[string]string{"0001_bad.sql": `CREATE TABLE a (id TEXT);`})); err != nil {
		t.Fatalf("retry after a failed migration: %v", err)
	}
	if got := versions(t, db); len(got) != 1 {
		t.Fatalf("ledger after the retry: %v", got)
	}
}

func TestRun_RefusesAMigrationEditedAfterItWasApplied(t *testing.T) {
	db := open(t)
	if err := run(t, db, load(t, map[string]string{"0001_init.sql": `CREATE TABLE a (id TEXT);`})); err != nil {
		t.Fatal(err)
	}
	// Same version, different content: the database no longer matches the
	// code, and saying so is the whole job of an operational store.
	err := run(t, db, load(t, map[string]string{"0001_init.sql": `CREATE TABLE a (id TEXT, extra TEXT);`}))
	if err == nil || !strings.Contains(err.Error(), "changed after it was applied") {
		t.Fatalf("got %v, want a checksum complaint", err)
	}
}

func TestRun_LineEndingsDoNotChangeTheChecksum(t *testing.T) {
	db := open(t)
	unix := load(t, map[string]string{"0001_init.sql": "CREATE TABLE a (id TEXT);\n"})
	if err := run(t, db, unix); err != nil {
		t.Fatal(err)
	}
	// The same file checked out on Windows. It must not read as edited, or
	// every upgrade there would be refused.
	windows := load(t, map[string]string{"0001_init.sql": "CREATE TABLE a (id TEXT);\r\n"})
	if err := run(t, db, windows); err != nil {
		t.Fatalf("carriage returns changed the checksum: %v", err)
	}
}

func TestStatements_SplitsAroundCommentsAndBlankLines(t *testing.T) {
	got := migrate.Statements(`
-- a comment; with a semicolon in it
CREATE TABLE a (id TEXT);

CREATE INDEX i ON a(id);
`)
	if len(got) != 2 {
		t.Fatalf("got %d statements, want 2: %q", len(got), got)
	}
	if !strings.HasPrefix(got[0], "CREATE TABLE") || !strings.HasPrefix(got[1], "CREATE INDEX") {
		t.Fatalf("got %q", got)
	}
}

func TestShippedMigrationsLoad(t *testing.T) {
	for _, load := range []struct {
		name string
		fn   func() ([]migrate.Migration, error)
	}{
		{"sqlite", migrate.SQLiteMigrations},
		{"postgres", migrate.PostgresMigrations},
	} {
		ms, err := load.fn()
		if err != nil {
			t.Fatalf("%s: %v", load.name, err)
		}
		if len(ms) == 0 {
			t.Fatalf("%s: no migrations embedded", load.name)
		}
		if ms[0].Version != "0001_init" {
			t.Fatalf("%s: first migration is %s", load.name, ms[0].Version)
		}
	}
}

// The shape agentic_steps had before threadId, text and toolCalls were added:
// a database last opened by that release, upgrading straight to the migrator.
const oldStepsTable = `CREATE TABLE agentic_steps (
  runId TEXT NOT NULL, agentId TEXT, "index" INTEGER NOT NULL,
  durationMs INTEGER NOT NULL, finishReason TEXT NOT NULL,
  inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
  outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
  tools TEXT, at INTEGER NOT NULL)`

func TestSQLiteBaselineRepairsADatabaseFromAnOlderRelease(t *testing.T) {
	db := open(t)
	if _, err := db.Exec(oldStepsTable); err != nil {
		t.Fatal(err)
	}
	store, err := sqliteadmin.NewContext(context.Background(), db, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The first call waits for the migration, so this is also the gate under
	// test: nothing here knows or cares that it ran in the background.
	if _, err := store.Steps().ListByThread(context.Background(), "t1"); err != nil {
		// CREATE TABLE IF NOT EXISTS leaves the old table alone, so the
		// columns it never got have to be added BEFORE the baseline indexes
		// one of them. Getting that order wrong fails right here.
		t.Fatalf("reading a repaired database: %v", err)
	}
	// Every embedded migration ran, the baseline first.
	all, err := migrate.SQLiteMigrations()
	if err != nil {
		t.Fatal(err)
	}
	got := versions(t, db)
	if len(got) != len(all) || got[0] != "0001_init" {
		t.Fatalf("ledger: %v", got)
	}
}

func TestGateReportsAFailedMigrationToEveryCaller(t *testing.T) {
	db := open(t)
	// A table in the way that the baseline cannot reconcile: the migration
	// fails, and the failure has to reach the caller rather than leaving them
	// querying a schema that was never built.
	if _, err := db.Exec(`CREATE TABLE agentic_migrations (nonsense TEXT)`); err != nil {
		t.Fatal(err)
	}
	store, err := sqliteadmin.NewContext(context.Background(), db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Runs().Get(context.Background(), "r1"); err == nil {
		t.Fatal("want the migration failure surfaced on the first call")
	}
}
