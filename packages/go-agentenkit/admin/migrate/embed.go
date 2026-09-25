package migrate

import "embed"

// The migrations ship inside the binary, so a deployed service needs nothing
// on disk beside it. One flat directory per dialect: the two databases spell
// enough DDL differently that a shared file would be mostly branches.
//
//go:embed sql/sqlite/*.sql sql/postgres/*.sql
var files embed.FS

// SQLiteMigrations are the admin migrations for SQLite, in apply order.
func SQLiteMigrations() ([]Migration, error) { return withEquivalents(Load(files, "sql/sqlite")) }

// PostgresMigrations are the admin migrations for Postgres, in apply order.
func PostgresMigrations() ([]Migration, error) { return withEquivalents(Load(files, "sql/postgres")) }

// tsChecksums are the checksums the TS runtime records for the steps whose
// released text differs from this runtime's, by version. The 0001 files
// were written apart and differ only in their header comment; the SQL is
// the same. A released file is never edited, so instead each runtime
// accepts the other's checksum, and one admin database serves both.
var tsChecksums = map[string][]string{
	"0001_init": {
		"25850a48728ef7ed8eaedd6b3659a54c8dc1d5167de6bcbe79808b214d453bfb", // postgres
		"9b61b4c0ae1e3cc0c3dc457b1ddb6afbaad1a2d136d3378e4675b10f51a9f341", // sqlite
	},
}

func withEquivalents(ms []Migration, err error) ([]Migration, error) {
	for i := range ms {
		ms[i].Equivalent = tsChecksums[ms[i].Version]
	}
	return ms, err
}
