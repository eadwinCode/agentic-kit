package migrate

import "embed"

// The migrations ship inside the binary, so a deployed service needs nothing
// on disk beside it. One flat directory per dialect: the two databases spell
// enough DDL differently that a shared file would be mostly branches.
//
//go:embed sql/sqlite/*.sql sql/postgres/*.sql
var files embed.FS

// SQLiteMigrations are the admin migrations for SQLite, in apply order.
func SQLiteMigrations() ([]Migration, error) { return Load(files, "sql/sqlite") }

// PostgresMigrations are the admin migrations for Postgres, in apply order.
func PostgresMigrations() ([]Migration, error) { return Load(files, "sql/postgres") }
