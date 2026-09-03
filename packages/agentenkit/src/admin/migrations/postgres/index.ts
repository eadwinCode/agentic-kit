import type { Migration, MigrationDialect } from '../runner.js';
import { sql as init } from './0001-init.js';

/** The admin migrations for Postgres, in apply order. Append only. */
export const migrations: Migration[] = [{ version: '0001_init', sql: init }];

/** Postgres takes a transaction-scoped advisory lock, so several workers
 *  starting at once queue rather than racing on the same DDL. The key is
 *  arbitrary and constant: it only has to be the same in every process
 *  migrating this database, and it matches the Go package's. */
export const dialect: MigrationDialect = {
  name: 'postgres',
  ledger: `CREATE TABLE IF NOT EXISTS agentic_migrations (
    version TEXT PRIMARY KEY, checksum TEXT NOT NULL,
    "appliedAt" TIMESTAMPTZ NOT NULL DEFAULT now())`,
  insert: 'INSERT INTO agentic_migrations (version, checksum) VALUES ($1, $2)',
  selectOne: 'SELECT version FROM agentic_migrations WHERE version = $1',
  lock: 'SELECT pg_advisory_xact_lock(4171939288)',
};
