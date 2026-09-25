import type { Migration, MigrationDialect } from '../runner.js';
import { sql as init } from './0001-init.js';
import { sql as runSettledAt } from './0002-run-settled-at.js';
import { sql as runEnqueuedAt } from './0003-run-enqueued-at.js';
import { sql as runSettleClaim } from './0004-run-settle-claim.js';
import { sql as bigintCounters } from './0005-bigint-counters.js';

/** The admin migrations for Postgres, in apply order. Append only. */
export const migrations: Migration[] = [
  // The Go runtime's 0001 differs from this one only in its header comment;
  // the SQL is the same. A released file is never edited, so each runtime
  // accepts the other's checksum, and one admin database serves both.
  { version: '0001_init', sql: init, equivalent: ['2e6ebddc37635c42e74e8c73bba63e0f01bdd8371d33698c50079ebfb8f6782c'] },
  // Named and worded exactly as the Go runtime's, so both record the same
  // version and checksum.
  { version: '0002_run_settled_at', sql: runSettledAt },
  { version: '0003_run_enqueued_at', sql: runEnqueuedAt },
  { version: '0004_run_settle_claim', sql: runSettleClaim },
  { version: '0005_bigint_counters', sql: bigintCounters },
];

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
