import type { Migration, MigrationDialect, MigrationDriver } from '../runner.js';
import { sql as init } from './0001-init.js';
import { sql as runSettledAt } from './0002-run-settled-at.js';
import { sql as runEnqueuedAt } from './0003-run-enqueued-at.js';
import { sql as runSettleClaim } from './0004-run-settle-claim.js';

/** The admin migrations for SQLite, in apply order. Append only. */
export const migrations: Migration[] = [
  // The Go runtime's 0001 differs from this one only in its header comment;
  // the SQL is the same. A released file is never edited, so each runtime
  // accepts the other's checksum, and one admin database serves both.
  { version: '0001_init', sql: init, equivalent: ['800097b6ae76a1d130ca14c520f0789974051fb7275be0bcc1458a2e88aaa039'] },
  // Named and worded exactly as the Go runtime's, so both record the same
  // version and checksum.
  { version: '0002_run_settled_at', sql: runSettledAt },
  { version: '0003_run_enqueued_at', sql: runEnqueuedAt },
  { version: '0004_run_settle_claim', sql: runSettleClaim },
];

/** Columns added after the first release, per table. SQLite cannot express
 *  ADD COLUMN IF NOT EXISTS, so these are checked against the live table. */
const LATER_COLUMNS: Record<string, Record<string, string>> = {
  agentic_steps: { text: 'TEXT', toolCalls: 'TEXT', threadId: 'TEXT' },
  agentic_runs: {
    prompt: 'TEXT', tokenBudget: 'INTEGER', runState: 'TEXT', providerOptions: 'TEXT',
  },
  agentic_threads: { startedWith: 'TEXT' },
};

/** Add the columns a database created by an older release never got. A table
 *  that does not exist at all is left alone: on a fresh database the migration
 *  files create it, columns and all. */
async function repair(db: MigrationDriver): Promise<void> {
  for (const [table, columns] of Object.entries(LATER_COLUMNS)) {
    const have = new Set(
      (await db.rows(`PRAGMA table_info(${table})`)).map((r) => String(r.name)),
    );
    if (have.size === 0) continue; // no such table yet
    for (const [column, type] of Object.entries(columns)) {
      if (!have.has(column)) await db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${type}`);
    }
  }
}

/** SQLite needs no lock: the database takes one writer at a time, and
 *  BEGIN IMMEDIATE makes a second process wait for it. */
export const dialect: MigrationDialect = {
  name: 'sqlite',
  ledger: `CREATE TABLE IF NOT EXISTS agentic_migrations (
    version TEXT PRIMARY KEY, checksum TEXT NOT NULL, appliedAt INTEGER NOT NULL)`,
  insert: `INSERT INTO agentic_migrations (version, checksum, appliedAt)
           VALUES (?, ?, CAST(strftime('%s','now') AS INTEGER) * 1000)`,
  selectOne: 'SELECT version FROM agentic_migrations WHERE version = ?',
  begin: 'BEGIN IMMEDIATE',
  repair,
};
