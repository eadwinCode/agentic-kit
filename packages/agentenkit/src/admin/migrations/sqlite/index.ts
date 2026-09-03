import type { Migration, MigrationDialect, MigrationDriver } from '../runner.js';
import { sql as init } from './0001-init.js';

/** The admin migrations for SQLite, in apply order. Append only. */
export const migrations: Migration[] = [{ version: '0001_init', sql: init }];

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

/** SQLite needs no lock: the driver holds one connection and the database
 *  takes one writer at a time. */
export const dialect: MigrationDialect = {
  name: 'sqlite',
  ledger: `CREATE TABLE IF NOT EXISTS agentic_migrations (
    version TEXT PRIMARY KEY, checksum TEXT NOT NULL, appliedAt INTEGER NOT NULL)`,
  insert: `INSERT INTO agentic_migrations (version, checksum, appliedAt)
           VALUES (?, ?, CAST(strftime('%s','now') AS INTEGER) * 1000)`,
  selectOne: 'SELECT version FROM agentic_migrations WHERE version = ?',
  repair,
};
