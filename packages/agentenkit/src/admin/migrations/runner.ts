/** Applies the platform's OWN schema to the admin store (§2.9), from numbered
 *  migration files.
 *
 *  Only the admin store is migrated here. A caller's `Storage` is their
 *  database and their schema; the platform never touches it.
 *
 *  The rules, in one place:
 *
 *  - A migration is one file, named for its number: `0001-init.ts`. The order
 *    in each dialect's `index.ts` is the apply order.
 *  - A file that has been released is never edited. Its checksum is recorded
 *    when it runs, and a later mismatch is an error, because a database that
 *    no longer matches the code is exactly the thing an operational store is
 *    supposed to tell you about.
 *  - 0001 is the schema as it stood before this existed, written so that
 *    running it against a database already holding those tables changes
 *    nothing. That is what lets an existing install pick the migrator up
 *    without a separate baseline step, and what repairs one that stopped on an
 *    older release.
 *
 *  The SQL lives in `.ts` files rather than `.sql` read from disk on purpose:
 *  this package runs inside Next.js and other bundlers, and a `node:fs` read
 *  of a file next to the module does not survive being bundled. The Go
 *  package embeds real `.sql` files; the two are kept in step by review. */
import { createHash } from 'node:crypto';
import type { AdminStore } from '../../ports/admin.js';

/** One versioned step. */
export interface Migration {
  /** Sorts and identifies it. Never reused, never renamed. */
  version: string;
  sql: string;
}

/** The two calls a migration runner needs. Both stores already have them. */
export interface MigrationDriver {
  exec(sql: string, params?: unknown[]): Promise<void>;
  rows(sql: string, params?: unknown[]): Promise<Array<Record<string, unknown>>>;
}

/** The little that differs between databases. */
export interface MigrationDialect {
  name: string;
  /** Creates the table recording what has run. */
  ledger: string;
  /** Records one applied migration. Two bind parameters. */
  insert: string;
  /** Reads one version back. One bind parameter. */
  selectOne: string;
  /** Taken first inside each migration's transaction, so two workers starting
   *  together queue rather than both applying the same file. Omitted for a
   *  database that serialises writers by itself. */
  lock?: string;
  /** Runs once BEFORE the migration files, for the one thing portable SQL
   *  cannot express: SQLite has no ADD COLUMN IF NOT EXISTS, so a database
   *  left behind by an older release needs a column check instead.
   *
   *  Before, not after, because the baseline creates indexes over columns that
   *  such a database may not have yet — and an index on a column that does not
   *  exist is an error, not a no-op.
   *
   *  Must be idempotent, and must tolerate a database where none of the tables
   *  exist: on a fresh one it runs before anything is created. */
  repair?: (db: MigrationDriver) => Promise<void>;
}

/** Split a migration into the statements to run one at a time, because
 *  neither driver reliably takes several at once.
 *
 *  Line comments are stripped BEFORE the split, so a semicolon inside one
 *  cannot cut a statement in half. A semicolon inside a string literal still
 *  would: migrations must not contain one. */
export function statements(sql: string): string[] {
  return sql
    .split('\n')
    .map((line) => {
      const i = line.indexOf('--');
      return i >= 0 ? line.slice(0, i) : line;
    })
    .join('\n')
    .split(';')
    .map((s) => s.trim())
    .filter(Boolean);
}

/** Hashed with carriage returns stripped: a Windows checkout can rewrite line
 *  endings, and a checksum that changed with the platform would reject every
 *  upgrade on it. */
export const checksum = (sql: string): string =>
  createHash('sha256').update(sql.replace(/\r\n/g, '\n')).digest('hex');

/** Apply every migration the ledger does not already carry, in order. Safe to
 *  call from several processes at once.
 *
 *  The whole run is ONE transaction, taken under the dialect's lock: the
 *  ledger, the repair step and every file commit together or not at all. Both
 *  databases roll DDL back with the transaction, so a run that fails part way
 *  leaves nothing behind to confuse the next attempt.
 *
 *  The lock has to cover the ledger too. Postgres's CREATE TABLE IF NOT EXISTS
 *  is not atomic against another session creating the same table: both see it
 *  missing, both create it, and one gets a duplicate-type error. Two workers
 *  starting together is the normal case here, so that is not a rare race.
 *
 *  `db` must be ONE connection, not a pool: BEGIN on one connection and DDL on
 *  another is not a transaction at all.
 *
 *  One consequence worth knowing: a migration that cannot run inside a
 *  transaction (CREATE INDEX CONCURRENTLY, say) does not belong in a file
 *  here. */
export async function runMigrations(
  db: MigrationDriver,
  dialect: MigrationDialect,
  migrations: Migration[],
): Promise<void> {
  const where = (msg: string) => `${dialect.name} migrations: ${msg}`;

  await db.exec('BEGIN');
  try {
    if (dialect.lock) await db.exec(dialect.lock);
    await db.exec(dialect.ledger);
    if (dialect.repair) await dialect.repair(db);

    const applied = new Map<string, string>();
    for (const row of await db.rows('SELECT version, checksum FROM agentic_migrations')) {
      applied.set(String(row.version), String(row.checksum));
    }

    for (const m of migrations) {
      const sum = checksum(m.sql);
      const already = applied.get(m.version);
      if (already !== undefined) {
        if (already !== sum) {
          throw new Error(
            `${m.version} changed after it was applied: the database no longer matches the code`,
          );
        }
        continue;
      }
      for (const stmt of statements(m.sql)) await db.exec(stmt);
      await db.exec(dialect.insert, [m.version, sum]);
    }
    await db.exec('COMMIT');
  } catch (err) {
    try {
      await db.exec('ROLLBACK');
    } catch {
      // The transaction is already gone; the original error is the useful one.
    }
    throw new Error(where(err instanceof Error ? err.message : String(err)));
  }
}

/** An AdminStore that waits for the schema before it does anything.
 *
 *  `setupAgentCore` returns as soon as the store is OPEN, and the schema is
 *  brought up to date behind it, so a service starts at the same speed whether
 *  or not it has migrating to do. Any admin call made in that window waits
 *  here rather than meeting a table that does not exist yet — and a migration
 *  that failed surfaces on the first call rather than as a mystery empty
 *  dashboard.
 *
 *  Written once here so every admin store, including one added later, gets the
 *  same behaviour for free. */
export function gatedAdminStore(inner: AdminStore, ready: Promise<void>): AdminStore {
  const gate = <T extends Record<string, any>>(group: T): T => {
    const out: Record<string, unknown> = {};
    for (const [name, fn] of Object.entries(group)) {
      out[name] =
        typeof fn === 'function'
          ? async (...args: unknown[]) => {
              await ready;
              return fn.apply(group, args);
            }
          : fn;
    }
    return out as T;
  };
  return {
    threads: gate(inner.threads),
    runs: gate(inner.runs),
    steps: gate(inner.steps),
    ...(inner.close ? { close: () => inner.close!() } : {}),
  };
}
