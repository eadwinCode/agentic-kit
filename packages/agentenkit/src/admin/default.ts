import type { AdminStore } from '../ports/admin.js';
import { openSqlite } from '../adapters/sqlite.js';
import { SqliteAdminStore } from './sqlite.js';
import { PostgresAdminStore, openPostgres } from './postgres.js';

/** Where the default store writes when no Postgres URL is set. */
export const DEFAULT_ADMIN_DB = 'agentic-kit-admin.sqlite';

/** The store `setupAgentCore` uses when none is configured (§2.9).
 *
 *  `AGENTIC_KIT_ADMIN_DATABASE_URL` selects Postgres — point it at its own
 *  database or the one you already have, the `agentic_` prefix keeps them
 *  apart. Without it, SQLite on disk at `AGENTIC_KIT_ADMIN_DB`, with whichever
 *  driver the process has.
 *
 *  The connection is opened eagerly. A store that cannot be opened is a
 *  configuration problem worth failing on at startup: degrading quietly would
 *  lose every run record, and a dashboard showing nothing looks the same as no
 *  traffic.
 *
 *  The SCHEMA is brought up to date behind it: a service starts at the same
 *  speed whether or not it has migrating to do, and the first admin call waits
 *  for the schema rather than meeting a table that is not there yet. */
export async function openDefaultAdminStore(
  log?: { error(message: string, ...rest: unknown[]): void },
): Promise<AdminStore> {
  const url = process.env.AGENTIC_KIT_ADMIN_DATABASE_URL;
  if (url) return PostgresAdminStore.connect(await openPostgres(url), log);
  return SqliteAdminStore.open(
    await openSqlite(process.env.AGENTIC_KIT_ADMIN_DB ?? DEFAULT_ADMIN_DB),
    log,
  );
}
