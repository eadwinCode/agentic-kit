import { describe, expect, it } from 'bun:test';
import { Database } from 'bun:sqlite';
import { SqliteAdminStore } from '../src/admin/sqlite.js';
import { runMigrations, statements, type Migration, type MigrationDriver } from '../src/admin/migrations/runner.js';
import { dialect, migrations } from '../src/admin/migrations/sqlite/index.js';
import type { SqliteLike } from '../src/adapters/sqlite.js';

function db() {
  const raw = new Database(':memory:');
  const handle = raw as unknown as SqliteLike;
  const driver: MigrationDriver = {
    exec: async (sql, params = []) => {
      handle.prepare(sql).run(...(params as unknown[]));
    },
    rows: async (sql, params = []) =>
      handle.prepare(sql).all(...(params as unknown[])) as Array<Record<string, unknown>>,
  };
  return { raw, handle, driver };
}

const versions = (driver: MigrationDriver) =>
  driver.rows('SELECT version FROM agentic_migrations ORDER BY version').then((r) =>
    r.map((x) => String(x.version)),
  );

const one = (sql: string): Migration[] => [{ version: '0001_test', sql }];

describe('admin migrations (§2.9)', () => {
  it('records each migration it applies', async () => {
    const { driver } = db();
    await runMigrations(driver, dialect, [
      { version: '0001_a', sql: 'CREATE TABLE a (id TEXT);' },
      { version: '0002_b', sql: 'CREATE TABLE b (id TEXT);' },
    ]);
    expect(await versions(driver)).toEqual(['0001_a', '0002_b']);
  });

  it('is idempotent', async () => {
    const { driver } = db();
    // Deliberately NOT idempotent SQL: running it twice would fail. Only the
    // ledger stops that, which is the point.
    const ms = one('CREATE TABLE a (id TEXT);');
    for (let i = 0; i < 3; i++) await runMigrations(driver, dialect, ms);
    expect(await versions(driver)).toEqual(['0001_test']);
  });

  it('leaves a clean slate when a file fails part way', async () => {
    const { driver } = db();
    await expect(
      runMigrations(driver, dialect, one('CREATE TABLE a (id TEXT);\nTHIS IS NOT SQL;')),
    ).rejects.toThrow();
    // The whole run is one transaction, so neither the table the first
    // statement made nor the ledger survives.
    await expect(driver.rows('SELECT * FROM a')).rejects.toThrow();
    await expect(driver.rows('SELECT * FROM agentic_migrations')).rejects.toThrow();
    // And the next attempt, with the file fixed, works.
    await runMigrations(driver, dialect, one('CREATE TABLE a (id TEXT);'));
    expect(await versions(driver)).toEqual(['0001_test']);
  });

  it('refuses a migration edited after it was applied', async () => {
    const { driver } = db();
    await runMigrations(driver, dialect, one('CREATE TABLE a (id TEXT);'));
    // Same version, different content: the database no longer matches the
    // code, and saying so is the whole job of an operational store.
    await expect(
      runMigrations(driver, dialect, one('CREATE TABLE a (id TEXT, extra TEXT);')),
    ).rejects.toThrow('changed after it was applied');
  });

  it('does not let line endings change the checksum', async () => {
    const { driver } = db();
    await runMigrations(driver, dialect, one('CREATE TABLE a (id TEXT);\n'));
    // The same file checked out on Windows. It must not read as edited, or
    // every upgrade there would be refused.
    await runMigrations(driver, dialect, one('CREATE TABLE a (id TEXT);\r\n'));
    expect(await versions(driver)).toEqual(['0001_test']);
  });

  it('splits statements around comments that contain a semicolon', () => {
    const got = statements(`
-- a comment; with a semicolon in it
CREATE TABLE a (id TEXT);

CREATE INDEX i ON a(id);
`);
    expect(got.length).toBe(2);
    expect(got[0]!.startsWith('CREATE TABLE')).toBe(true);
    expect(got[1]!.startsWith('CREATE INDEX')).toBe(true);
  });

  it('repairs a database from an older release before indexing its columns', async () => {
    const { raw, handle, driver } = db();
    // The shape agentic_steps had before threadId, text and toolCalls were
    // added: a database last opened by that release, upgrading straight to
    // the migrator.
    raw.exec(`CREATE TABLE agentic_steps (
      runId TEXT NOT NULL, agentId TEXT, "index" INTEGER NOT NULL,
      durationMs INTEGER NOT NULL, finishReason TEXT NOT NULL,
      inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
      outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
      tools TEXT, at INTEGER NOT NULL)`);

    // CREATE TABLE IF NOT EXISTS leaves the old table alone, so the columns it
    // never got have to be added BEFORE the baseline indexes one of them.
    // Getting that order wrong fails right here.
    await runMigrations(driver, dialect, migrations);

    const store = SqliteAdminStore.open(handle);
    expect(await store.steps.listByThread('t1')).toEqual([]);
    expect(await versions(driver)).toEqual(['0001_init']);
  });

  it('surfaces a failed migration on the first call, not as an empty dashboard', async () => {
    const { raw, handle } = db();
    // A table in the way that the baseline cannot reconcile.
    raw.exec('CREATE TABLE agentic_migrations (nonsense TEXT)');
    const store = SqliteAdminStore.open(handle, { error: () => {} });
    await expect(store.runs.get('r1')).rejects.toThrow();
  });
});
