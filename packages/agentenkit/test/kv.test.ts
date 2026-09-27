import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { MemoryKv } from '../src/adapters/memory.js';
import { SqliteKv } from '../src/adapters/sqlite.js';
import type { Kv } from '../src/ports/kv.js';

// The promise every Kv adapter keeps, run against each one. The Go package
// runs the same cases under the same names (kv_test.go).

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function kvSuite(name: string, make: () => Kv) {
  describe(`kv (${name})`, () => {
    it('get returns what set wrote, and null for a missing key', async () => {
      const kv = make();
      expect(await kv.set('a', '1')).toBe(true);
      expect(await kv.get('a')).toBe('1');
      await kv.set('a', '2');
      expect(await kv.get('a')).toBe('2');
      expect(await kv.get('missing')).toBeNull();
    });

    it('set with onlyIfNotExists writes only when the key is absent', async () => {
      const kv = make();
      expect(await kv.set('lock', 'w1', { onlyIfNotExists: true })).toBe(true);
      expect(await kv.set('lock', 'w2', { onlyIfNotExists: true })).toBe(false);
      expect(await kv.get('lock')).toBe('w1');
    });

    it('an expired key reads as missing, and SET NX can take it', async () => {
      const kv = make();
      await kv.set('lock', 'w1', { exSeconds: 1 });
      expect(await kv.get('lock')).toBe('w1');
      await sleep(1_100);
      expect(await kv.get('lock')).toBeNull();
      expect(await kv.set('lock', 'w2', { onlyIfNotExists: true })).toBe(true);
      expect(await kv.get('lock')).toBe('w2');
    });

    it('del removes a key', async () => {
      const kv = make();
      await kv.set('a', '1');
      await kv.del('a');
      expect(await kv.get('a')).toBeNull();
      await kv.del('a'); // fine when it is not there
    });

    it('incr counts up from 1', async () => {
      const kv = make();
      expect(await kv.incr('n')).toBe(1);
      expect(await kv.incr('n')).toBe(2);
      expect(await kv.get('n')).toBe('2');
    });

    it("incrWithExpiry stamps a new counter and keeps a live one's expiry", async () => {
      const kv = make();
      expect(await kv.incrWithExpiry('n', 1)).toBe(1);
      await sleep(600);
      // Still the first expiry: a second count does not push it back.
      expect(await kv.incrWithExpiry('n', 60)).toBe(2);
      await sleep(600);
      expect(await kv.get('n')).toBeNull();
      // An expired counter starts again, with the new expiry.
      expect(await kv.incrWithExpiry('n', 60)).toBe(1);
    });

    it('setIfValue writes only while the key holds the expected value', async () => {
      const kv = make();
      await kv.set('lock', 'w1');
      expect(await kv.setIfValue('lock', 'w2', 'w3')).toBe(false);
      expect(await kv.setIfValue('lock', 'w1', 'w1b', { exSeconds: 60 })).toBe(true);
      expect(await kv.get('lock')).toBe('w1b');
      expect(await kv.setIfValue('missing', 'x', 'y')).toBe(false);
      expect(await kv.get('missing')).toBeNull();
    });

    it('delIfValue deletes only while the key holds the expected value', async () => {
      const kv = make();
      await kv.set('lock', 'w1');
      expect(await kv.delIfValue('lock', 'w2')).toBe(false);
      expect(await kv.get('lock')).toBe('w1');
      expect(await kv.delIfValue('lock', 'w1')).toBe(true);
      expect(await kv.get('lock')).toBeNull();
    });
  });
}

kvSuite('memory', () => new MemoryKv());
kvSuite('sqlite', () => new SqliteKv(new Database(':memory:')));

describe('kv (sqlite) across a restart', () => {
  it('what the kv holds outlives the handle', async () => {
    const db = new Database(':memory:');
    await new SqliteKv(db).set('agent:tool:sandbox:t1', '{"id":"s1"}');
    // A new handle on the same file, as the app after a restart.
    expect(await new SqliteKv(db).get('agent:tool:sandbox:t1')).toBe('{"id":"s1"}');
  });
});
