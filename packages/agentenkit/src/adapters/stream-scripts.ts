import { StreamClosedError, StreamGoneError, type RunStreams, type StreamMeta, type StreamSnapshot } from '../ports/streams.js';
import { isStreamEnd, type StreamEnd, type StreamEvent, type StreamItem } from '../core/stream-events.js';

/** RunStreams over Redis Streams, shared by the Redis and Upstash adapters.
 *  Each stream is two keys in one hash slot: a hash with who it belongs to
 *  and whether it is closed, and the Redis Stream itself, one entry per
 *  event. The offset is the entry id. Every write is one Lua script, so a
 *  close can never slip between an append's check and its XADD. The Go
 *  adapters run the same scripts on the same keys. */

export const streamMetaKey = (streamId: string) => `agent:run:{${streamId}}:meta`;
export const streamKey = (streamId: string) => `agent:run:{${streamId}}`;

/** KEYS: meta, stream. ARGV: threadId, runId, ttlMs. 1 opened, 0 already open. */
export const STREAM_OPEN_SCRIPT = `if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'threadId', ARGV[1], 'runId', ARGV[2], 'closed', '0')
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1`;

/** KEYS: meta, stream. ARGV: one event JSON each. The entry ids, or -1 gone,
 *  -2 closed. The stream key takes the meta key's expiry. */
export const STREAM_APPEND_SCRIPT = `local closed = redis.call('HGET', KEYS[1], 'closed')
if not closed then return -1 end
if closed == '1' then return -2 end
local ids = {}
for i = 1, #ARGV do ids[i] = redis.call('XADD', KEYS[2], '*', 'e', ARGV[i]) end
local ttl = redis.call('PTTL', KEYS[1])
if ttl > 0 then redis.call('PEXPIRE', KEYS[2], ttl) end
return ids`;

/** KEYS: meta, stream. ARGV: end JSON, graceMs. 1 closed, 0 already closed,
 *  -1 gone. */
export const STREAM_CLOSE_SCRIPT = `local closed = redis.call('HGET', KEYS[1], 'closed')
if not closed then return -1 end
if closed == '1' then return 0 end
redis.call('XADD', KEYS[2], '*', 'e', ARGV[1])
redis.call('HSET', KEYS[1], 'closed', '1', 'end', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 1`;

/** KEYS: meta, stream. ARGV: the XRANGE start ('-', or '(' + an entry id).
 *  -1 gone, else { threadId, runId, closed, end JSON or '', entries }. */
export const STREAM_READ_SCRIPT = `local meta = redis.call('HMGET', KEYS[1], 'threadId', 'runId', 'closed', 'end')
if not meta[1] then return -1 end
return { meta[1], meta[2], meta[3], meta[4] or '', redis.call('XRANGE', KEYS[2], ARGV[1], '+') }`;

/** Runs a script: the one thing the Redis and Upstash clients differ in. */
export type RunScript = (script: string, keys: string[], args: string[]) => Promise<unknown>;

/** Waits until the stream may have something after `after`, or `maxMs`
 *  passes, or the signal aborts. A wait that returns early without news is
 *  harmless: the reader reads again and finds nothing. */
export type WaitForNews = (key: string, after: string, maxMs: number, signal?: AbortSignal) => Promise<void>;

/** Compares two entry ids ("<ms>-<seq>"). */
export function compareEntryIds(a: string, b: string): number {
  const [am, as] = a.split('-').map(Number);
  const [bm, bs] = b.split('-').map(Number);
  return am! - bm! || (as ?? 0) - (bs ?? 0);
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener('abort', done, { once: true });
  });
}

/** A value from a script reply as a string. Upstash's client may already
 *  have parsed a JSON string into an object. */
const text = (v: unknown): string => (typeof v === 'string' ? v : JSON.stringify(v));

function parseEntry(entry: unknown): StreamItem {
  const [id, fields] = entry as [unknown, unknown[]];
  const at = fields.findIndex((f) => text(f) === 'e');
  const raw = fields[at + 1];
  const event = (typeof raw === 'string' ? JSON.parse(raw) : raw) as StreamEvent;
  return { ...event, offset: text(id) } as StreamItem;
}

export class ScriptedRunStreams implements RunStreams {
  constructor(
    private readonly run: RunScript,
    private readonly waitForNews: WaitForNews,
    /** The longest a reader waits before reading again, news or not: the
     *  floor under a missed wake-up. */
    private readonly pollMs: number,
  ) {}

  async open(streamId: string, meta: StreamMeta, ttlMs: number) {
    await this.run(STREAM_OPEN_SCRIPT, [streamMetaKey(streamId), streamKey(streamId)], [
      meta.threadId, meta.runId, String(Math.max(1, Math.round(ttlMs))),
    ]);
  }

  async append(streamId: string, events: StreamEvent[]) {
    if (events.length === 0) return [];
    const res = await this.run(
      STREAM_APPEND_SCRIPT,
      [streamMetaKey(streamId), streamKey(streamId)],
      events.map((e) => JSON.stringify(e)),
    );
    if (Number(res) === -1) throw new StreamGoneError(streamId);
    if (Number(res) === -2) throw new StreamClosedError(streamId);
    return (res as unknown[]).map(text);
  }

  private async page(streamId: string, after: string | null) {
    const res = await this.run(STREAM_READ_SCRIPT, [streamMetaKey(streamId), streamKey(streamId)], [
      after ? `(${after}` : '-',
    ]);
    if (!Array.isArray(res)) return null;
    const [threadId, runId, closed, end, entries] = res as [unknown, unknown, unknown, unknown, unknown[]];
    return {
      meta: { threadId: text(threadId), runId: text(runId) },
      closed: text(closed) === '1',
      end: text(closed) === '1' ? ((typeof end === 'string' ? JSON.parse(end) : end) as StreamEnd) : null,
      items: (entries ?? []).map(parseEntry),
    };
  }

  async *read(streamId: string, after: string | null, signal?: AbortSignal): AsyncIterable<StreamItem> {
    let cursor = after;
    for (;;) {
      if (signal?.aborted) return;
      const page = await this.page(streamId, cursor);
      if (!page) throw new StreamGoneError(streamId);
      for (const item of page.items) {
        yield item;
        cursor = item.offset;
        if (isStreamEnd(item)) return;
      }
      // Read from past the end item: nothing more will ever come.
      if (page.closed && page.items.length === 0) return;
      if (page.items.length === 0) {
        await this.waitForNews(streamKey(streamId), cursor ?? '0-0', this.pollMs, signal);
      }
    }
  }

  async snapshot(streamId: string, after?: string | null): Promise<StreamSnapshot | null> {
    const page = await this.page(streamId, after ?? null);
    if (!page) return null;
    return { meta: page.meta, items: page.items, end: page.end };
  }

  async close(streamId: string, end: StreamEnd, graceMs: number) {
    const res = await this.run(STREAM_CLOSE_SCRIPT, [streamMetaKey(streamId), streamKey(streamId)], [
      JSON.stringify(end), String(Math.max(1, Math.round(graceMs))),
    ]);
    if (Number(res) === -1) throw new StreamGoneError(streamId);
  }

  async delete(streamId: string) {
    await this.run(`redis.call('DEL', KEYS[1], KEYS[2]) return 1`, [streamMetaKey(streamId), streamKey(streamId)], []);
  }
}
