import { describe, expect, it } from 'bun:test';
import { StreamClosedError, StreamGoneError, type RunStreams } from '../src/ports/streams.js';
import type { StreamEnd, StreamEvent, StreamItem } from '../src/core/stream-events.js';

// The promise every RunStreams adapter keeps, run against each one. The Go
// package runs the same cases under the same names (run_streams_test.go).

const meta = { threadId: 't1', runId: 'r1' };
const text = (delta: string): StreamEvent => ({ type: 'TEXT_MESSAGE_CONTENT', messageId: 'm1', delta });
const finished: StreamEnd = { type: 'RUN_FINISHED', status: 'finished' };
const HOUR = 60 * 60_000;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function collect(it: AsyncIterable<StreamItem>): Promise<StreamItem[]> {
  const out: StreamItem[] = [];
  for await (const item of it) out.push(item);
  return out;
}

const deltas = (items: StreamItem[]) =>
  items.map((i) => (i.type === 'TEXT_MESSAGE_CONTENT' ? i.delta : i.type));

export interface SuiteOptions {
  /** A second handle on the same store, as another process would have. A
   *  reader on one must see appends made through the other, with no
   *  in-process wake-up to help. */
  other?: () => Promise<RunStreams>;
  /** How long a live wait may take to notice an append (poll interval). */
  settleMs?: number;
}

export function runStreamsSuite(name: string, make: () => Promise<RunStreams>, opts: SuiteOptions = {}) {
  const settle = opts.settleMs ?? 20;
  let n = 0;
  const fresh = () => `s${Date.now()}-${n++}:1`;

  describe(`run streams (${name})`, () => {
    it('append returns increasing offsets', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      const offsets = [...(await s.append(id, [text('a'), text('b')])), ...(await s.append(id, [text('c')]))];
      expect(new Set(offsets).size).toBe(3);
      // Increasing means: reading after each one gives exactly what followed.
      for (let i = 0; i < offsets.length; i++) {
        const snap = await s.snapshot(id, offsets[i]);
        expect(snap!.items.map((x) => x.offset)).toEqual(offsets.slice(i + 1));
      }
    });

    it('opening an open stream is a no-op', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      await s.append(id, [text('a')]);
      await s.open(id, meta, HOUR);
      expect(deltas((await s.snapshot(id))!.items)).toEqual(['a']);
      expect((await s.snapshot(id))!.meta).toEqual(meta);
    });

    it('read from null replays everything, then tails', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      await s.append(id, [text('a'), text('b')]);
      const got = collect(s.read(id, null));
      await sleep(settle);
      await s.append(id, [text('c')]);
      await s.close(id, finished, HOUR);
      expect(deltas(await got)).toEqual(['a', 'b', 'c', 'RUN_FINISHED']);
    });

    it('read after an offset skips what came before', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      const [first] = await s.append(id, [text('a'), text('b')]);
      await s.close(id, finished, HOUR);
      expect(deltas(await collect(s.read(id, first!)))).toEqual(['b', 'RUN_FINISHED']);
    });

    it('a read ends on close and the end is the last item', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      const got = collect(s.read(id, null));
      await sleep(settle);
      await s.append(id, [text('a')]);
      await s.close(id, { type: 'RUN_ERROR', status: 'error', error: 'boom' }, HOUR);
      const items = await got;
      expect(items.at(-1)).toMatchObject({ type: 'RUN_ERROR', status: 'error', error: 'boom' });
      expect(items).toHaveLength(2);
    });

    it('a late reader of a closed stream gets the end at once', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      await s.append(id, [text('a')]);
      await s.close(id, finished, HOUR);
      expect(deltas(await collect(s.read(id, null)))).toEqual(['a', 'RUN_FINISHED']);
      const snap = await s.snapshot(id);
      expect(snap!.end).toMatchObject({ type: 'RUN_FINISHED', status: 'finished' });
      // Reading from the end item on: nothing more will ever come.
      expect(await collect(s.read(id, snap!.items.at(-1)!.offset))).toEqual([]);
    });

    it('snapshot never waits on an open stream', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      await s.append(id, [text('a')]);
      const snap = await s.snapshot(id);
      expect(deltas(snap!.items)).toEqual(['a']);
      expect(snap!.end).toBeNull();
    });

    it('append after close is refused', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      await s.close(id, finished, HOUR);
      await expect(s.append(id, [text('late')])).rejects.toBeInstanceOf(StreamClosedError);
    });

    it('a second close is a no-op', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      await s.close(id, finished, HOUR);
      await s.close(id, { type: 'RUN_ERROR', status: 'lost', error: 'late sweep' }, HOUR);
      const snap = await s.snapshot(id);
      expect(deltas(snap!.items)).toEqual(['RUN_FINISHED']); // the first end stands
    });

    it('a stream past its grace window reads as gone', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      await s.close(id, finished, 1000);
      await sleep(1100);
      expect(await s.snapshot(id)).toBeNull();
      await expect(collect(s.read(id, null))).rejects.toBeInstanceOf(StreamGoneError);
    });

    it('a read of a deleted stream ends as gone', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      await s.append(id, [text('a')]);
      const got = collect(s.read(id, null));
      await sleep(settle);
      await s.delete(id);
      await expect(got).rejects.toBeInstanceOf(StreamGoneError);
      await s.delete(id); // a missing one is a no-op
    });

    it('a stream never opened is gone', async () => {
      const s = await make();
      const id = fresh();
      expect(await s.snapshot(id)).toBeNull();
      await expect(s.append(id, [text('a')])).rejects.toBeInstanceOf(StreamGoneError);
      await expect(s.close(id, finished, HOUR)).rejects.toBeInstanceOf(StreamGoneError);
    });

    it('an aborted read ends quietly', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      const abort = new AbortController();
      const got = collect(s.read(id, null, abort.signal));
      await sleep(settle);
      abort.abort();
      expect(await got).toEqual([]);
    });

    it('every event keeps its fields', async () => {
      const s = await make();
      const id = fresh();
      await s.open(id, meta, HOUR);
      const events: StreamEvent[] = [
        { type: 'TOOL_CALL_END', toolCallId: 'c1', toolName: 'search', args: { q: 'x', n: 2 } },
        { type: 'CUSTOM', name: 'SEARCH_PROGRESS', value: { done: 3, of: 10 } },
        { type: 'SUBAGENT_EVENT', subagentId: 'sub_1', event: text('nested') },
      ];
      await s.append(id, events);
      const items = (await s.snapshot(id))!.items;
      expect(items.map(({ offset: _, ...e }) => e)).toEqual(events);
    });

    if (opts.other) {
      it('a missed wake-up only delays, never drops', async () => {
        const reader = await make();
        const writer = await opts.other!();
        const id = fresh();
        await writer.open(id, meta, HOUR);
        const got = collect(reader.read(id, null));
        await sleep(settle);
        // Written through another handle: no wake-up reaches the reader,
        // only its own poll.
        await writer.append(id, [text('a')]);
        await writer.close(id, finished, HOUR);
        expect(deltas(await got)).toEqual(['a', 'RUN_FINISHED']);
      });
    }
  });
}
