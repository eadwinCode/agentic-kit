import type { RuntimePorts, ThreadSnapshot } from '../ports/runtime.js';
import { StreamGoneError } from '../ports/streams.js';
import { snapshotStream, threadSnapshot } from './snapshot.js';
import type { StreamItem } from './stream-events.js';
import type { AgentEvent } from './types.js';

export interface FollowOptions {
  /** Resume after this seq. `-1` (the default) replays the thread from the
   *  start. Comes from the client's cursor — `Last-Event-ID` for SSE. */
  since?: number;
  /** Stops the stream and unsubscribes. Wire your request's abort signal to
   *  it, or the subscription outlives the client. */
  signal?: AbortSignal;
}

/** Caps the events a follower holds for a slow consumer. Past it the queue is
 *  dropped and the follower reads what it missed back from storage instead:
 *  the log has every durable event, so nothing is lost but notices, which
 *  nobody needs twice. */
const MAX_LIVE_QUEUE = 10_000;

/** How long a follower waits for a missing seq to reach storage: another
 *  process may have taken seq N, and still be writing it, when seq N+1
 *  arrives on the bus. */
const GAP_RETRIES = 3;
const GAP_WAIT_MS = 50;

/** Every event on a thread, replay first and then live, as one sequence.
 *
 *  The ordering here is the whole point, and it is easy to get wrong in a route
 *  handler:
 *
 *   1. **Subscribe before replaying.** An event published between the replay
 *      finishing and the tail starting is otherwise lost for ever.
 *   2. **Never emit at or below the cursor.** The client would render it twice.
 *   3. **`seq === 0` is a bus-only notice** (heartbeats, death notices).
 *      Always forward it, never let it move the cursor.
 *   4. **Never skip a seq.** The bus is at-most-once and does not promise
 *      order: an event can be dropped, or arrive after the one published
 *      behind it. An event that jumps past the next seq first has the gap
 *      read back from storage, in order.
 *
 *  Framework-neutral on purpose: an async iterable is something Express, Hono,
 *  Nest, Next and a plain worker can each consume in their own way. */
export async function* followEvents(
  deps: RuntimePorts,
  threadId: string,
  options: FollowOptions = {},
): AsyncGenerator<AgentEvent> {
  const since = options.since ?? -1;
  const { signal } = options;

  let lastSeq = since;
  let live = false;
  let closed = false;
  let overflow = false;
  /** Published while the replay is still running. */
  const pending: AgentEvent[] = [];
  /** Published once live, waiting for the consumer. */
  let queue: AgentEvent[] = [];
  let wake: (() => void) | null = null;

  const notify = () => {
    const w = wake;
    wake = null;
    w?.();
  };

  // Rule 1: subscribe FIRST.
  const unsubscribe = await deps.bus.subscribe(threadId, (event) => {
    if (!live) {
      pending.push(event);
      return;
    }
    if (queue.length >= MAX_LIVE_QUEUE) {
      queue = [];
      overflow = true;
    }
    queue.push(event);
    notify();
  });

  const onAbort = () => {
    closed = true;
    notify();
  };
  signal?.addEventListener('abort', onAbort, { once: true });

  /** Every stored event after the cursor and before `upTo` (all of them when
   *  absent), in order, moving the cursor. */
  const fromStorage = async (upTo?: number): Promise<AgentEvent[]> => {
    const out: AgentEvent[] = [];
    for (const e of await deps.storage.events.listSince(threadId, lastSeq)) {
      if (upTo !== undefined && e.seq >= upTo) break;
      if (e.seq > lastSeq) {
        out.push(e);
        lastSeq = e.seq;
      }
    }
    return out;
  };

  /** Rules 2 to 4 in one place, so no caller has to remember them. */
  const deliver = async (event: AgentEvent): Promise<AgentEvent[]> => {
    if (event.seq === 0) return [event]; // a notice: forward, but do not advance
    const out: AgentEvent[] = [];
    for (let attempt = 0; event.seq > Math.max(lastSeq, 0) + 1 && attempt < GAP_RETRIES; attempt++) {
      if (attempt > 0) await new Promise((r) => setTimeout(r, GAP_WAIT_MS));
      try {
        out.push(...(await fromStorage(event.seq)));
      } catch {
        break; // the live event still goes out; the gap stays
      }
    }
    if (event.seq > lastSeq) {
      lastSeq = event.seq;
      out.push(event);
    }
    return out;
  };

  try {
    if (signal?.aborted) return;

    // …then the durable log…
    for (const event of await fromStorage()) yield event;

    // …then whatever arrived behind it, in order.
    for (const event of pending.sort((a, b) => a.seq - b.seq)) {
      for (const e of await deliver(event)) yield e;
    }
    pending.length = 0;
    live = true;

    while (!closed && !signal?.aborted) {
      if (overflow) {
        // The consumer fell too far behind: catch up from the log.
        overflow = false;
        queue = [];
        for (const e of await fromStorage()) yield e;
      }
      while (queue.length > 0) {
        const event = queue.shift()!;
        for (const e of await deliver(event)) yield e;
      }
      if (closed || signal?.aborted) break;
      await new Promise<void>((resolve) => {
        wake = resolve;
      });
    }
  } finally {
    // Runs on abort, on a consumer that stops iterating, and on error alike —
    // a subscription that outlives its reader is a leak on every reconnect.
    signal?.removeEventListener('abort', onAbort);
    try {
      await unsubscribe();
    } catch {
      // A bus that fails to unsubscribe must not mask the reason we stopped.
    }
  }
}

/** Headers an SSE response needs. `X-Accel-Buffering` is for nginx, which
 *  otherwise buffers the stream and makes it look like nothing is happening. */
export const SSE_HEADERS: Record<string, string> = {
  'Content-Type': 'text/event-stream; charset=utf-8',
  'Cache-Control': 'no-cache, no-transform',
  Connection: 'keep-alive',
  'X-Accel-Buffering': 'no',
};

/** One event as an SSE frame.
 *
 *  A bus-only notice (`seq === 0`) is sent WITHOUT an `id:` line. EventSource
 *  stores any id it sees and sends it back as `Last-Event-ID` on reconnect —
 *  so stamping `id: 0` on a heartbeat would rewind the client's cursor to the
 *  beginning of the thread and replay everything. */
export function sseFrame(event: AgentEvent): string {
  const data = JSON.stringify(event);
  return event.seq === 0 ? `data: ${data}\n\n` : `id: ${event.seq}\ndata: ${data}\n\n`;
}

export interface SseOptions extends FollowOptions {
  /** Emitted once, up front: how long a browser waits before reconnecting. */
  retryMs?: number;
}

export interface SseStream {
  /** SSE-encoded bytes. Serve it directly where a web `Response` is native, or
   *  pipe it where it is not — see the docs for Express and Nest. */
  stream: ReadableStream<Uint8Array>;
  headers: Record<string, string>;
}

/** The event sequence, encoded as Server-Sent Events.
 *
 *  Returns the stream and the headers rather than a `Response`, because half
 *  the ecosystem does not have one. */
export function toSseStream(
  events: AsyncGenerator<AgentEvent>,
  options: SseOptions = {},
): SseStream {
  const encoder = new TextEncoder();
  let started = false;

  const stream = new ReadableStream<Uint8Array>({
    // `pull` rather than `start`: the consumer sets the pace, and a slow client
    // does not make an unbounded queue of encoded frames.
    async pull(controller) {
      if (!started) {
        started = true;
        if (options.retryMs !== undefined) {
          controller.enqueue(encoder.encode(`retry: ${options.retryMs}\n\n`));
          return;
        }
      }
      try {
        const { value, done } = await events.next();
        if (done) {
          controller.close();
          return;
        }
        controller.enqueue(encoder.encode(sseFrame(value)));
      } catch (error) {
        controller.error(error);
      }
    },
    // The client hung up: stop the generator so its `finally` unsubscribes.
    async cancel() {
      try {
        await events.return(undefined as never);
      } catch {
        // already finished
      }
    },
  });

  return { stream, headers: SSE_HEADERS };
}

/** Where a client is: the thread record's last seq, and its place in a run
 *  stream. On the wire (an SSE `id:`, or a `cursor` query) it is one string,
 *  `<seq> <streamId> <offset>`, with `-` for a part it does not have. */
export interface ThreadCursor {
  seq: number;
  streamId?: string;
  offset?: string | null;
}

/** One frame of a thread's follow: a thread event (a record entry, or a
 *  notice), an item from its run stream, or a SNAPSHOT when the stream the
 *  client was reading is gone. */
export type FollowFrame =
  | { kind: 'thread'; event: AgentEvent }
  | { kind: 'stream'; streamId: string; item: StreamItem }
  | { kind: 'snapshot'; snapshot: ThreadSnapshot };

export function formatCursor(c: ThreadCursor): string {
  return `${c.seq} ${c.streamId || '-'} ${c.offset || '-'}`;
}

/** Reads a cursor. A bare number is a record seq (what older clients send);
 *  anything unreadable is no cursor. */
export function parseCursor(raw: string | null | undefined): ThreadCursor | null {
  if (!raw) return null;
  const [seqPart, streamId, offset] = raw.trim().split(/\s+/);
  const seq = Number(seqPart);
  if (!Number.isFinite(seq)) return null;
  return {
    seq,
    ...(streamId && streamId !== '-' ? { streamId } : {}),
    ...(offset && offset !== '-' ? { offset } : {}),
  };
}

export interface FollowThreadOptions {
  /** Where the client is. Absent, it follows from now: the record from the
   *  start, and the run stream in flight from what the messages lack. */
  cursor?: ThreadCursor | null;
  /** The last message the client has, so a SNAPSHOT carries only newer
   *  ones. */
  lastMessageId?: string;
  signal?: AbortSignal;
}

/** A thread live, as one sequence of frames (§2.2): its record entries and
 *  notices (subscribe first, replay after the cursor, never skip a seq; see
 *  followEvents), and its run streams. The stream the cursor names is read
 *  on from its offset; a RUN_STARTED moves the read to the new segment's
 *  stream; a stream that is gone becomes one SNAPSHOT frame, after which the
 *  follow carries on from the snapshot's stream. */
export async function* followThread(
  deps: RuntimePorts,
  threadId: string,
  options: FollowThreadOptions = {},
): AsyncGenerator<FollowFrame> {
  const outer = options.signal;
  const inner = new AbortController();
  const stop = () => inner.abort();
  outer?.addEventListener('abort', stop, { once: true });
  if (outer?.aborted) inner.abort();

  const queue: FollowFrame[] = [];
  let wake: (() => void) | null = null;
  let failure: unknown;
  const push = (frame: FollowFrame) => {
    queue.push(frame);
    const w = wake;
    wake = null;
    w?.();
  };

  /** The stream being read, and the read's own stop. */
  let reading: { streamId: string; stop: AbortController } | null = null;

  const readStream = (streamId: string, after: string | null, fromCursor: boolean) => {
    if (reading?.streamId === streamId) return;
    reading?.stop.abort();
    const stopRead = new AbortController();
    const onStop = () => stopRead.abort();
    inner.signal.addEventListener('abort', onStop, { once: true });
    reading = { streamId, stop: stopRead };
    void (async () => {
      try {
        for await (const item of deps.streams!.read(streamId, after, stopRead.signal)) {
          push({ kind: 'stream', streamId, item });
        }
      } catch (err) {
        if (!(err instanceof StreamGoneError)) {
          failure = err;
          push(null as never); // wake the consumer to see the failure
          return;
        }
        // Only a stream the client was already reading needs a catch-up:
        // one a RUN_STARTED named was never on its screen.
        if (fromCursor && !stopRead.signal.aborted) {
          const snapshot = await threadSnapshot(deps, threadId, { afterMessageId: options.lastMessageId });
          if (snapshot) {
            push({ kind: 'snapshot', snapshot });
            if (snapshot.stream && !snapshot.stream.end) readStream(snapshot.stream.streamId, snapshot.stream.offset, false);
          }
        }
      } finally {
        inner.signal.removeEventListener('abort', onStop);
      }
    })();
  };

  const cursor = options.cursor ?? null;
  try {
    if (deps.streams) {
      if (cursor?.streamId) {
        readStream(cursor.streamId, cursor.offset ?? null, true);
      } else {
        // No stream cursor: start from what the messages lack, as a
        // snapshot would.
        const current = await snapshotStream(deps, threadId);
        if (current) {
          for (const item of current.items) push({ kind: 'stream', streamId: current.streamId, item });
          if (!current.end) readStream(current.streamId, current.offset, false);
        }
      }
    }

    const thread = followEvents(deps, threadId, { since: cursor?.seq ?? -1, signal: inner.signal });
    void (async () => {
      try {
        for await (const event of thread) {
          push({ kind: 'thread', event });
          if (event.type === 'RUN_STARTED' && event.seq > 0 && deps.streams) {
            const { streamId } = event.payload as { streamId?: string };
            if (streamId) readStream(streamId, null, false);
          }
        }
      } catch (err) {
        failure = err;
        push(null as never);
      }
    })();

    while (!inner.signal.aborted) {
      while (queue.length > 0) {
        const frame = queue.shift();
        if (failure !== undefined) throw failure;
        if (frame) yield frame;
      }
      if (failure !== undefined) throw failure;
      if (inner.signal.aborted) break;
      await new Promise<void>((resolve) => {
        wake = resolve;
        const onAbort = () => resolve();
        inner.signal.addEventListener('abort', onAbort, { once: true });
      });
    }
  } finally {
    outer?.removeEventListener('abort', stop);
    inner.abort(); // stops the record follow (and its subscription) and the stream read
  }
}

/** The SSE frame for a follow frame. A frame that moves the client's
 *  cursor carries it as its `id:`, so a browser that reconnects sends it
 *  back as Last-Event-ID; a notice carries none, and leaves it as it was. */
export function followFrame(frame: FollowFrame, cursor: ThreadCursor): string {
  let moved = false;
  if (frame.kind === 'thread' && frame.event.seq > 0) {
    cursor.seq = frame.event.seq;
    moved = true;
  } else if (frame.kind === 'stream') {
    cursor.streamId = frame.streamId;
    cursor.offset = frame.item.offset;
    moved = true;
  } else if (frame.kind === 'snapshot') {
    cursor.seq = frame.snapshot.lastEventSeq;
    cursor.streamId = frame.snapshot.stream?.streamId;
    cursor.offset = frame.snapshot.stream?.offset ?? null;
    moved = true;
  }
  const data = JSON.stringify(frame);
  return moved ? `id: ${formatCursor(cursor)}\ndata: ${data}\n\n` : `data: ${data}\n\n`;
}

/** A thread's follow, encoded as Server-Sent Events (see followThread). */
export function toFollowSse(
  frames: AsyncGenerator<FollowFrame>,
  cursor: ThreadCursor | null,
  options: { retryMs?: number } = {},
): SseStream {
  const encoder = new TextEncoder();
  const at: ThreadCursor = cursor ? { ...cursor } : { seq: -1 };
  let started = false;
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (!started) {
        started = true;
        if (options.retryMs !== undefined) {
          controller.enqueue(encoder.encode(`retry: ${options.retryMs}\n\n`));
          return;
        }
      }
      try {
        const { value, done } = await frames.next();
        if (done) {
          controller.close();
          return;
        }
        controller.enqueue(encoder.encode(followFrame(value, at)));
      } catch (error) {
        controller.error(error);
      }
    },
    async cancel() {
      try {
        await frames.return(undefined as never);
      } catch {
        // already finished
      }
    },
  });
  return { stream, headers: SSE_HEADERS };
}
