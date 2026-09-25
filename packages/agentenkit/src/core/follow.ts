import type { RuntimePorts } from '../ports/runtime.js';
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
