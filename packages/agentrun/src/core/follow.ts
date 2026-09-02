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
  /** Published while the replay is still running. */
  const pending: AgentEvent[] = [];
  /** Published once live, waiting for the consumer. */
  const queue: AgentEvent[] = [];
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
    queue.push(event);
    notify();
  });

  const onAbort = () => {
    closed = true;
    notify();
  };
  signal?.addEventListener('abort', onAbort, { once: true });

  /** Rules 2 and 3 in one place, so no caller has to remember them. */
  const admit = (event: AgentEvent): boolean => {
    if (event.seq === 0) return true; // a notice: forward, but do not advance
    if (event.seq <= lastSeq) return false;
    lastSeq = event.seq;
    return true;
  };

  try {
    if (signal?.aborted) return;

    // …then the durable log…
    for (const event of await deps.storage.events.listSince(threadId, since)) {
      if (admit(event)) yield event;
    }

    // …then whatever arrived behind it, in order.
    for (const event of pending.sort((a, b) => a.seq - b.seq)) {
      if (admit(event)) yield event;
    }
    pending.length = 0;
    live = true;

    while (!closed && !signal?.aborted) {
      while (queue.length > 0) {
        const event = queue.shift()!;
        if (admit(event)) yield event;
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
