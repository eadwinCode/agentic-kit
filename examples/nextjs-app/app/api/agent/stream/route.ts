import { NextRequest } from 'next/server';
import { runtime } from '@/lib/runtime';
import type { AgentEvent } from '@agentic-kit/core';

// SSE distributor (§2.2): replay after the client's cursor, then tail live.
// A parked HITL request carries its own expiry on the queue (§2.5), so this
// connection no longer has to poll for one — it just heals on connect, which
// makes an already-expired approval resolve live in front of the user.
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId')!;
  // EventSource sends Last-Event-ID automatically on auto-reconnect — honor it
  // so clients never replay events they've already seen (§2.2). A malformed
  // cursor falls back to -1 (full replay) rather than silently dropping it.
  const rawCursor = req.headers.get('last-event-id') ?? req.nextUrl.searchParams.get('since');
  const parsedCursor = rawCursor === null ? -1 : Number(rawCursor);
  const since = Number.isFinite(parsedCursor) ? parsedCursor : -1;
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      let lastSeq = since;
      let live = false;
      let closed = false;
      const buffer: AgentEvent[] = [];

      const send = (e: AgentEvent) => {
        if (closed) return;
        if (e.seq !== 0) lastSeq = e.seq;
        controller.enqueue(encoder.encode(`id: ${e.seq}\ndata: ${JSON.stringify(e)}\n\n`));
      };
      const onEvent = (e: AgentEvent) => {
        if (!live) {
          buffer.push(e); // buffer until replay finishes…
          return;
        }
        // seq 0 = bus-only notice (heartbeat / death) — always forward
        if (e.seq === 0 || e.seq > lastSeq) send(e);
      };

      // Subscribe FIRST so events published between replay and tailing are
      // buffered instead of dropped (§2.2) …
      const unsubscribe = await runtime.events.subscribe(threadId, onEvent);
      // §2.5 expiry rides the queue now — parkForApproval schedules it, so it
      // fires with nobody watching. This one heal-on-connect stays as the
      // fallback: it catches threads parked before the timer existed, and any
      // queue adapter that ignores the delay. Cheap no-op when there is
      // nothing pending, and it costs one call per connection rather than a
      // poll per viewer.
      void runtime.hitl.reclaimIfOrphaned(threadId);
      try {
        // …then replay from the durable log, deduped against the buffer …
        for (const e of await runtime.events.since(threadId, since)) {
          if (e.seq > lastSeq) send(e);
        }
        for (const e of buffer.sort((a, b) => a.seq - b.seq)) {
          if (e.seq === 0 || e.seq > lastSeq) send(e);
        }
        live = true;
      } catch (err) {
        await unsubscribe();
        throw err;
      }

      req.signal.addEventListener('abort', () => {
        if (closed) return;
        closed = true;
        void (async () => {
          await unsubscribe();
          // The runtime may already have closed the controller when the
          // browser disconnected; cleanup must never become an unhandled error.
          try {
            controller.close();
          } catch {
            // already closed
          }
        })();
      }, { once: true });
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    },
  });
}
