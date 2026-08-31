import { NextRequest } from 'next/server';
import { runtime } from '@/lib/runtime';
import type { AgentEvent } from '@agent/core';

// SSE distributor (§2.2): replay after the client's cursor, then tail live.
// The subscription doubles as the §2.5 HITL orphan watchdog — death notices
// trigger reclamation instantly, and RedisBus HEARTBEAT notices (every 60s)
// re-check orphans in case a death notice was published to zero subscribers.
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
      const buffer: AgentEvent[] = [];

      const send = (e: AgentEvent) => {
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
        void (async () => {
          await unsubscribe();
          controller.close();
        })();
      });
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
