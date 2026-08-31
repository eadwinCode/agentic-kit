import { NextRequest } from 'next/server';
import { runtime } from '@/lib/runtime';

// SSE distributor (§2.2): replay after the client's cursor, then tail live.
// The subscription doubles as the §2.5 HITL orphan watchdog (death notices +
// heartbeat live inside the reference EventBus adapter).
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId')!;
  const since = Number(req.nextUrl.searchParams.get('since') ?? -1);
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      for (const e of await runtime.events.since(threadId, since)) {
        controller.enqueue(encoder.encode(`id: ${e.seq}\ndata: ${JSON.stringify(e)}\n\n`));
      }

      void runtime.hitl.reclaimIfOrphaned(threadId);

      const unsubscribe = await runtime.events.subscribe(threadId, (event) => {
        controller.enqueue(encoder.encode(`id: ${event.seq}\ndata: ${JSON.stringify(event)}\n\n`));
        if (event.type === 'HITL_ORPHANED') void runtime.hitl.reclaimIfOrphaned(threadId);
      });

      req.signal.addEventListener('abort', () => {
        unsubscribe();
        controller.close();
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
