import { NextRequest } from 'next/server';
import { runtime } from '@/lib/runtime';

// SSE distributor (§2.2). The replay-then-tail dance — subscribe first, never
// re-send at or below the cursor, forward bus-only notices without moving it —
// lives in the runtime, so this handler is a cursor and a Response.
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId')!;

  // EventSource sends Last-Event-ID automatically on auto-reconnect; honour it
  // so a client never replays what it has already seen. A malformed cursor
  // falls back to a full replay rather than being silently dropped.
  const raw = req.headers.get('last-event-id') ?? req.nextUrl.searchParams.get('since');
  const parsed = raw === null ? -1 : Number(raw);
  const since = Number.isFinite(parsed) ? parsed : -1;

  // §2.5 expiry rides the queue now, so this is only the fallback: it catches
  // threads parked before the timer existed, and any queue adapter that
  // ignores the delay. Cheap when there is nothing pending, and it costs one
  // call per connection rather than a poll per viewer.
  void runtime.hitl.reclaimIfOrphaned(threadId);

  const { stream, headers } = runtime.events.sse(threadId, {
    since,
    signal: req.signal,
  });

  return new Response(stream, { headers });
}
