import { NextRequest } from 'next/server';
import { runtime } from '@/lib/runtime';

// SSE distributor (§2.2). The follow — the thread record from the cursor, the
// run stream from its offset, one SNAPSHOT when that stream is gone — lives
// in the runtime, so this handler is a cursor and a Response.
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId')!;

  // Where the client is: the id of the last frame it got. EventSource sends
  // it as Last-Event-ID when it reconnects by itself; the hook puts it in
  // the query when it opens the stream. It names the thread record's seq
  // and the place in the run stream, and the runtime reads it as it is.
  const cursor =
    req.headers.get('last-event-id') ??
    req.nextUrl.searchParams.get('cursor') ??
    req.nextUrl.searchParams.get('since'); // a bare record seq, from an older client
  // So a catch-up after a long gap carries only the messages the tab lacks.
  const lastMessageId = req.nextUrl.searchParams.get('lastMessageId') ?? undefined;

  // §2.5 expiry rides the queue now, so this is only the fallback: it catches
  // threads parked before the timer existed, and any queue adapter that
  // ignores the delay. Cheap when there is nothing pending, and it costs one
  // call per connection rather than a poll per viewer.
  void runtime.hitl.reclaimIfOrphaned(threadId);

  const { stream, headers } = runtime.events.sse(threadId, {
    cursor,
    lastMessageId,
    signal: req.signal,
  });

  return new Response(stream, { headers });
}
