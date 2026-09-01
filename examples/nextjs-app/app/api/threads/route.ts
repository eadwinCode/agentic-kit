import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

/** Thread picker / sidebar: most recent first. */
export async function GET() {
  const threads = await runtime.listThreads();
  return NextResponse.json({ threads });
}

/** Delete a thread and everything that follows it (§3.2): messages, events,
 *  usage rows, subagent runs, and the thread's hot kv keys. Refused with 409
 *  while a run is active — stop the thread first. */
export async function DELETE(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId');
  if (!threadId) {
    return NextResponse.json({ accepted: false, error: 'threadId is required' }, { status: 400 });
  }
  const result = await runtime.deleteThread(threadId);
  const status = result.accepted ? 200 : result.error === 'Thread not found' ? 404 : 409;
  return NextResponse.json(result, { status });
}
