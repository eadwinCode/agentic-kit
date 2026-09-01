import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

/** Tokens spent on this thread (§4) and how full its context is (§2.6).
 *  Read-only — the client refetches it after every finished run. */
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId');
  if (!threadId) {
    return NextResponse.json({ error: 'threadId is required' }, { status: 400 });
  }

  const usage = await runtime.getThreadUsage(threadId);
  if (!usage) return NextResponse.json({ error: 'Thread not found' }, { status: 404 });

  return NextResponse.json(usage);
}
