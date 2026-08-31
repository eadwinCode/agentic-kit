import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

/** Durable client hydration before SSE resumes from the returned event cursor. */
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get('threadId');
  if (!threadId) {
    return NextResponse.json({ error: 'threadId is required' }, { status: 400 });
  }

  const snapshot = await runtime.getThreadSnapshot(threadId);
  if (!snapshot) {
    return NextResponse.json({ error: 'Thread not found' }, { status: 404 });
  }

  return NextResponse.json(snapshot);
}
