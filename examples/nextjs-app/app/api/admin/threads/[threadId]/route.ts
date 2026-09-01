import { NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

/** One thread opened up: its runs, and every step across them in order (§2.9). */
export async function GET(
  _req: Request,
  { params }: { params: Promise<{ threadId: string }> },
) {
  const { threadId } = await params;
  const detail = await runtime.admin.getThread(threadId);
  if (!detail) return NextResponse.json({ error: 'Thread not found' }, { status: 404 });
  return NextResponse.json(detail);
}
