import { NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

/** One run in full: its steps, the nested runs beneath it, and its event
 *  timeline with the token firehose stripped out (§2.9). */
export async function GET(
  _req: Request,
  { params }: { params: Promise<{ runId: string }> },
) {
  const { runId } = await params;
  const detail = await runtime.admin.getRun(runId);
  if (!detail) return NextResponse.json({ error: 'Run not found' }, { status: 404 });
  return NextResponse.json(detail);
}
