import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';
import type { ExecutionState } from 'agentenkit';

/** Threads with their runs rolled up (§2.9) — the dashboard's top level, since
 *  a thread is what a person recognises. */
export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams;
  const state = q.getAll('state') as ExecutionState[];
  const hours = Number(q.get('hours') ?? 24);

  const threads = await runtime.admin.listThreads({
    ...(state.length ? { state } : {}),
    since: new Date(Date.now() - hours * 3_600_000),
    limit: Number(q.get('limit') ?? 100),
  });
  return NextResponse.json({ threads });
}
