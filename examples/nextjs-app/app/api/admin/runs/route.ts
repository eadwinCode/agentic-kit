import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';
import type { ExecutionState } from 'agentenkit';

/** The run list (§2.9). `state` may repeat: ?state=FAILED&state=CANCELLED. */
export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams;
  const state = q.getAll('state') as ExecutionState[];
  const hours = Number(q.get('hours') ?? 24);

  const runs = await runtime.admin.listRuns({
    ...(state.length ? { state } : {}),
    ...(q.get('agent') ? { agent: q.get('agent')! } : {}),
    since: new Date(Date.now() - hours * 3_600_000),
    limit: Number(q.get('limit') ?? 100),
  });
  return NextResponse.json({ runs });
}
