import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

export async function POST(req: NextRequest) {
  const { threadId, prompt, model } = await req.json();

  const result = await runtime.run({ threadId, prompt, model });
  // runtime.run: heal orphans (§2.5) → billing pre-check (§4) → persist user
  // message → state RUNNING → enqueue `agent-runs` (§2.8)
  return NextResponse.json(result, { status: result.accepted ? 202 : 409 });
}
