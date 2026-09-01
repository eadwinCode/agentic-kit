import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

export async function POST(req: NextRequest) {
  const body = await req.json();
  const result = await runtime.hitl.respond(body);
  // runtime.hitl.respond: heal orphans (§2.5) → validate thread WAITING_FOR_INPUT
  // + latest pending INPUT_REQUIRED → write the handoff key (remaining-TTL) →
  // enqueue the resume job so a worker appends the tool result and continues
  // the run (§2.5, §2.8)
  return NextResponse.json(result, { status: result.delivered ? 200 : 409 });
}
