import { NextRequest, NextResponse } from 'next/server';
import { waitUntil } from '@vercel/functions';
import { runtime, INLINE_WORKER } from '@/lib/runtime';

export async function POST(req: NextRequest) {
  const { threadId, prompt, model } = await req.json();

  const result = await runtime.run({ threadId, prompt, model });
  // runtime.run: heal orphans (§2.5) → billing pre-check (§4) → persist user
  // message → state RUNNING → enqueue `agent-runs` (§2.8)
  if (!result.accepted) return NextResponse.json(result, { status: 409 });

  // Local dev convenience (INLINE_WORKER=1): execute the engine in-process
  // instead of waiting for the queue delivery. Production leaves this off —
  // the QStash worker (§5.6) is the executor.
  if (INLINE_WORKER) {
    waitUntil(runtime.engine.executeWithPolicy({ threadId: result.threadId, model }));
  }

  return NextResponse.json(result, { status: 202 });
}
