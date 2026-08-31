import { verifySignatureApprouter } from '@upstash/qstash/nextjs';
import { NextRequest, NextResponse } from 'next/server';
import { waitUntil } from '@vercel/functions';
import { runtime } from '@/lib/runtime';

// The `agent-runs` consumer (§2.8). The message is a dispatch ticket, not an
// execution leash: runs — including parked HITL waits — outlive this HTTP
// response inside the worker. executeWithPolicy redrives transient failures
// and finalizes FAILED when attempts are exhausted.
export async function POST(req: NextRequest) {
  if (!(await verifySignatureApprouter(req))) {
    return new NextResponse('invalid signature', { status: 401 });
  }
  const { threadId, model } = await req.json();

  waitUntil(runtime.engine.executeWithPolicy({ threadId, model }));

  // Ack immediately — at-least-once delivery; double dispatch is a no-op (§3.4)
  return NextResponse.json({ accepted: true });
}
