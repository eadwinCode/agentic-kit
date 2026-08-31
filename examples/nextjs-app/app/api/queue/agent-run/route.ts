import { verifySignatureApprouter } from '@upstash/qstash/nextjs';
import { NextRequest, NextResponse } from 'next/server';
import { waitUntil } from '@vercel/functions';
import { runtime } from '@/lib/runtime';

// The `agent-runs` consumer (§2.8). The message is a dispatch ticket, not an
// execution leash: runs — including parked HITL waits — outlive this HTTP
// response inside the worker. executeWithPolicy redrives transient failures
// and finalizes FAILED when attempts are exhausted.
async function handler(req: NextRequest) {
  const { threadId, model } = await req.json();

  waitUntil(runtime.engine.executeWithPolicy({ threadId, model }));

  // Ack immediately. Delivery is at-least-once, so double dispatch is possible;
  // the per-thread run lock (§3.4) makes it a no-op.
  return NextResponse.json({ accepted: true });
}

// Signature verification WRAPS the handler — only genuine QStash deliveries
// ever reach the runtime (§2.8).
export const POST = verifySignatureApprouter(handler);
