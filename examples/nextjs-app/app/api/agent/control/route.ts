import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

// One stop button: runtime.stop is the single `state → CANCELLED` write (§2.1)
export async function POST(req: NextRequest) {
  const { threadId } = await req.json();
  const result = await runtime.stop(threadId);
  return NextResponse.json(result, { status: result.accepted ? 200 : 409 });
}
