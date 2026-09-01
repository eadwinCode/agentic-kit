import { NextRequest, NextResponse } from 'next/server';
import { chat } from '@/lib/runtime';

// One stop button: the handle's stop is the single `state → CANCELLED` write (§2.1)
export async function POST(req: NextRequest) {
  const { threadId } = await req.json();
  const result = await chat.stop(threadId);
  return NextResponse.json(result, { status: result.accepted ? 200 : 409 });
}
