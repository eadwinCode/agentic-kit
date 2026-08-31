import { NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

/** Thread picker / sidebar: most recent first. */
export async function GET() {
  const threads = await runtime.listThreads();
  return NextResponse.json({ threads });
}
