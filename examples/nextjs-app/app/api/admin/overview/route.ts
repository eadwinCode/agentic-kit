import { NextRequest, NextResponse } from 'next/server';
import { runtime } from '@/lib/runtime';

/** Everything the dashboard's top half needs, in one read (§2.9). All of it
 *  comes from the platform's own store — this route never touches the app's
 *  own tables. */
export async function GET(req: NextRequest) {
  const hours = Number(req.nextUrl.searchParams.get('hours') ?? 24);
  const since = new Date(Date.now() - hours * 3_600_000);
  return NextResponse.json(await runtime.admin.overview({ since }));
}
