import { NextRequest, NextResponse } from 'next/server';
import { waitUntil } from '@vercel/functions';
import { runtime, INLINE_WORKER, chat } from '@/lib/runtime';

export async function POST(req: NextRequest) {
  const { threadId, prompt, model, tokenBudget, providerOptions, editMessageId } =
    await req.json();

  // editMessageId (§5.1): replace that user turn, drop everything after it,
  // and answer again from there.
  const result = await chat.run({
    threadId, prompt, model, tokenBudget, providerOptions, editMessageId,
  });
  // chat.run: heal orphans (§2.5) → billing pre-check (§4) → persist user
  // message → state RUNNING → enqueue `agent-runs` (§2.8)
  if (!result.accepted) return NextResponse.json(result, { status: 409 });

  // Local dev convenience (INLINE_WORKER=1): execute the engine in-process
  // with the SAME dispatch ticket the queue would deliver — agent, model,
  // tokenBudget and providerOptions all included, so inline execution and
  // the QStash worker path are byte-identical (§2.8). Production leaves
  // this off — the QStash worker (§5.6) is the executor.
  if (INLINE_WORKER) {
    waitUntil(
      runtime.worker.handleJob({
        threadId: result.threadId,
        // Same run id run() enqueued: the inline worker must carry the
        // dispatch's identity or it cannot be stopped or replaced (§2.1).
        runId: result.runId,
        model: model ?? 'gpt-4o',
        agent: chat.name,
        tokenBudget,
        providerOptions,
      }),
    );
  }

  return NextResponse.json(result, { status: 202 });
}
