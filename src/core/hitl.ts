import type { RuntimePorts } from '../ports/runtime.js';
import type { RespondInput, RespondResult } from '../ports/runtime.js';
import type { AgentEvent, ResumeInfo } from './types.js';
import { publish } from './publish.js';
import { claimRun } from './keys.js';
import { reclaimIfOrphaned } from './reclaim.js';

export const HITL_TTL_MS = 15 * 60_000;

/** Result value a parked `requiresConfirmation` tool returns (§2.5). The
 *  engine scans a step's tool results for this marker to end the run segment;
 *  it is never persisted as a tool result — the resumed segment appends the
 *  user's verdict (or the timeout denial) instead. */
export const HITL_PARKED = '__hitl_parked__';

export const hitlKey = (toolCallId: string) => `agent:hitl:${toolCallId}`;

export interface HitlResponse {
  approved: boolean;
  payload?: unknown;
}

export interface ParkInput {
  threadId: string;
  toolCallId: string;
  toolName: string;
  args: unknown;
  agentId?: string | null;
  /** Dispatch ticket persisted in the INPUT_REQUIRED payload (§2.5) */
  resume: ResumeInfo;
}

/** The §2.5 suspension as a durable state transition — NO process waits.
 *  Flips WAITING_FOR_INPUT on both homes and appends INPUT_REQUIRED to the
 *  replayable event log (with the resume ticket). The engine then ends the
 *  run segment; /api/agent/respond (or TTL expiry, §2.5) resumes it via the
 *  queue. */
export async function parkForApproval(deps: RuntimePorts, i: ParkInput): Promise<void> {
  await deps.kv.set(`agent:state:${i.threadId}`, 'WAITING_FOR_INPUT');
  await deps.storage.threads.setState(i.threadId, 'WAITING_FOR_INPUT');
  await publish(deps, i.threadId, 'INPUT_REQUIRED', {
    toolCallId: i.toolCallId,
    toolName: i.toolName,
    agentId: i.agentId ?? null,
    arguments: i.args,
    inputSchema: null,
    resume: i.resume,
  });
  await publish(deps, i.threadId, 'STATE_CHANGE', { state: 'WAITING_FOR_INPUT' });
}

/** The pending request behind a WAITING_FOR_INPUT thread, hydrated from the
 *  durable event log (§2.5). */
export interface PendingHitl {
  toolCallId: string;
  toolName: string;
  agentId: string | null;
  arguments: unknown;
  /** Epoch ms of the INPUT_REQUIRED event — the TTL clock (§2.5) */
  requestedAt: number;
}

export async function loadPendingHitl(
  deps: RuntimePorts,
  threadId: string,
): Promise<PendingHitl | null> {
  const pending = await deps.storage.events.latest(threadId, 'INPUT_REQUIRED');
  if (!pending) return null;
  const p = pending.payload as {
    toolCallId: string;
    toolName: string;
    agentId?: string | null;
    arguments?: unknown;
  };
  return {
    toolCallId: p.toolCallId,
    toolName: p.toolName,
    agentId: p.agentId ?? null,
    arguments: p.arguments,
    requestedAt: new Date(pending.createdAt).getTime(),
  };
}

/** The §5.4 behavior: heal orphans first (§2.5), then record the answer in
 *  the handoff key and resume the run segment via the queue (§2.8) — the
 *  resumed worker appends the tool result and continues the loop.
 *  Answer-latest policy: only the most recent pending INPUT_REQUIRED is
 *  answerable (§2.7). */
export async function respond(deps: RuntimePorts, input: RespondInput): Promise<RespondResult> {
  await reclaimIfOrphaned(deps, input.threadId);

  const thread = await deps.storage.threads.get(input.threadId);
  const pending = await deps.storage.events.latest(input.threadId, 'INPUT_REQUIRED');

  if (
    thread?.state !== 'WAITING_FOR_INPUT' ||
    (pending?.payload as any)?.toolCallId !== input.toolCallId
  ) {
    return { delivered: false, error: 'No matching pending input request' };
  }

  // Remaining-TTL expiry so a stale answer can never outlive its request: the
  // key vanishing is what makes the resumed segment treat the request as
  // unanswered (§2.5)
  const remainingSec = Math.max(
    60,
    Math.floor((deps.config.hitlTtlMs - (Date.now() - new Date(pending!.createdAt).getTime())) / 1000),
  );
  await deps.kv.set(
    hitlKey(input.toolCallId),
    JSON.stringify({ approved: input.approved, payload: input.payload }),
    { exSeconds: remainingSec },
  );
  // Bus-only fast-path notice (seq 0 = never persisted) for live UIs (§2.5)
  await deps.bus.publish(input.threadId, {
    threadId: input.threadId,
    seq: 0,
    type: 'HITL_RESPONSE',
    payload: { toolCallId: input.toolCallId, approved: input.approved },
    createdAt: new Date(),
  } as AgentEvent);

  // Resume the run segment through the queue — same dispatch path as the
  // original run, rebuilt from the ticket persisted in the event payload.
  // A legacy park without a ticket falls back to the default handle.
  const resume = (pending!.payload as any).resume as ResumeInfo | undefined;
  const runId = await claimRun(deps, input.threadId);
  await deps.queue.enqueue({
    threadId: input.threadId,
    runId,
    model: resume?.model ?? thread.model,
    ...(resume
      ? {
          agent: resume.agent,
          ...(resume.tokenBudget !== undefined ? { tokenBudget: resume.tokenBudget } : {}),
          ...(resume.providerOptions ? { providerOptions: resume.providerOptions } : {}),
        }
      : {}),
  });

  return { delivered: true };
}
