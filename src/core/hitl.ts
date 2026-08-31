import type { RuntimePorts } from '../ports/runtime.js';
import type { RespondInput, RespondResult } from '../ports/runtime.js';
import type { AgentEvent } from './types.js';
import { publish } from './publish.js';
import { reclaimIfOrphaned } from './reclaim.js';

export const HITL_TTL_MS = 15 * 60_000;

const hitlKey = (toolCallId: string) => `agent:hitl:${toolCallId}`;

export interface HitlResponse {
  approved: boolean;
  payload?: unknown;
}

export interface TimeoutOutcome {
  responded: false;
  cancelled: true;
  reason: 'timeout';
}

export interface SuspendInput {
  threadId: string;
  toolCallId: string;
  toolName: string;
  agentId?: string;
  args: unknown;
  inputSchema?: unknown;
  ttlMs: number;
  signal: AbortSignal;
}

/** Parks the suspended tool call until the user responds, the TTL expires, or
 *  the run aborts. Returns null on abort; a TimeoutOutcome on TTL expiry (§2.5). */
export async function waitForEvent(
  deps: RuntimePorts,
  toolCallId: string,
  opts: { ttlMs: number; signal?: AbortSignal },
): Promise<HitlResponse | null> {
  const deadline = Date.now() + opts.ttlMs;

  while (Date.now() < deadline) {
    if (opts.signal?.aborted) return null; // stop teardown handled by the engine
    const raw = await deps.kv.get(hitlKey(toolCallId));
    if (raw) {
      await deps.kv.del(hitlKey(toolCallId));
      return JSON.parse(raw) as HitlResponse;
    }
    await sleep(1_000); // 1s poll — adapter-friendly, no blocking ops
  }
  return null; // TTL expired: the user had no response
}

/** Engine-side suspension around any `requiresConfirmation` tool (§2.5).
 *  A park flips durable + cached thread state to WAITING_FOR_INPUT, publishes
 *  the request, and blocks this tool call in place. Returns:
 *    - HitlResponse   → the user answered (approved / denied)
 *    - TimeoutOutcome → TTL expired; the timeout tool result is already appended
 *    - null           → aborted (stop) — the engine tears down */
export async function suspendForApproval(
  deps: RuntimePorts,
  i: SuspendInput,
): Promise<HitlResponse | TimeoutOutcome | null> {
  const { threadId, toolCallId, toolName, ttlMs, signal } = i;

  await deps.kv.set(`agent:state:${threadId}`, 'WAITING_FOR_INPUT');
  await deps.storage.threads.setState(threadId, 'WAITING_FOR_INPUT');
  await publish(deps, threadId, 'INPUT_REQUIRED', {
    toolCallId,
    toolName,
    agentId: i.agentId ?? null,
    arguments: i.args,
    inputSchema: i.inputSchema ?? null,
  });
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'WAITING_FOR_INPUT' });

  const response = await waitForEvent(deps, toolCallId, { ttlMs, signal });

  if (!response) {
    if (signal.aborted) return null; // stop during the wait (§2.1) — engine tears down

    // TTL expired: flip the thread back to RUNNING and publish; the timeout
    // tool result itself is recorded by the SDK in response.messages and
    // persisted at finish (§5.6) — no manual append here.
    await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');
    await deps.storage.threads.setState(threadId, 'RUNNING');
    await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });
    await publish(deps, threadId, 'INPUT_EXPIRED', { toolCallId });
    return { responded: false, cancelled: true, reason: 'timeout' };
  }

  await deps.kv.set(`agent:state:${threadId}`, 'RUNNING');
  await deps.storage.threads.setState(threadId, 'RUNNING');
  await publish(deps, threadId, 'STATE_CHANGE', { state: 'RUNNING' });
  // The SDK records the tool result (approved payload / { denied: true }) in
  // response.messages; the engine persists it at finish (§5.6).
  return response;
}

/** The §5.4 behavior: heal orphans first (§2.5), then deliver the response to
 *  the parked waitForEvent via its handoff key. Answer-latest policy: only the
 *  most recent pending INPUT_REQUIRED is answerable (§2.7). */
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

  // Remaining-TTL expiry so a stale response can never leak into a later run
  const remainingSec = Math.max(
    60,
    Math.floor((deps.config.hitlTtlMs - (Date.now() - new Date(pending!.createdAt).getTime())) / 1000),
  );
  await deps.kv.set(
    hitlKey(input.toolCallId),
    JSON.stringify({ approved: input.approved, payload: input.payload }),
    { exSeconds: remainingSec },
  );
  // Bus-only fast-path notice (seq 0 = never persisted) — the parked waiter
  // polls the key either way (§2.5)
  await deps.bus.publish(input.threadId, {
    threadId: input.threadId,
    seq: 0,
    type: 'HITL_RESPONSE',
    payload: { toolCallId: input.toolCallId, approved: input.approved },
    createdAt: new Date(),
  } as AgentEvent);

  return { delivered: true };
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
