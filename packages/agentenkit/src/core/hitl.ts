import { randomUUID } from 'node:crypto';
import type { RuntimePorts } from '../ports/runtime.js';
import type { RespondInput, RespondResult } from '../ports/runtime.js';
import type { AgentEvent, NestedDescriptor, ResumeInfo } from './types.js';
import { publish, setThreadState } from './publish.js';
import { currentRunId } from './keys.js';
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

/** One tool call left waiting on an approval further down (§2.7). A park by
 *  the main agent has none; a park inside a nested run has one per level, the
 *  innermost waiter first. `agentId` names the stream the waiting call lives
 *  in — `null` for the main agent. */
export interface HitlFrame {
  agentId: string | null;
  toolCallId: string;
  /** How to re-enter the owner's loop when this frame unwinds. Absent for the
   *  main agent, whose loop the engine re-enters itself. */
  nested?: NestedDescriptor;
}

/** Marks a park raised by a `requiresConfirmation` tool: a human decides.
 *  Any other reason is a tool that parked itself (§2.5). */
export const REASON_APPROVAL = 'approval';

/** A tool's own park (§2.5). The tool has started something it must not wait
 *  for in-process (a build, a render, a long job) and asks to be resumed when
 *  it is done: the run lock and the worker are released, the request is
 *  durable, and `respond(toolCallId, payload)` runs the SAME tool call again
 *  with the payload on its `approval`. Nothing waits meanwhile. */
export interface ParkRequest {
  /** Names what the run is waiting on ("job", say). It rides the
   *  INPUT_REQUIRED event, so a UI can tell it from an approval and skip the
   *  card. Empty means REASON_APPROVAL. */
  reason?: string;
  /** Published as the request's arguments: the job id, a URL, whatever the
   *  responder needs to find the work. */
  payload?: unknown;
  /** How long the park stays answerable; absent keeps `config.hitlTtlMs`. A
   *  job that outlives it resumes as expired, like an unanswered approval. */
  ttlMs?: number;
}

/** What a tool throws to park itself:
 *
 * ```ts
 * throw parkForInput({ reason: 'job', payload: { jobId }, ttlMs: 30 * 60_000 });
 * ```
 *
 *  The engine turns it into a durable park. A resumed call sees
 *  `ctx.approval` set and must return a result; a resumed call that parks
 *  again is reported to the model as a tool error. */
export class ToolParkedError extends Error {
  constructor(public readonly request: ParkRequest) {
    super(`tool parked: ${request.reason ?? REASON_APPROVAL}`);
    this.name = 'ToolParkedError';
  }
}

export function parkForInput(request: ParkRequest = {}): ToolParkedError {
  return new ToolParkedError(request);
}

export interface ParkInput {
  threadId: string;
  toolCallId: string;
  toolName: string;
  args: unknown;
  agentId?: string | null;
  /** REASON_APPROVAL, or what a self-parking tool said (§2.5). */
  reason?: string;
  /** Overrides `config.hitlTtlMs` for this park; absent keeps it. */
  ttlMs?: number;
  /** The calls waiting on this answer, innermost first (§2.7). Empty for a
   *  main-agent park, which reduces this to the plain §2.5 flow. */
  frames?: HitlFrame[];
  /** The nested run that raised this park; absent when the main agent did. */
  nested?: NestedDescriptor;
  /** Dispatch ticket persisted in the INPUT_REQUIRED payload (§2.5) */
  resume: ResumeInfo;
}

/** Wrap every tool so a call can park (§2.5) instead of blocking. A marked
 *  tool parks BEFORE it runs: the request is persisted as INPUT_REQUIRED and
 *  the wrapper returns the park sentinel; the real tool runs when a human
 *  answers (see resumePendingHitl), in whichever stream owns it. Any other
 *  tool may park ITSELF by throwing `parkForInput(...)` after starting work
 *  it cannot wait for; the same machinery resumes it with the responder's
 *  payload. Nothing blocks either way.
 *
 *  Shared by the main agent and every nested run (§2.7): the only difference
 *  is the `agentId` asking and the `frames` waiting on the answer. */
export function withHitl(
  deps: RuntimePorts,
  threadId: string,
  tools: Record<string, any>,
  ctx: {
    resume: ResumeInfo;
    agentId?: string | null;
    frames?: HitlFrame[];
    nested?: NestedDescriptor;
  },
): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [name, t] of Object.entries(tools)) {
    const park = async (toolCallId: string, args: unknown, request: ParkRequest) => {
      await parkForApproval(deps, {
        threadId,
        toolCallId,
        toolName: name,
        args,
        agentId: ctx.agentId ?? null,
        frames: ctx.frames ?? [],
        ...(ctx.nested ? { nested: ctx.nested } : {}),
        resume: ctx.resume,
        ...(request.reason ? { reason: request.reason } : {}),
        ...(request.ttlMs !== undefined ? { ttlMs: request.ttlMs } : {}),
      });
      return { [HITL_PARKED]: toolCallId };
    };
    if ((t as any)?.requiresConfirmation) {
      out[name] = {
        ...t,
        execute: (args: unknown, opts: { toolCallId?: string }) =>
          park(opts?.toolCallId ?? randomUUID(), args, { reason: REASON_APPROVAL }),
      };
      continue;
    }
    const execute = (t as any)?.execute as
      | ((args: unknown, opts: any) => Promise<unknown>)
      | undefined;
    if (!execute) {
      out[name] = t;
      continue;
    }
    out[name] = {
      ...t,
      execute: async (args: unknown, opts: { toolCallId?: string; approval?: unknown }) => {
        try {
          return await execute(args, opts);
        } catch (err) {
          if (!(err instanceof ToolParkedError)) throw err;
          // A resumed call that parks again has nowhere to go: the verdict
          // for this call is being consumed right now.
          if (opts?.approval) {
            throw new Error(`tool ${name} parked again on resume; a resumed call must return`);
          }
          return park(opts?.toolCallId ?? randomUUID(), err.request.payload ?? null, err.request);
        }
      },
    };
  }
  return out;
}

/** The §2.5 suspension as a durable state transition — NO process waits.
 *  Flips WAITING_FOR_INPUT on both homes and appends INPUT_REQUIRED to the
 *  replayable event log (with the resume ticket). The engine then ends the
 *  run segment; /api/agent/respond (or the expiry job below) resumes it via
 *  the queue.
 *
 *  The park also schedules its OWN expiry: one delayed dispatch of the same
 *  run, timed for just after the TTL. Without it the deadline only exists
 *  while somebody happens to be watching the thread — close the tab and the
 *  approval never times out at all. The delayed job holds no process; the
 *  queue holds it, exactly like the original dispatch (§2.8).
 *
 *  It carries the PARKED run's id, so the answer and the expiry are two
 *  deliveries of one run: whichever resolves the park first wins, and the
 *  run lock makes the other a no-op. */
export async function parkForApproval(deps: RuntimePorts, i: ParkInput): Promise<void> {
  const ttlMs = i.ttlMs && i.ttlMs > 0 ? i.ttlMs : deps.config.hitlTtlMs;
  const expiresAt = new Date(Date.now() + ttlMs).toISOString();
  await deps.kv.set(`agent:state:${i.threadId}`, 'WAITING_FOR_INPUT');
  await setThreadState(deps, i.threadId, 'WAITING_FOR_INPUT', i.resume.model);
  await publish(deps, i.threadId, 'INPUT_REQUIRED', {
    toolCallId: i.toolCallId,
    toolName: i.toolName,
    agentId: i.agentId ?? null,
    arguments: i.args,
    inputSchema: null,
    // The unwind chain (§2.7) — empty for a main-agent park.
    frames: i.frames ?? [],
    ...(i.nested ? { nested: i.nested } : {}),
    resume: i.resume,
    // REASON_APPROVAL, or the self-parking tool's own word, and the park's
    // own deadline (§2.5). A park recorded before these existed reads as
    // an approval on the config's TTL.
    reason: i.reason ?? REASON_APPROVAL,
    expiresAt,
  });
  await publish(deps, i.threadId, 'STATE_CHANGE', { state: 'WAITING_FOR_INPUT' });

  // Best-effort, and deliberately last. The park is ALREADY durable by this
  // point — state flipped on both homes, INPUT_REQUIRED on the event log — so
  // a queue that cannot schedule must not be allowed to throw back through the
  // tool call and fail the run. Reclamation (§2.5) covers the thread instead.
  //
  // Arriving early is equally harmless: an unexpired, unanswered request
  // resolves to nothing and the job is a no-op (see resumePendingHitl).
  try {
    await deps.queue.enqueue(
      {
        threadId: i.threadId,
        runId: await currentRunId(deps, i.threadId),
        model: i.resume.model,
        agent: i.resume.agent,
        ...(i.resume.tokenBudget !== undefined ? { tokenBudget: i.resume.tokenBudget } : {}),
        ...(i.resume.costBudgetMicros !== undefined
          ? { costBudgetMicros: i.resume.costBudgetMicros }
          : {}),
        ...(i.resume.providerOptions ? { providerOptions: i.resume.providerOptions } : {}),
        ...(i.resume.state ? { state: i.resume.state } : {}),
      },
      { delaySeconds: Math.ceil((ttlMs + deps.config.reclaimGraceMs) / 1000) },
    );
  } catch {
    // No expiry scheduled — the thread still heals on first touch (§2.5).
  }
}

/** The pending request behind a WAITING_FOR_INPUT thread, hydrated from the
 *  durable event log (§2.5). */
export interface PendingHitl {
  toolCallId: string;
  toolName: string;
  agentId: string | null;
  arguments: unknown;
  /** Calls waiting on this answer, innermost first (§2.7). */
  frames: HitlFrame[];
  /** The nested run that raised it; absent for a main-agent park. */
  nested?: NestedDescriptor;
  /** Epoch ms of the INPUT_REQUIRED event — the TTL clock (§2.5) */
  requestedAt: number;
  /** The park's own deadline, epoch ms; absent on a park recorded before
   *  per-park TTLs, which `hitlDeadline` reads as requestedAt + hitlTtlMs. */
  expiresAt?: number;
  /** REASON_APPROVAL, or the self-parking tool's word (§2.5). */
  reason?: string;
}

/** When a park stops being answerable, epoch ms. */
export function hitlDeadline(p: PendingHitl, config: { hitlTtlMs: number }): number {
  return p.expiresAt ?? p.requestedAt + config.hitlTtlMs;
}

/** Whether a human is the responder. */
export function isApprovalPark(p: Pick<PendingHitl, 'reason'>): boolean {
  return !p.reason || p.reason === REASON_APPROVAL;
}

export async function loadPendingHitl(
  deps: RuntimePorts,
  threadId: string,
): Promise<PendingHitl | null> {
  const pending = await deps.storage.events.latest(threadId, 'INPUT_REQUIRED');
  if (!pending) return null;
  return fromInputRequired(pending);
}

function fromInputRequired(pending: AgentEvent): PendingHitl {
  const p = pending.payload as {
    toolCallId: string;
    toolName: string;
    agentId?: string | null;
    arguments?: unknown;
    frames?: HitlFrame[];
    nested?: NestedDescriptor;
    reason?: string;
    expiresAt?: string;
  };
  const expiresAt = p.expiresAt ? new Date(p.expiresAt).getTime() : NaN;
  return {
    toolCallId: p.toolCallId,
    toolName: p.toolName,
    agentId: p.agentId ?? null,
    arguments: p.arguments,
    // A park recorded before frames existed unwinds as a main-agent park.
    frames: p.frames ?? [],
    ...(p.nested ? { nested: p.nested } : {}),
    requestedAt: new Date(pending.createdAt).getTime(),
    ...(Number.isFinite(expiresAt) ? { expiresAt } : {}),
    ...(p.reason ? { reason: p.reason } : {}),
  };
}

/** Every approval on the thread that is still open (§2.7).
 *
 *  Derived from durable state, never cached: a request is settled once a tool
 *  result carries its `toolCallId` — in whichever stream owns it — or an
 *  INPUT_EXPIRED event names it. Both are already written on the settling
 *  path. A cached set would be a read-modify-write race between exactly the
 *  concurrent siblings this exists to serve, since Kv has no set operations. */
export async function loadOpenHitls(
  deps: RuntimePorts,
  threadId: string,
): Promise<PendingHitl[]> {
  const requested = await deps.storage.events.listByType(threadId, 'INPUT_REQUIRED');
  if (requested.length === 0) return [];

  const expired = new Set(
    (await deps.storage.events.listByType(threadId, 'INPUT_EXPIRED')).map(
      (e) => (e.payload as { toolCallId?: string } | null)?.toolCallId,
    ),
  );
  const answered = new Set<string>();
  for (const m of await deps.storage.messages.list(threadId, undefined)) {
    if (m.role !== 'tool') continue;
    for (const part of Array.isArray(m.content) ? m.content : []) {
      const id = (part as { toolCallId?: string } | null)?.toolCallId;
      if (id) answered.add(id);
    }
  }

  return requested
    .map((e) => fromInputRequired(e))
    .filter((p) => !answered.has(p.toolCallId) && !expired.has(p.toolCallId));
}

/** The §5.4 behavior: heal orphans first (§2.5), then record the answer in
 *  the handoff key and resume the run segment via the queue (§2.8) — the
 *  resumed worker appends the tool result and continues the loop.
 *  Answer-latest policy: only the most recent pending INPUT_REQUIRED is
 *  answerable (§2.7). */
export async function respond(deps: RuntimePorts, input: RespondInput): Promise<RespondResult> {
  await reclaimIfOrphaned(deps, input.threadId);

  const thread = await deps.storage.threads.get(input.threadId);
  // ANY open request is answerable, not just the newest (§2.7): one parent
  // step can park several nested runs at once, and each is answered on its
  // own. The run resumes when the last of them is settled.
  const open = await loadOpenHitls(deps, input.threadId);
  const match = open.find((p) => p.toolCallId === input.toolCallId);

  if (thread?.state !== 'WAITING_FOR_INPUT' || !match) {
    return { delivered: false, error: 'No matching pending input request' };
  }

  // Remaining-TTL expiry so a stale answer can never outlive its request: the
  // key vanishing is what makes the resumed segment treat the request as
  // unanswered (§2.5)
  const remainingSec = Math.max(
    60,
    Math.floor((hitlDeadline(match, deps.config) - Date.now()) / 1000),
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
  const requested = await deps.storage.events.listByType(input.threadId, 'INPUT_REQUIRED');
  const event = requested.find(
    (e) => (e.payload as { toolCallId?: string } | null)?.toolCallId === input.toolCallId,
  );
  const resume = (event?.payload as any)?.resume as ResumeInfo | undefined;
  // REUSE the parked run's id, never mint a new one: this dispatch and the
  // park's expiry job are the same run, and the lock must be able to tell
  // that. Bumping here would let both run and reply twice (§2.5).
  const runId = await currentRunId(deps, input.threadId);
  await deps.queue.enqueue({
    threadId: input.threadId,
    runId,
    model: resume?.model ?? thread.model,
    ...(resume
      ? {
          agent: resume.agent,
          ...(resume.tokenBudget !== undefined ? { tokenBudget: resume.tokenBudget } : {}),
          ...(resume.costBudgetMicros !== undefined
            ? { costBudgetMicros: resume.costBudgetMicros }
            : {}),
          ...(resume.providerOptions ? { providerOptions: resume.providerOptions } : {}),
          // The answer resumes the SAME run, so it must scope storage the same
          // way the parked segment did (§2.10).
          ...(resume.state ? { state: resume.state } : {}),
        }
      : {}),
  });

  return { delivered: true };
}
