import type { Storage } from '../ports/storage.js';

/** Whatever a caller needs carried through a run: tenant, user, request id,
 *  feature flags (§2.10). The platform never reads it — it only guarantees
 *  that everything acting on behalf of a run can see it.
 *
 *  Augment it to type your own fields:
 *
 * ```ts
 * declare module 'agentrun' {
 *   interface AgentRunState { orgId: string; userId: string }
 * }
 * ```
 *
 *  Declaration merging rather than a `<TState>` type parameter, which would
 *  otherwise have to appear on Storage, AgentCore, AgentHandle, every tool and
 *  every subagent before it reached the one place it is read. */
export interface AgentRunState {
  [key: string]: unknown;
}

/** Passed to every domain storage call so an implementation can scope a query,
 *  stamp a row, or route to a tenant's database (§2.10). */
export interface StorageContext {
  state: AgentRunState;
  /** The run this call serves. Absent for reads outside a run — a thread
   *  listing, for instance. */
  runId?: string;
}

/** Strips the trailing StorageContext from every method of T. */
type Bound<T> = {
  [K in keyof T]: T[K] extends (...args: [...infer A, StorageContext]) => infer R
    ? (...args: A) => R
    : T[K] extends object
      ? Bound<T[K]>
      : T[K];
};

/** The Storage shape core works against: the caller's implementation with this
 *  run's context already attached. */
export type BoundStorage = Bound<Storage>;

/** Attach a run's context to every storage method once, so core keeps calling
 *  `storage.messages.append(threadId, msg)` and the implementation still
 *  receives the state (§2.10). Threading a parameter through every call site
 *  in the engine instead would put the plumbing in ~60 places and make it
 *  possible to forget it in one. */
export function bindStorage(storage: Storage, ctx: StorageContext): BoundStorage {
  const out: Record<string, unknown> = {};
  for (const [group, methods] of Object.entries(storage)) {
    if (!methods || typeof methods !== 'object') continue;
    const bound: Record<string, unknown> = {};
    for (const [name, fn] of Object.entries(methods as Record<string, unknown>)) {
      bound[name] =
        typeof fn === 'function'
          ? (...args: unknown[]) => (fn as Function).call(methods, ...args, ctx)
          : fn;
    }
    out[group] = bound;
  }
  return out as BoundStorage;
}

/** Give every tool the run's state alongside the SDK's own options (§2.10).
 *
 *  The AI SDK calls `execute(args, { toolCallId, abortSignal, … })`; this adds
 *  `state` to that second argument, so a tool reads it exactly where it already
 *  reads its tool-call id. Applied to the whole toolset, not just the marked
 *  ones — a tool that does not need approval still needs to know which tenant
 *  it is acting for. */
export function withRunState(
  tools: Record<string, any>,
  state: AgentRunState,
): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [name, t] of Object.entries(tools)) {
    out[name] =
      typeof t?.execute === 'function'
        ? { ...t, execute: (args: unknown, opts: object) => t.execute(args, { ...opts, state }) }
        : t;
  }
  return out;
}
