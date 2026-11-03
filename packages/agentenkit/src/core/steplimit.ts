/** The wall-time limit of one step (§2.8). It starts at `stepTimeoutMs`.
 *  While a call to a tool with its own `timeoutMs` runs, the end moves out to
 *  cover it, so one slow tool gets the time it needs without raising
 *  `stepTimeoutMs` for every step of every run. Once the call returns, the
 *  step is back to `stepTimeoutMs`, counted from then. */
export class StepLimit {
  private timer: ReturnType<typeof setTimeout> | undefined;
  /** When `stepTimeoutMs` runs out: from the step's start, or from the last
   *  timed call's return when that is later. 0: no limit for the step,
   *  though each timed call's own limit still holds. */
  private baseEnd = 0;
  private end = 0;
  /** The timed calls running now, by id. */
  private readonly calls = new Map<number, { end: number; limit: number }>();
  private nextId = 0;
  private done = false;
  /** What set the end, and what the error says the step ran past. */
  limit: number;
  expired = false;

  constructor(private readonly abort: AbortController, readonly base: number) {
    this.limit = base;
    if (base > 0) {
      this.baseEnd = Date.now() + base;
      this.arm(this.baseEnd, base);
    }
  }

  private arm(end: number, limit: number): void {
    clearTimeout(this.timer);
    this.end = end;
    this.limit = limit;
    this.timer = setTimeout(() => this.expire(limit), end - Date.now());
  }

  /** Set the end to the latest of `stepTimeoutMs` and the timed calls
   *  running now. */
  private rearm(): void {
    if (this.done || this.expired || this.baseEnd === 0) return;
    let end = this.baseEnd;
    let limit = this.base;
    for (const c of this.calls.values()) {
      if (c.end > end) ({ end, limit } = c);
    }
    if (end !== this.end) this.arm(end, limit);
  }

  /** Note a timed call that may run for `ms`, and return its id. */
  begin(ms: number): number {
    const id = ++this.nextId;
    this.calls.set(id, { end: Date.now() + ms, limit: ms });
    this.rearm();
    return id;
  }

  /** The call has returned: the step has `stepTimeoutMs` from now to finish,
   *  unless other timed calls still run longer. */
  finish(id: number): void {
    this.calls.delete(id);
    if (this.baseEnd !== 0) this.baseEnd = Math.max(this.baseEnd, Date.now() + this.base);
    this.rearm();
  }

  /** End the step as timed out, once. */
  expire(limit: number): void {
    if (this.done || this.expired) return;
    this.expired = true;
    this.limit = limit;
    this.abort.abort(new Error('step timed out'));
  }

  /** The step is over: no timer fires after this. */
  stop(): void {
    this.done = true;
    clearTimeout(this.timer);
  }
}

/** Give a tool its own time limit inside a step, for a tool that needs longer
 *  than `stepTimeoutMs`. While a call to it runs, the step has at least this
 *  long; a call past it ends the step the way a step past `stepTimeoutMs`
 *  does, with the same error and the same retry. It does not lift
 *  `segmentTimeoutMs`, or the queue's own limit on a job: keep those longer
 *  than the longest tool. */
export function withToolTimeout<T extends object>(t: T, timeoutMs: number): T {
  return { ...t, timeoutMs }; // a copy, as in Go: the tool passed in keeps its own limit
}

const timed = (t: any) => typeof t?.execute === 'function' && typeof t?.timeoutMs === 'number' && t.timeoutMs > 0;

/** Does this toolset have a tool with its own `timeoutMs`? */
export const hasTimedTool = (tools: Record<string, any>) => Object.values(tools).some(timed);

/** Make each tool with its own `timeoutMs` keep to it inside the step. */
export function withStepLimit(tools: Record<string, any>, limit: StepLimit): Record<string, any> {
  if (!hasTimedTool(tools)) return tools;
  const out: Record<string, any> = {};
  for (const [name, t] of Object.entries(tools)) {
    if (!timed(t)) {
      out[name] = t;
      continue;
    }
    const ms: number = t.timeoutMs;
    out[name] = {
      ...t,
      execute: async (args: unknown, opts: { abortSignal?: AbortSignal } & Record<string, unknown>) => {
        const parent = opts?.abortSignal;
        if (parent?.aborted) throw parent.reason; // the step is already over: do not start
        const id = limit.begin(ms);
        const own = new AbortController();
        const onParent = () => own.abort(parent?.reason);
        parent?.addEventListener('abort', onParent, { once: true });
        const timer = setTimeout(() => {
          if (parent?.aborted) return;
          own.abort(new Error('tool timed out'));
          limit.expire(ms);
        }, ms);
        try {
          return await t.execute(args, { ...opts, abortSignal: own.signal });
        } finally {
          clearTimeout(timer);
          parent?.removeEventListener('abort', onParent);
          limit.finish(id);
        }
      },
    };
  }
  return out;
}
