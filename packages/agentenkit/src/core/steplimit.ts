/** The wall-time limit of one step (§2.8). It starts at `stepTimeoutMs`. A
 *  tool with its own `timeoutMs` moves the end out while it runs, so one slow
 *  tool gets the time it needs without raising `stepTimeoutMs` for every step
 *  of every run. */
export class StepLimit {
  private timer: ReturnType<typeof setTimeout> | undefined;
  /** 0: no limit for the step, though a tool's own `timeoutMs` still holds. */
  private end = 0;
  private done = false;
  /** What set the end, and what the error says the step ran past. */
  limit: number;
  expired = false;

  constructor(private readonly abort: AbortController, readonly base: number) {
    this.limit = base;
    if (base > 0) this.arm(Date.now() + base, base);
  }

  private arm(end: number, limit: number): void {
    clearTimeout(this.timer);
    this.end = end;
    this.limit = limit;
    this.timer = setTimeout(() => this.expire(limit), end - Date.now());
  }

  /** Give the step at least `ms` from now. It never brings the end in, and a
   *  step with no limit keeps none: the tool's own timer still holds. */
  extend(ms: number): void {
    if (this.done || this.expired || this.end === 0) return;
    const end = Date.now() + ms;
    if (end > this.end) this.arm(end, ms);
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

/** Make each tool with its own `timeoutMs` keep to it inside the step. Once
 *  the call returns, the step has at least `stepTimeoutMs` again to finish. */
export function withStepLimit(tools: Record<string, any>, limit: StepLimit): Record<string, any> {
  if (!Object.values(tools).some(timed)) return tools;
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
        limit.extend(ms);
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
          limit.extend(limit.base);
        }
      },
    };
  }
  return out;
}
