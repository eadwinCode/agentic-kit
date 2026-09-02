import type { StreamEvent } from 'use-agentenkit';

/** UI state derived from the thread's custom events. The platform's own
 *  events drive the hook; these are the app's, published by its tools and by
 *  the credit-limit endpoint, and reduced here. Durable ones come back on a
 *  reload through the snapshot replay; notices only arrive live. */
export interface CustomState {
  progress: string | null;
  preview: { url: string; brief: string } | null;
  emailSent: { to: string; subject: string } | null;
  answers: Record<string, string> | null;
  creditLimit: { kind: string; resetAt: string } | null;
  /** What the chat shows about the LAST attempt: a refused run, a spent
   *  budget. Cleared by the next user turn or a restored credit. */
  notice: { kind: 'credit' | 'budget'; text: string } | null;
  error: string | null;
  /** Every custom event seen, newest first: the demo's inspector. */
  log: Array<{ seq: number; type: string; payload: unknown; at: string }>;
}

export const initialCustomState: CustomState = {
  progress: null, preview: null, emailSent: null, answers: null, creditLimit: null, notice: null, error: null, log: [],
};

const CUSTOM = new Set(['PROGRESS', 'DESIGN_PREVIEW', 'EMAIL_SENT', 'QUESTIONS_ANSWERED', 'CREDIT_LIMIT', 'CREDIT_RESTORED']);
/** Platform events the app also logs, without claiming them from the hook. */
const LOGGED = new Set([...CUSTOM, 'RUN_REFUSED', 'TOKEN_BUDGET_EXHAUSTED']);

export type Action = { type: 'event'; event: StreamEvent } | { type: 'reset' } | { type: 'clear' };

/** A new user turn clears the last turn's transient state, the way the
 *  snippet in the docs does; durable facts (the preview) stay until replaced. */
export function reduceCustom(state: CustomState, action: Action): CustomState {
  if (action.type === 'clear') return initialCustomState;
  if (action.type === 'reset') {
    return { ...state, progress: null, emailSent: null, answers: null, error: null, notice: null };
  }
  const e = action.event;
  const p = (e.payload ?? {}) as Record<string, any>;
  const logged = LOGGED.has(e.type)
    ? [{ seq: e.seq, type: e.type, payload: e.payload, at: new Date().toLocaleTimeString() }, ...state.log].slice(0, 30)
    : state.log;

  switch (e.type) {
    case 'MESSAGE_APPENDED':
      return p.role === 'user' ? reduceCustom(state, { type: 'reset' }) : state;
    case 'STATE_CHANGE':
      if (p.state === 'FAILED') return { ...state, progress: null, error: p.error ?? 'Run failed' };
      if (p.state === 'COMPLETED' || p.state === 'CANCELLED') return { ...state, progress: null };
      return state;
    case 'PROGRESS':
      return { ...state, progress: p.label ?? null, log: logged };
    case 'DESIGN_PREVIEW':
      return { ...state, progress: null, preview: { url: p.url, brief: p.brief ?? '' }, log: logged };
    case 'EMAIL_SENT':
      return { ...state, emailSent: { to: p.to, subject: p.subject }, log: logged };
    case 'QUESTIONS_ANSWERED':
      return { ...state, answers: p.answers ?? null, log: logged };
    case 'CREDIT_LIMIT': {
      // Published by the billing pre-check as it refuses the run: the chat
      // shows why the message went nowhere.
      const resets = formatDay(p.resetAt);
      return {
        ...state, progress: null,
        creditLimit: { kind: p.kind, resetAt: p.resetAt },
        notice: { kind: 'credit', text: `credit limit reached. resets ${resets} - clear it to continue` },
        log: logged,
      };
    }
    case 'RUN_REFUSED':
      // The platform's own record of a refusal. The app's CREDIT_LIMIT
      // already said it in its own words; keep that when both arrive.
      return state.notice ? { ...state, log: logged } : { ...state, notice: { kind: 'credit', text: String(p.error ?? 'run refused') }, log: logged };
    case 'TOKEN_BUDGET_EXHAUSTED':
      return {
        ...state, progress: null,
        notice: { kind: 'budget', text: `token budget reached (${p.tokensUsed} of ${p.tokenBudget} tokens). send another message to continue` },
        log: logged,
      };
    case 'CREDIT_RESTORED':
      return { ...state, creditLimit: null, notice: null, error: null, log: logged };
    default:
      return state;
  }
}

/** Which event types the reducer fully owns: the hook must not see them. */
export const isCustomEvent = (type: string) => CUSTOM.has(type);

function formatDay(value: unknown): string {
  const date = new Date(String(value));
  return Number.isNaN(date.getTime()) ? 'soon' : new Intl.DateTimeFormat(undefined, { day: 'numeric', month: 'short' }).format(date);
}
