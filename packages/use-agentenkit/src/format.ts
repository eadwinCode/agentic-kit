import type { ActivityLabels, EntryFormat } from './config.js';
import type {
  AgentActivity,
  AgentState,
  ChatEntry,
  EntryPart,
  SnapshotMessage,
  UsageTotals,
} from './types.js';

const json = (value: unknown) => {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

/** Flatten AI-SDK message content into something a bubble can render. */
export function contentToText(content: unknown, format: EntryFormat): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return json(content);

  return content
    .map((part: any) => {
      if (part?.type === 'text') return part.text ?? '';
      // Reasoning is deliberately NOT folded in here — `messageToEntries`
      // lifts it into its own entry so the answer stays clean.
      if (part?.type === 'tool-call') {
        return format.toolCall(part.toolName ?? 'tool', part.args ?? {});
      }
      if (part?.type === 'tool-result') return format.toolResult(undefined, part.result);
      return '';
    })
    .filter(Boolean)
    .join('\n');
}

/** Whether a tool result reports a failure: an object with an `error` key
 *  (a tool that threw, or an unknown tool), or the `error: …` text a failed
 *  tool's stored result carries. A denial or a cancellation is an answer,
 *  not a failure. */
export function isToolError(result: unknown): boolean {
  if (typeof result === 'string') return result.startsWith('error: ');
  return !!result && typeof result === 'object' && 'error' in (result as object);
}

/** What a stored history says about each tool call: `done` when a result is
 *  durable, `error` when that result reports a failure. Keyed by call id, so
 *  one call's result can never flip another. */
export type ToolCallOutcomes = ReadonlyMap<string, 'done' | 'error'>;

/** The state a durable tool call is in, given what is known about results. */
function durableToolState(
  toolCallId: string,
  answered: ReadonlySet<string> | ToolCallOutcomes,
): 'running' | 'done' | 'error' {
  if (answered instanceof Map) return answered.get(toolCallId) ?? 'running';
  return answered.has(toolCallId) ? 'done' : 'running';
}

/** The structured parts of a stored message, thinking left out (it becomes
 *  its own entry). `answered` names the tool calls whose result is durable,
 *  which is how a call knows it is done — as a set, or as the outcomes from
 *  `toolCallOutcomes`, which also know which calls failed. */
export function contentToParts(
  content: unknown,
  answered: ReadonlySet<string> | ToolCallOutcomes = new Set(),
): EntryPart[] {
  if (typeof content === 'string') return content ? [{ type: 'text', text: content }] : [];
  if (!Array.isArray(content)) {
    const text = json(content);
    return text ? [{ type: 'text', text }] : [];
  }
  const out: EntryPart[] = [];
  for (const part of content as any[]) {
    switch (part?.type) {
      case 'text':
        if (part.text) out.push({ type: 'text', text: part.text });
        break;
      case 'image':
        if (part.image) out.push({ type: 'image', image: part.image, mimeType: part.mimeType });
        break;
      case 'tool-call':
        out.push({
          type: 'tool-call',
          toolCallId: part.toolCallId,
          toolName: part.toolName ?? 'tool',
          args: part.args ?? {},
          state: durableToolState(part.toolCallId, answered),
        });
        break;
      case 'tool-result':
        out.push({
          type: 'tool-result',
          toolCallId: part.toolCallId,
          toolName: part.toolName,
          result: part.result,
        });
        break;
    }
  }
  return out;
}

/** One durable message → one entry, or null when it carries nothing to show.
 *  A message that is only tool activity is marked `kind: 'tool'` so a UI can
 *  style it apart from conversation. */
export function messageToEntry(
  message: SnapshotMessage,
  format: EntryFormat,
  answered: ReadonlySet<string> | ToolCallOutcomes = new Set(),
): ChatEntry | null {
  const text = contentToText(message.content, format);
  const structured = contentToParts(message.content, answered);
  if (!text && structured.length === 0) return null;
  const parts = Array.isArray(message.content) ? message.content : [];
  const containsText = parts.some((part: any) => part?.type === 'text' && part.text);
  const containsToolActivity = parts.some(
    (part: any) => part?.type === 'tool-call' || part?.type === 'tool-result',
  );
  return {
    id: message.id,
    kind:
      message.role === 'tool' ||
      message.role === 'system' ||
      (containsToolActivity && !containsText)
        ? 'tool'
        : 'text',
    role: message.role,
    text,
    agentId: message.agentId ?? null,
    parts: structured,
  };
}

/** The model's thinking, as persisted on a message. Providers that do not
 *  expose it simply have no such parts, and this returns ''. */
export function reasoningText(content: unknown): string {
  if (!Array.isArray(content)) return '';
  return content
    .filter((part: any) => part?.type === 'reasoning')
    .map((part: any) => part?.text ?? part?.reasoning ?? '')
    .join('');
}

/** One durable message → the entries it renders as: its thinking first, then
 *  the answer. Split because they stream separately and a UI folds thinking
 *  away, so keeping them in one bubble would make a reload look different from
 *  the live run. */
export function messageToEntries(
  message: SnapshotMessage,
  format: EntryFormat,
  answered: ReadonlySet<string> | ToolCallOutcomes = new Set(),
): ChatEntry[] {
  const out: ChatEntry[] = [];
  const thought = reasoningText(message.content);
  if (thought) {
    out.push({
      id: `${message.id}:reasoning`,
      kind: 'reasoning',
      role: message.role,
      text: thought,
      agentId: message.agentId ?? null,
      parts: [{ type: 'reasoning', text: thought }],
    });
  }
  const entry = messageToEntry(message, format, answered);
  if (entry) out.push(entry);
  return out;
}

/** Every tool call a stored history has a result for, and how it went. A
 *  denied approval, a stop, or an approval that ran the tool later never
 *  streams a result, so this is the only way a reload learns their state. */
export function toolCallOutcomes(messages: readonly SnapshotMessage[]): Map<string, 'done' | 'error'> {
  const outcomes = new Map<string, 'done' | 'error'>();
  for (const m of messages) {
    if (!Array.isArray(m.content)) continue;
    for (const part of m.content as any[]) {
      if (part?.type === 'tool-result' && typeof part.toolCallId === 'string') {
        outcomes.set(part.toolCallId, isToolError(part.result) ? 'error' : 'done');
      }
    }
  }
  return outcomes;
}

/** Every tool call a stored history has a result for. */
export function answeredToolCalls(messages: readonly SnapshotMessage[]): Set<string> {
  return new Set(toolCallOutcomes(messages).keys());
}

/** The activity a thread state implies when nothing more specific is happening. */
export function stateActivity(state: AgentState, labels: ActivityLabels): AgentActivity {
  switch (state) {
    case 'RUNNING':
      return { phase: 'thinking', label: labels.thinking };
    case 'WAITING_FOR_INPUT':
      return { phase: 'waiting-input', label: labels.waitingApproval };
    case 'COMPLETED':
      return { phase: 'completed', label: labels.completed };
    case 'CANCELLED':
      return { phase: 'stopped', label: labels.stopped };
    case 'FAILED':
      return { phase: 'failed', label: labels.failed };
    default:
      return { phase: 'idle', label: labels.idle };
  }
}

/** Render a cost for a thread header: `formatCost(usage.tokens)` gives
 *  "$0.0125", or "≥ $0.0125" when some calls went unpriced and the figure is
 *  a floor rather than the whole bill. Returns null when there is nothing to
 *  show, so a header can leave the slot empty instead of printing "$0.00" for
 *  a server with no pricer configured.
 *
 *  Display only — never do arithmetic on the string. */
export function formatCost(
  usage: Pick<UsageTotals, 'costMicros' | 'currency' | 'unpriced'> | null | undefined,
  locale?: string,
): string | null {
  if (!usage) return null;
  const micros = usage.costMicros ?? 0;
  if (micros === 0 && !usage.unpriced) return null;
  if (micros === 0) return null; // priced nothing at all: no figure to show
  const currency = usage.currency || 'USD';
  let text: string;
  try {
    text = new Intl.NumberFormat(locale, {
      style: 'currency',
      currency,
      // Agent runs are cheap: two decimals would round most of them to $0.00.
      minimumFractionDigits: 2,
      maximumFractionDigits: 4,
    }).format(micros / 1_000_000);
  } catch {
    // An unknown currency code: show the number and the code plainly.
    text = `${(micros / 1_000_000).toFixed(4)} ${currency}`;
  }
  return usage.unpriced ? `≥ ${text}` : text;
}
