/** Prompt-side repair of the message history (§2.5).
 *
 *  History can carry an assistant tool call with no tool result: a run that
 *  was stopped while parked for approval persisted the call and never its
 *  result, or a worker died between the two. Strict providers (OpenAI,
 *  Anthropic) reject such a prompt outright, which would wedge the thread on
 *  every later run. The repair inserts a synthetic result saying so. It is
 *  prompt-side only; nothing is written back. */

interface PartLike {
  type?: string;
  toolCallId?: string;
  toolName?: string;
  [key: string]: unknown;
}

interface MessageLike {
  role: string;
  content: unknown;
  [key: string]: unknown;
}

const parts = (content: unknown): PartLike[] => (Array.isArray(content) ? (content as PartLike[]) : []);

export const DANGLING_CALL_RESULT = {
  cancelled: true,
  reason: 'no result was recorded for this call',
};

/** Close every assistant tool call that has no tool result before the next
 *  turn. Idempotent. */
export function repairDanglingToolCalls<T extends MessageLike>(messages: T[]): T[] {
  const out: T[] = [];
  for (let i = 0; i < messages.length; i += 1) {
    const m = messages[i]!;
    out.push(m);
    if (m.role !== 'assistant') continue;
    const calls = parts(m.content).filter((p) => p.type === 'tool-call' && p.toolCallId);
    if (calls.length === 0) continue;

    const answered = new Set<string>();
    let j = i + 1;
    for (; j < messages.length && messages[j]!.role === 'tool'; j += 1) {
      for (const p of parts(messages[j]!.content)) {
        if (p.type === 'tool-result' && p.toolCallId) answered.add(p.toolCallId);
      }
    }
    const missing = calls
      .filter((c) => !answered.has(c.toolCallId!))
      .map((c) => ({
        type: 'tool-result',
        toolCallId: c.toolCallId,
        toolName: c.toolName,
        result: DANGLING_CALL_RESULT,
      }));
    if (missing.length === 0) continue;

    // Keep the results together, right after the call, before any later turn.
    out.push(...messages.slice(i + 1, j));
    out.push({ role: 'tool', content: missing } as T);
    i = j - 1;
  }
  return out;
}
