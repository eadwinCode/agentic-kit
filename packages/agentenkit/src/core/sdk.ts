/** The one place the AI SDK's shapes meet the platform's own.
 *
 *  The platform stores messages and publishes chunks in one format, shared
 *  with the Go runtime and pinned by the parity fixtures: tool calls carry
 *  `args`, tool results carry `result`, text deltas carry `textDelta`, images
 *  carry `mimeType`. That is the AI SDK v4 shape, and it stays — threads
 *  already stored keep loading, and a client written against one runtime keeps
 *  working against the other.
 *
 *  The SDK itself (v7) speaks another shape: `input`/`output`, typed tool
 *  outputs, start/delta/end stream parts, nested usage. Everything that goes
 *  to the SDK is turned into its shape here, and everything that comes back is
 *  turned into the platform's, so nothing else in the package has to know
 *  which SDK version is installed. */

import { InvalidToolInputError, NoSuchToolError } from 'ai';

type Part = Record<string, any>;
type Message = { role: string; content: unknown; [key: string]: unknown };

const isObject = (v: unknown): v is Record<string, any> =>
  typeof v === 'object' && v !== null && !Array.isArray(v);

const errorMessage = (err: unknown): string =>
  err instanceof Error ? err.message : typeof err === 'string' ? err : JSON.stringify(err) ?? String(err);

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/** A stored tool result as the SDK's typed tool output: a string is text,
 *  anything else is JSON. The SDK picks the same way for a fresh result, so a
 *  replayed thread reads to the model exactly as it did live. */
function toolOutput(result: unknown): Part {
  if (typeof result === 'string') return { type: 'text', value: result };
  return { type: 'json', value: result === undefined ? null : result };
}

/** The SDK's typed tool output back as the plain value the platform stores. */
function storedResult(output: unknown): unknown {
  if (!isObject(output) || typeof output.type !== 'string') return output;
  switch (output.type) {
    case 'text':
    case 'json':
    case 'error-text':
    case 'error-json':
    case 'content':
      return output.value;
    case 'execution-denied':
      return { denied: true, ...(output.reason ? { reason: output.reason } : {}) };
    default:
      return output;
  }
}

/** The v4 name for provider options, still on parts stored before v5. */
function withProviderOptions<T extends Record<string, any>>(p: T): T {
  if (!('experimental_providerMetadata' in p)) return p;
  const { experimental_providerMetadata: legacy, ...rest } = p;
  return (rest.providerOptions === undefined ? { ...rest, providerOptions: legacy } : rest) as unknown as T;
}

function toModelPart(raw: unknown): unknown {
  if (!isObject(raw)) return raw;
  const p = withProviderOptions(raw);
  switch (p.type) {
    case 'tool-call': {
      const { args, ...rest } = p;
      let input = 'input' in p ? p.input : args;
      // A stored call can hold its arguments as the JSON text the model sent.
      if (typeof input === 'string') {
        try { input = JSON.parse(input); } catch { /* kept as the string it came as */ }
      }
      return { ...rest, input: input ?? {} };
    }
    case 'tool-result': {
      if ('output' in p && isObject(p.output) && typeof p.output.type === 'string') return p;
      const { result, isError: _isError, experimental_content: _content, ...rest } = p;
      return { ...rest, output: toolOutput(result) };
    }
    case 'image':
    case 'file': {
      if (!('mimeType' in p)) return p;
      const { mimeType, ...rest } = p;
      return { ...rest, mediaType: mimeType };
    }
    case 'reasoning': {
      // The signature moved into provider options, where the provider that
      // made it reads it back.
      if (!('signature' in p)) return p;
      const { signature, ...rest } = p;
      if (typeof signature !== 'string') return rest;
      return {
        ...rest,
        providerOptions: {
          ...(rest.providerOptions ?? {}),
          anthropic: { ...(rest.providerOptions?.anthropic ?? {}), signature },
        },
      };
    }
    case 'redacted-reasoning': {
      const { data, type: _t, ...rest } = p;
      return {
        ...rest,
        type: 'reasoning',
        text: '',
        providerOptions: {
          ...(rest.providerOptions ?? {}),
          anthropic: { ...(rest.providerOptions?.anthropic ?? {}), redactedData: data },
        },
      };
    }
    default:
      return p;
  }
}

/** One stored message as the SDK accepts it in a prompt. */
export function toModelMessage(m: Message): Message {
  const base = withProviderOptions(m);
  if (!Array.isArray(m.content)) return base;
  return { ...base, content: m.content.map(toModelPart) };
}

/** A whole prompt, stored shape → SDK shape. */
export const toModelMessages = (messages: Message[]): Message[] => messages.map(toModelMessage);

function toStoredPart(p: unknown): unknown {
  if (!isObject(p)) return p;
  switch (p.type) {
    case 'tool-call': {
      const { input, ...rest } = p;
      return { ...rest, args: input };
    }
    case 'tool-result': {
      const { output, ...rest } = p;
      return { ...rest, result: storedResult(output) };
    }
    default:
      return p;
  }
}

/** One message the SDK produced, in the shape the platform stores. */
export function toStoredMessage(m: Message): Message {
  if (!Array.isArray(m.content)) return m;
  return { ...m, content: m.content.map(toStoredPart) };
}

/** The messages a step produced, SDK shape → stored shape. */
export const toStoredMessages = (messages: Message[]): Message[] => messages.map(toStoredMessage);

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

/** The usage shape a CHUNK payload carries — the same field names the Go
 *  runtime publishes (usageJSON in core/stream.go), so a client reads both. */
export function chunkUsage(usage: unknown): Record<string, number> {
  const u = (usage ?? {}) as Record<string, any>;
  const n = (v: unknown) => (typeof v === 'number' && Number.isFinite(v) ? v : 0);
  return {
    promptTokens: n(u.inputTokens),
    completionTokens: n(u.outputTokens),
    totalTokens: n(u.totalTokens),
    cachedInputTokens: n(u.inputTokenDetails?.cacheReadTokens ?? u.cachedInputTokens),
  };
}

// ---------------------------------------------------------------------------
// Stream parts
// ---------------------------------------------------------------------------

/** Turns the SDK's stream parts into the chunks the platform publishes, in
 *  the shape core/stream.go's ChunkPayload sends. One per model call: a tool
 *  input delta names only its call id, so the tool name is remembered from
 *  the part that started it. Returns null for the parts that have no
 *  platform chunk (the start/end markers around a block). */
export class ChunkMapper {
  private toolNames = new Map<string, string>();

  map(part: unknown): unknown | null {
    if (!isObject(part)) return null;
    switch (part.type) {
      case 'text-delta':
        return { type: 'text-delta', textDelta: String(part.text ?? part.delta ?? '') };
      case 'reasoning-delta':
        return { type: 'reasoning', textDelta: String(part.text ?? part.delta ?? '') };
      case 'tool-input-start':
        this.toolNames.set(String(part.id), String(part.toolName));
        return { type: 'tool-call-streaming-start', toolCallId: part.id, toolName: part.toolName };
      case 'tool-input-delta':
        return {
          type: 'tool-call-delta',
          toolCallId: part.id,
          toolName: this.toolNames.get(String(part.id)) ?? '',
          argsTextDelta: String(part.delta ?? ''),
        };
      case 'tool-call':
        return { type: 'tool-call', toolCallId: part.toolCallId, toolName: part.toolName, args: part.input };
      case 'tool-result':
        // A preliminary result is a progress update from a streaming tool;
        // only the final one is the call's result.
        if (part.preliminary) return null;
        return { type: 'tool-result', toolCallId: part.toolCallId, toolName: part.toolName, result: part.output };
      case 'tool-error':
        // One that ends the step is not published: the step never happened.
        if (propagates(part)) return null;
        // Worded as a failed call's result is everywhere else: `{ error }`.
        return {
          type: 'tool-result',
          toolCallId: part.toolCallId,
          toolName: part.toolName,
          result: { error: errorMessage(part.error) },
        };
      case 'source': {
        const { type: _t, ...source } = part;
        return { type: 'source', source };
      }
      case 'finish-step':
        return { type: 'step-finish', finishReason: part.finishReason, usage: chunkUsage(part.usage) };
      case 'finish':
        return { type: 'finish', finishReason: part.finishReason, usage: chunkUsage(part.totalUsage ?? part.usage) };
      case 'error':
        return { type: 'error', error: errorMessage(part.error) };
      case 'start':
      case 'start-step':
      case 'text-start':
      case 'text-end':
      case 'reasoning-start':
      case 'reasoning-end':
      case 'tool-input-end':
      case 'abort':
      case 'raw':
        return null;
      default:
        return part;
    }
  }
}

// ---------------------------------------------------------------------------
// Tool errors
// ---------------------------------------------------------------------------

/** True for an error a tool's own code threw on purpose, which must end the
 *  step rather than reach the model.
 *
 *  The SDK no longer fails a step when a tool throws: it hands the model the
 *  error as the call's result and carries on. The platform wraps every tool
 *  (see withHitl) so that an ordinary failure is already returned as a result;
 *  what still throws is meant to propagate — a user stop, above all, which in
 *  both runtimes ends the run without the cancelled step ever reaching the
 *  model. A call the model got wrong (an unknown tool, bad input) is different:
 *  that is news for the model, as it is in Go, and so is the failure of a tool
 *  the provider ran itself. */
export function propagates(part: { error?: unknown; providerExecuted?: boolean }): boolean {
  // A tool the provider ran itself never passed through the platform's
  // wrapper: its failure is the provider's report, for the model to read.
  if (part.providerExecuted) return false;
  return !NoSuchToolError.isInstance(part.error) && !InvalidToolInputError.isInstance(part.error);
}

/** The first error in a step's content that must end the step. */
export function thrownByTool(content: ReadonlyArray<Record<string, any>> | undefined): unknown {
  const hit = (content ?? []).find((p) => p.type === 'tool-error' && propagates(p));
  return hit ? hit.error ?? new Error('tool failed') : undefined;
}

// ---------------------------------------------------------------------------
// Step results
// ---------------------------------------------------------------------------

/** A step's executed tool calls in the shape the platform reads them. */
export function storedToolResults(
  results: ReadonlyArray<Record<string, any>> | undefined,
): Array<{ toolCallId: string; toolName: string; args: unknown; result: unknown }> {
  return (results ?? []).map((r) => ({
    toolCallId: r.toolCallId,
    toolName: r.toolName,
    args: r.input,
    result: r.output,
  }));
}

/** True when the call ended on a finish the provider gave a reason for. The
 *  SDK reports `other` with no raw reason both for a provider finish that
 *  named no reason and for a stream that closed with no finish at all (then
 *  with no counters either). Neither is a finished step: the provider cut it
 *  short without saying so. */
export function finishedByProvider(finishReason: string, rawFinishReason: string | undefined): boolean {
  return finishReason !== 'other' || rawFinishReason !== undefined;
}
