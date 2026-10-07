/** The test suite's fake model, written in the v1 provider shape.
 *
 *  The tests were written against AI SDK v4, whose fake model streamed v1
 *  parts: `text-delta` with `textDelta`, one `tool-call` with `args`, and a
 *  `finish` with `promptTokens`/`completionTokens`. The SDK now (v7) drives a
 *  v4 provider, with start/delta/end parts and nested usage. Rather than
 *  rewrite every fixture, this wraps `MockLanguageModelV4`: the fixtures stay
 *  in the shape they were written in, and each call is translated both ways —
 *  the prompt and tools the model is handed go back to the v1 shape (tool
 *  calls with `args`, results with `result`, tools under `mode.tools`), and
 *  the parts it streams go forward to v4. */
import { MockLanguageModelV4 } from 'ai/test';

/** A part the fake model streams, in the v1 shape the fixtures use. */
export type LanguageModelV1StreamPart =
  | { type: 'text-delta'; textDelta: string }
  | { type: 'reasoning'; textDelta: string }
  | { type: 'tool-call-delta'; toolCallType?: 'function'; toolCallId: string; toolName: string; argsTextDelta: string }
  | { type: 'tool-call'; toolCallType?: 'function'; toolCallId: string; toolName: string; args: string }
  | { type: 'source'; source: Record<string, unknown> }
  | { type: 'response-metadata'; id?: string; timestamp?: Date; modelId?: string }
  | {
      type: 'finish';
      finishReason: string;
      usage: { promptTokens: number; completionTokens: number };
      providerMetadata?: Record<string, Record<string, unknown>>;
    }
  | { type: 'error'; error: unknown };

type V1Options = {
  provider?: string;
  modelId?: string;
  doStream?: (call: any) => PromiseLike<any> | any;
  doGenerate?: (call: any) => PromiseLike<any> | any;
  [key: string]: unknown;
};

const usage = (u: { promptTokens?: number; completionTokens?: number } | undefined) => ({
  inputTokens: { total: u?.promptTokens, noCache: undefined, cacheRead: undefined, cacheWrite: undefined },
  outputTokens: { total: u?.completionTokens, text: undefined, reasoning: undefined },
});

/** v1 called a made-up finish `unknown`; v4 has no such reason. */
const finishReason = (reason: string) =>
  reason === 'unknown'
    ? { unified: 'other' as const, raw: undefined }
    : { unified: reason as any, raw: reason };

function v1Output(output: any): unknown {
  if (!output || typeof output !== 'object') return output;
  if (output.type === 'execution-denied') return { denied: true };
  return 'value' in output ? output.value : output;
}

function v1Part(p: any): any {
  const base = p && typeof p === 'object' && p.providerOptions !== undefined
    ? { ...p, providerMetadata: p.providerOptions }
    : p;
  return v1Shape(base);
}

/** Safe to apply twice: a fake model may hand its call on to another. */
function v1Shape(p: any): any {
  switch (p?.type) {
    case 'tool-call': {
      if (!('input' in p)) return p;
      const { input, ...rest } = p;
      return { ...rest, args: input };
    }
    case 'tool-result': {
      if (!('output' in p)) return p;
      const { output, ...rest } = p;
      return { ...rest, result: v1Output(output) };
    }
    case 'file': {
      if (typeof p.mediaType !== 'string' || !p.mediaType.startsWith('image')) return p;
      const data = p.data?.type === 'url' ? p.data.url : p.data?.type === 'data' ? p.data.data : p.data;
      return { type: 'image', image: data, mimeType: p.mediaType };
    }
    default:
      return p;
  }
}

/** The call a v1 fake model was handed: v1 prompt parts, tools under
 *  `mode.tools`, provider options under their v1 name. */
function v1Call(options: any): any {
  const prompt = (options.prompt ?? []).map((raw: any) => {
    const m = raw.providerOptions !== undefined ? { ...raw, providerMetadata: raw.providerOptions } : raw;
    return Array.isArray(m.content) ? { ...m, content: m.content.map(v1Part) } : m;
  });
  const tools = (options.tools ?? []).map((t: any) =>
    t.type === 'function' ? { type: 'function', name: t.name, description: t.description, parameters: t.inputSchema } : t,
  );
  return {
    ...options,
    prompt,
    mode: { type: 'regular', tools },
    providerMetadata: options.providerOptions,
  };
}

/** v1 stream parts → v4 stream parts. */
function v4Stream(stream: ReadableStream<any>): ReadableStream<any> {
  let text: string | null = null;
  let reasoning: string | null = null;
  const inputs = new Set<string>();
  let n = 0;
  const close = (out: TransformStreamDefaultController<any>) => {
    if (text) out.enqueue({ type: 'text-end', id: text });
    if (reasoning) out.enqueue({ type: 'reasoning-end', id: reasoning });
    text = null;
    reasoning = null;
  };
  return stream.pipeThrough(
    new TransformStream<any, any>({
      transform(p, out) {
        // Already v4: a fake model that hands its call on to another gets the
        // other's v4 stream back.
        const v4 =
          (p?.type === 'text-delta' && !('textDelta' in p)) ||
          (p?.type === 'tool-call' && 'input' in p) ||
          (p?.type === 'finish' && typeof p.finishReason === 'object') ||
          (p?.type === 'source' && !('source' in p));
        if (v4) { out.enqueue(p); return; }
        switch (p?.type) {
          case 'text-delta':
            if (reasoning) { out.enqueue({ type: 'reasoning-end', id: reasoning }); reasoning = null; }
            if (!text) { text = `text-${n++}`; out.enqueue({ type: 'text-start', id: text }); }
            out.enqueue({ type: 'text-delta', id: text, delta: p.textDelta });
            return;
          case 'reasoning':
            if (text) { out.enqueue({ type: 'text-end', id: text }); text = null; }
            if (!reasoning) { reasoning = `reasoning-${n++}`; out.enqueue({ type: 'reasoning-start', id: reasoning }); }
            out.enqueue({ type: 'reasoning-delta', id: reasoning, delta: p.textDelta });
            return;
          case 'tool-call-delta':
            close(out);
            if (!inputs.has(p.toolCallId)) {
              inputs.add(p.toolCallId);
              out.enqueue({ type: 'tool-input-start', id: p.toolCallId, toolName: p.toolName });
            }
            out.enqueue({ type: 'tool-input-delta', id: p.toolCallId, delta: p.argsTextDelta });
            return;
          case 'tool-call':
            close(out);
            if (inputs.delete(p.toolCallId)) out.enqueue({ type: 'tool-input-end', id: p.toolCallId });
            out.enqueue({
              type: 'tool-call',
              toolCallId: p.toolCallId,
              toolName: p.toolName,
              input: typeof p.args === 'string' ? p.args : JSON.stringify(p.args ?? {}),
            });
            return;
          case 'source':
            out.enqueue({ type: 'source', ...p.source });
            return;
          case 'finish':
            close(out);
            out.enqueue({
              type: 'finish',
              finishReason: finishReason(p.finishReason),
              usage: usage(p.usage),
              ...(p.providerMetadata ? { providerMetadata: p.providerMetadata } : {}),
            });
            return;
          default:
            out.enqueue(p);
        }
      },
      flush(out) {
        close(out);
      },
    }),
  );
}

/** A v1 generate result → a v4 one. */
function v4Generate(r: any): any {
  const content: any[] = [];
  if (r.reasoning) content.push({ type: 'reasoning', text: typeof r.reasoning === 'string' ? r.reasoning : '' });
  if (r.text) content.push({ type: 'text', text: r.text });
  for (const c of r.toolCalls ?? []) {
    content.push({
      type: 'tool-call',
      toolCallId: c.toolCallId,
      toolName: c.toolName,
      input: typeof c.args === 'string' ? c.args : JSON.stringify(c.args ?? {}),
    });
  }
  return {
    content,
    finishReason: finishReason(r.finishReason ?? 'stop'),
    usage: usage(r.usage),
    warnings: [],
    ...(r.providerMetadata ? { providerMetadata: r.providerMetadata } : {}),
    ...(r.response ? { response: r.response } : {}),
  };
}

export class MockLanguageModelV1 extends MockLanguageModelV4 {
  constructor({ provider, modelId, doStream, doGenerate }: V1Options = {}) {
    super({
      // Image URLs go to the model as URLs, as a real provider takes them;
      // otherwise the SDK would try to download them first.
      supportedUrls: { 'image/*': [/^https?:\/\/.*$/] },
      ...(provider ? { provider } : {}),
      ...(modelId ? { modelId } : {}),
      ...(doStream
        ? {
            doStream: async (options: any) => {
              const r = await doStream(v1Call(options));
              return { stream: v4Stream(r.stream), ...(r.response ? { response: r.response } : {}) };
            },
          }
        : {}),
      ...(doGenerate
        ? { doGenerate: async (options: any) => v4Generate(await doGenerate(v1Call(options))) }
        : {}),
    } as any);
  }
}
