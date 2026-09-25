import type { RuntimePorts } from '../ports/runtime.js';
import type { RunStreams } from '../ports/streams.js';
import { THREAD_KEY_TTL_SECONDS } from './keys.js';
import { publish } from './publish.js';
import { streamIdOf, type StreamEnd, type StreamEvent } from './stream-events.js';

/** The worker's side of a run stream: one per segment, open from pickup to
 *  the segment's end. Everything published on the thread while it is open
 *  is turned into typed stream events here (see forward), so the engine
 *  keeps calling `publish` and only opens and closes the segment.
 *
 *  Events are appended together: they wait up to `streamFlushMs`, or until
 *  `streamFlushEvents` are waiting. A step end, a tool result, a park and a
 *  close go out at once. A failed append is logged, never thrown: the stream
 *  is delivery, and the run's messages are saved either way. */

/** The segment counter: each pickup of a run takes the next number, so a
 *  retry or a resume gets a stream of its own. */
export const segmentKey = (runId: string) => `agent:segment:${runId}`;

/** Open segments by thread, per RunStreams, so two runtimes in one process
 *  never see each other's. */
const open = new WeakMap<RunStreams, Map<string, SegmentStream>>();

/** The segment this process has open on a thread, if any. */
export function activeSegment(deps: RuntimePorts, threadId: string): SegmentStream | undefined {
  return deps.streams ? open.get(deps.streams)?.get(threadId) : undefined;
}

/** Events that go out at once rather than wait for the window. */
const IMMEDIATE = new Set([
  'RUN_STARTED', 'TOOL_CALL_RESULT', 'STEP_FINISHED', 'INPUT_REQUIRED', 'SUBAGENT_FINISHED',
]);

const immediate = (e: StreamEvent): boolean =>
  IMMEDIATE.has(e.type) || (e.type === 'SUBAGENT_EVENT' && immediate(e.event));

/** The text, reasoning and tool calls one agent has open in the stream. */
interface Blocks {
  text?: string;
  reasoning?: string;
  tools: Set<string>;
}

/** Opens the segment's stream and says RUN_STARTED on it. Null when the
 *  runtime has no streams, or the open failed (logged): the run goes on
 *  without a stream. */
export async function openSegment(
  deps: RuntimePorts,
  threadId: string,
  runId: string | undefined,
): Promise<SegmentStream | null> {
  if (!deps.streams || !runId) return null;
  try {
    const segment = await deps.kv.incrWithExpiry(segmentKey(runId), THREAD_KEY_TTL_SECONDS);
    const streamId = streamIdOf(runId, segment);
    await deps.streams.open(streamId, { threadId, runId }, deps.config.streamTtlMs);
    const seg = new SegmentStream(deps, deps.streams, threadId, runId, segment, streamId);
    let byThread = open.get(deps.streams);
    if (!byThread) {
      byThread = new Map();
      open.set(deps.streams, byThread);
    }
    byThread.set(threadId, seg);
    await seg.push([{ type: 'RUN_STARTED', threadId, runId, streamId, segment }]);
    // The thread record says which stream is open, so a tab that opens the
    // thread finds it.
    await publish(deps, threadId, 'RUN_STARTED', { runId, streamId, segment }, runId);
    return seg;
  } catch (err) {
    (deps.log ?? console).error('run stream not opened', { threadId, runId, err });
    return null;
  }
}

/** Closes a segment's stream that its worker could not: the sweep's end for
 *  a run whose worker died. A stream that is already closed or gone is left
 *  as it is. */
export async function closeLostSegment(
  deps: RuntimePorts,
  threadId: string,
  runId: string,
  error: string,
): Promise<void> {
  if (!deps.streams) return;
  const segment = Number(await deps.kv.get(segmentKey(runId)));
  if (!segment) return;
  const streamId = streamIdOf(runId, segment);
  const snap = await deps.streams.snapshot(streamId).catch(() => null);
  if (!snap || snap.end) return; // gone, or its worker closed it after all
  await deps.streams
    .close(streamId, { type: 'RUN_ERROR', status: 'lost', error }, deps.config.streamGraceMs)
    .catch(() => undefined);
  await publish(deps, threadId, 'RUN_ENDED', { runId, streamId, segment, status: 'lost', error }, runId);
}

export class SegmentStream {
  private buf: StreamEvent[] = [];
  private timer?: ReturnType<typeof setTimeout>;
  private chain: Promise<void> = Promise.resolve();
  private ended = false;
  private readonly agents = new Map<string, Blocks>();
  private ids = 0;
  /** A one-shot agent's final text, carried on RUN_FINISHED. */
  oneShotText?: string;

  constructor(
    private readonly deps: RuntimePorts,
    private readonly streams: RunStreams,
    readonly threadId: string,
    readonly runId: string,
    readonly segment: number,
    readonly streamId: string,
  ) {}

  get closed(): boolean {
    return this.ended;
  }

  /** Queue events; resolves once they are appended when any must go at once. */
  push(events: StreamEvent[]): Promise<void> {
    if (this.ended || events.length === 0) return Promise.resolve();
    this.buf.push(...events);
    const { streamFlushMs, streamFlushEvents } = this.deps.config;
    if (streamFlushMs === 0 || this.buf.length >= streamFlushEvents || events.some(immediate)) {
      return this.flush();
    }
    if (!this.timer) {
      this.timer = setTimeout(() => void this.flush(), streamFlushMs);
      (this.timer as unknown as { unref?: () => void }).unref?.();
    }
    return Promise.resolve();
  }

  /** Append what waits; resolves once everything pushed so far is out. */
  flush(): Promise<void> {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    const batch = this.buf.splice(0);
    if (batch.length > 0) {
      this.chain = this.chain
        .then(() => this.streams.append(this.streamId, batch))
        .then(() => undefined)
        .catch((err) => {
          (this.deps.log ?? console).error('run stream events not appended', { streamId: this.streamId, err });
        });
    }
    return this.chain;
  }

  /** End the stream: what is open is ended, what waits is appended, and the
   *  end item goes last. A second close is a no-op. */
  async close(end: StreamEnd): Promise<void> {
    if (this.ended) return;
    const closing: StreamEvent[] = [];
    for (const key of this.agents.keys()) closing.push(...this.endBlocks(key));
    await this.push(closing);
    await this.flush();
    this.ended = true;
    const byThread = open.get(this.streams);
    if (byThread?.get(this.threadId) === this) byThread.delete(this.threadId);
    await this.streams.close(this.streamId, end, this.deps.config.streamGraceMs).catch((err) => {
      (this.deps.log ?? console).error('run stream not closed', { streamId: this.streamId, err });
    });
    // The thread record says how the segment ended.
    await publish(this.deps, this.threadId, 'RUN_ENDED', {
      runId: this.runId, streamId: this.streamId, segment: this.segment, status: end.status,
      ...(end.type === 'RUN_ERROR' ? { error: end.error } : {}),
    }, this.runId).catch((err) => {
      (this.deps.log ?? console).error('run end not recorded', { streamId: this.streamId, err });
    });
  }

  /** Turn a published event into stream events. `reserved` is whether the
   *  type is one of the platform's; any other is an app's own, and goes out
   *  as CUSTOM. */
  forward(type: string, payload: unknown, reserved: boolean): Promise<void> {
    const p = (payload ?? {}) as Record<string, any>;
    switch (type) {
      case 'CHUNK':
        return this.push(this.mapChunk('', p));
      case 'SUBAGENT_CHUNK': {
        const agentId = String(p.agentId);
        return this.push(this.wrap(agentId, this.mapChunk(agentId, p.chunk ?? {})));
      }
      // The step is saved: the stream's STEP_FINISHED says so, right here,
      // ahead of any park the step raised.
      case 'STEP_COMMITTED': {
        const agentId: string | null = p.agentId ?? null;
        const ending = agentId === null ? this.endBlocks('') : this.wrap(agentId, this.endBlocks(agentId));
        return this.push([
          ...ending,
          {
            type: 'STEP_FINISHED',
            step: Number(p.step ?? 0),
            agentId,
            finishReason: String(p.finishReason ?? ''),
            usage: {
              inputTokens: Number(p.inputTokens ?? 0),
              cachedInputTokens: Number(p.cachedInputTokens ?? 0),
              outputTokens: Number(p.outputTokens ?? 0),
              totalTokens: Number(p.totalTokens ?? 0),
            },
          },
        ]);
      }
      case 'INPUT_REQUIRED':
        // Through JSON, so a Date goes out as the string every store holds.
        return this.push([{ ...JSON.parse(JSON.stringify(p)), type: 'INPUT_REQUIRED' }]);
      case 'SUBAGENT_STARTED':
        return this.push([
          { type: 'SUBAGENT_STARTED', subagentId: String(p.agentId), name: String(p.name ?? ''), depth: Number(p.depth ?? 0) },
        ]);
      case 'SUBAGENT_COMPLETED':
      case 'SUBAGENT_FAILED': {
        const agentId = String(p.agentId);
        const failed = type === 'SUBAGENT_FAILED';
        const status = !failed ? 'completed' : p.state === 'CANCELLED' ? 'cancelled' : 'failed';
        return this.push([
          ...this.wrap(agentId, this.endBlocks(agentId)),
          {
            type: 'SUBAGENT_FINISHED',
            subagentId: agentId,
            status,
            ...(failed && p.error !== undefined ? { error: String(p.error) } : {}),
          },
        ]);
      }
      case 'TEXT_RESULT':
        this.oneShotText = typeof p.text === 'string' ? p.text : undefined;
        return Promise.resolve();
      default:
        if (reserved) return Promise.resolve(); // the thread record's, or a notice
        return this.push([{ type: 'CUSTOM', name: type, value: payload ?? null }]);
    }
  }

  private blocks(key: string): Blocks {
    let b = this.agents.get(key);
    if (!b) {
      b = { tools: new Set() };
      this.agents.set(key, b);
    }
    return b;
  }

  private newId(): string {
    return `${this.streamId}:m${++this.ids}`;
  }

  private wrap(agentId: string, events: StreamEvent[]): StreamEvent[] {
    return events.map((event) => ({ type: 'SUBAGENT_EVENT', subagentId: agentId, event }));
  }

  /** End an agent's open text and reasoning. */
  private endBlocks(key: string): StreamEvent[] {
    const b = this.agents.get(key);
    if (!b) return [];
    const out: StreamEvent[] = [];
    if (b.reasoning) out.push({ type: 'REASONING_END', messageId: b.reasoning });
    if (b.text) out.push({ type: 'TEXT_MESSAGE_END', messageId: b.text });
    b.reasoning = undefined;
    b.text = undefined;
    return out;
  }

  /** One SDK stream part as stream events, for one agent ('' the main one). */
  private mapChunk(key: string, c: Record<string, any>): StreamEvent[] {
    const b = this.blocks(key);
    const out: StreamEvent[] = [];
    const startTool = (toolCallId: string, toolName: string) => {
      if (b.tools.has(toolCallId)) return;
      b.tools.add(toolCallId);
      out.push({ type: 'TOOL_CALL_START', toolCallId, toolName });
    };
    switch (c.type) {
      case 'text-delta': {
        if (b.reasoning) {
          out.push({ type: 'REASONING_END', messageId: b.reasoning });
          b.reasoning = undefined;
        }
        if (!b.text) {
          b.text = this.newId();
          out.push({ type: 'TEXT_MESSAGE_START', messageId: b.text, role: 'assistant' });
        }
        out.push({ type: 'TEXT_MESSAGE_CONTENT', messageId: b.text, delta: String(c.textDelta ?? '') });
        break;
      }
      case 'reasoning': {
        if (b.text) {
          out.push({ type: 'TEXT_MESSAGE_END', messageId: b.text });
          b.text = undefined;
        }
        if (!b.reasoning) {
          b.reasoning = this.newId();
          out.push({ type: 'REASONING_START', messageId: b.reasoning });
        }
        out.push({ type: 'REASONING_CONTENT', messageId: b.reasoning, delta: String(c.textDelta ?? '') });
        break;
      }
      case 'tool-call-streaming-start':
        out.push(...this.endBlocks(key));
        startTool(String(c.toolCallId), String(c.toolName));
        break;
      case 'tool-call-delta':
        startTool(String(c.toolCallId), String(c.toolName));
        out.push({ type: 'TOOL_CALL_ARGS', toolCallId: String(c.toolCallId), delta: String(c.argsTextDelta ?? '') });
        break;
      case 'tool-call': {
        out.push(...this.endBlocks(key));
        const toolCallId = String(c.toolCallId);
        startTool(toolCallId, String(c.toolName));
        b.tools.delete(toolCallId);
        let args = c.args;
        if (typeof args === 'string') {
          try { args = JSON.parse(args); } catch { /* left as the string it came as */ }
        }
        out.push({ type: 'TOOL_CALL_END', toolCallId, toolName: String(c.toolName), args: args ?? null });
        break;
      }
      case 'tool-result':
        out.push({
          type: 'TOOL_CALL_RESULT', toolCallId: String(c.toolCallId), toolName: String(c.toolName), result: c.result ?? null,
        });
        break;
      case 'source':
        out.push({ type: 'SOURCE', source: c.source ?? null });
        break;
    }
    return out;
  }
}
