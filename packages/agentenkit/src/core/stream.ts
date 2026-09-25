/** Drain a `streamText` result, turning a provider failure into a throw.
 *
 *  streamText reports a failure — an aborted call included — as an `error`
 *  part and then ends the stream NORMALLY, while its `text`/`usage`/`response`
 *  promises never settle. Awaiting those without rethrowing hangs the caller
 *  forever, still holding whatever it owns: for a run segment that is the
 *  thread's run lock, which wedges every later message on the thread.
 *
 *  Draining fully first also means `onChunk` fires for every part that did
 *  arrive before the failure. */
export async function drainOrThrow(fullStream: AsyncIterable<unknown>): Promise<void> {
  let streamError: unknown;
  for await (const part of fullStream) {
    if ((part as { type?: string } | null)?.type === 'error' && streamError === undefined) {
      streamError = (part as { error?: unknown }).error;
    }
  }
  if (streamError !== undefined) throw streamError;
}

/** Merges token deltas before they go out as events (§2.2). Every published
 *  event costs a seq, a stored row and a bus send; one per token is thousands
 *  a reply. Consecutive text (or reasoning) deltas are joined into one delta
 *  of the same shape, so a client reads them exactly as before. A held delta
 *  goes out after `windowMs`, or at once when any other kind of chunk comes,
 *  or on `flush`: the loop flushes before a step is committed, so its text is
 *  never published after the step that produced it. Sends go out in order. */
export class ChunkBatcher {
  private held: { type: string; textDelta: string } | null = null;
  private timer?: ReturnType<typeof setTimeout>;
  private chain: Promise<void> = Promise.resolve();

  constructor(
    private readonly send: (chunk: unknown) => Promise<void>,
    private readonly windowMs = 50,
  ) {}

  /** Take one chunk. A delta is held; anything else is sent, after what is
   *  held, and the promise resolves once it is out, so the step does not move
   *  on (a tool starting, say) before what it streamed is stored. */
  push(chunk: unknown): Promise<void> {
    const c = chunk as { type?: unknown; textDelta?: unknown } | null;
    const isDelta =
      (c?.type === 'text-delta' || c?.type === 'reasoning') && typeof c.textDelta === 'string';
    if (isDelta && this.held && this.held.type === c!.type) {
      this.held.textDelta += c!.textDelta as string;
      return Promise.resolve();
    }
    void this.flush();
    if (isDelta) {
      this.held = { ...(c as object), type: c!.type as string, textDelta: c!.textDelta as string };
      this.timer = setTimeout(() => void this.flush(), this.windowMs);
      (this.timer as unknown as { unref?: () => void }).unref?.();
      return Promise.resolve();
    }
    this.enqueue(chunk);
    return this.chain;
  }

  /** Send what is held, and resolve once everything pushed so far is out. */
  flush(): Promise<void> {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    const held = this.held;
    this.held = null;
    if (held) this.enqueue(held);
    return this.chain;
  }

  private enqueue(chunk: unknown) {
    this.chain = this.chain.then(() => this.send(chunk)).catch((err) => {
      // A lost live chunk is not a lost step: the step's messages are stored
      // at commit. It must not stop the chunks after it either.
      console.error('chunk not published', err);
    });
  }
}

/** A chunk as it is published: the SDK part, except that a tool result goes
 *  out in the shape the Go runtime sends, `{type, toolCallId, toolName,
 *  result}`, with `{error}` as the result of a call whose tool failed (the
 *  model got `error: <message>`, but a client marks the call failed from
 *  this), and a parked call's placeholder is not sent at all (its verdict
 *  arrives with the resume). Null means "do not publish". */
export function chunkPayload(chunk: unknown, toolErrors?: Map<string, string>): unknown | null {
  const c = chunk as { type?: string; toolCallId?: string; toolName?: string; result?: unknown } | null;
  if (c?.type !== 'tool-result') return chunk;
  const r = c.result as Record<string, unknown> | null;
  if (r && typeof r === 'object' && '__hitl_parked__' in r) return null;
  const failed = c.toolCallId ? toolErrors?.get(c.toolCallId) : undefined;
  if (failed !== undefined) toolErrors!.delete(c.toolCallId!);
  return {
    type: 'tool-result',
    toolCallId: c.toolCallId,
    toolName: c.toolName,
    result: failed !== undefined ? { error: failed } : c.result,
  };
}
