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
