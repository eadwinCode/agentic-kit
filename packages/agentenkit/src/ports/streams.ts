import type { StreamEnd, StreamEvent, StreamItem } from '../core/stream-events.js';

export type { StreamEnd, StreamEvent, StreamItem } from '../core/stream-events.js';

/** Who a stream belongs to. */
export interface StreamMeta {
  threadId: string;
  runId: string;
}

/** A stream as it stands now. */
export interface StreamSnapshot {
  meta: StreamMeta;
  items: StreamItem[];
  /** The end item when the stream is closed; it is also the last of `items`. */
  end: StreamEnd | null;
}

/** The stream does not exist: never opened, past its grace window, or
 *  deleted. A reader takes a fresh snapshot of the thread instead. */
export class StreamGoneError extends Error {
  constructor(readonly streamId: string) {
    super(`run stream ${streamId} is gone`);
    this.name = 'StreamGoneError';
  }
}

/** An append to a stream that is already closed. */
export class StreamClosedError extends Error {
  constructor(readonly streamId: string) {
    super(`run stream ${streamId} is closed`);
    this.name = 'StreamClosedError';
  }
}

/** Short-lived logs, one per run segment. Delivery lives here, not state:
 *  a stream is closed when its segment ends and deleted after a grace
 *  window. What must outlive the run is in the messages and the thread
 *  record.
 *
 *  Every adapter keeps these rules:
 *  - Offsets are opaque strings. Only the store compares them; a client
 *    echoes the last one back and nothing else.
 *  - Offsets strictly increase within a stream. One writer per stream: the
 *    run lock already makes sure of that.
 *  - `close` writes the end as the last item, so a reader that sees it
 *    stops, and one that comes late still gets it.
 *  - A missed wake-up only delays a reader, never drops an event. */
export interface RunStreams {
  /** Open a stream. Opening one that exists is a no-op. `ttlMs` deletes it
   *  even if it is never closed. */
  open(streamId: string, meta: StreamMeta, ttlMs: number): Promise<void>;
  /** Append in order; one offset per event. Throws StreamClosedError once
   *  closed and StreamGoneError when the stream does not exist. */
  append(streamId: string, events: StreamEvent[]): Promise<string[]>;
  /** The items after `after` (null: from the start), then live ones as they
   *  come. Ends after the end item, or when the signal aborts. Throws
   *  StreamGoneError when the stream does not exist, or goes while read. */
  read(streamId: string, after: string | null, signal?: AbortSignal): AsyncIterable<StreamItem>;
  /** The items after `after` now, without waiting. Null when the stream does
   *  not exist. */
  snapshot(streamId: string, after?: string | null): Promise<StreamSnapshot | null>;
  /** Write the end item and start the grace window: the stream is deleted
   *  `graceMs` later. Closing a closed stream is a no-op; closing one that
   *  does not exist throws StreamGoneError. */
  close(streamId: string, end: StreamEnd, graceMs: number): Promise<void>;
  /** Remove a stream now (thread delete). A missing one is a no-op. */
  delete(streamId: string): Promise<void>;
}
