import { DuplicateJobError, UnsupportedError, type EnqueueOptions, type Queue, type QueueStats, type QueuedJob } from '../ports/queue.js';
import type { RunJob } from '../core/types.js';

/** What QStash answers to a publish or an enqueue. */
interface QStashResult {
  messageId?: string;
  /** True when a message with the same deduplication id was already sent. */
  deduplicated?: boolean;
}

/** Minimal structural type of the @upstash/qstash (v2) client we use. */
export interface QStashLike {
  queue(a: { queueName: string }): {
    enqueueJSON(a: { url: string; body: unknown; deduplicationId?: string }): Promise<unknown>;
  };
  /** `delay` is in seconds. QStash supports it on publish only — an enqueue
   *  carrying Upstash-Delay is REJECTED outright ("Upstash-Not-Before/
   *  Upstash-Delay can not be used with enqueue"), so a delayed dispatch has
   *  to be published instead. */
  publishJSON(a: { url: string; body: unknown; delay?: number; deduplicationId?: string }): Promise<unknown>;
}

export interface QStashQueueOptions {
  /** Fully-qualified consumer URL, e.g. https://app.example.com/api/queue/agent-run */
  url: string;
  /** Queue name for flow control (§2.8). Defaults to `agent-runs`. */
  queueName?: string;
}

/** Reference Queue adapter over Upstash QStash HTTP queues.
 *
 *  A job's `key` dedupes on QStash's side (its deduplication id). QStash
 *  remembers an id for ten minutes, not for as long as the message waits, so
 *  a key reused after that goes out again; every caller treats a second
 *  delivery as a no-op. QStash has no priorities, cannot withdraw a message by
 *  key, and has no read side: `priority` is ignored, `cancel` does nothing
 *  (the delivered job is a correct no-op), and `find` / `stats` throw
 *  `UnsupportedError`. */
export class QStashQueue implements Queue {
  constructor(private readonly client: QStashLike, private readonly opts: QStashQueueOptions) {}

  async enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void> {
    const dedupe = opts?.key ? { deduplicationId: opts.key } : {};
    // A delayed job goes out as a published message rather than a queued one:
    // QStash rejects Upstash-Delay on enqueue. The trade is that this one
    // message skips the queue's flow control — acceptable for the things that
    // ask for a delay (a HITL expiry, a retry backoff, a blocked job's
    // redrive), since the run lock already serializes them.
    const res = (opts?.delaySeconds
      ? await this.client.publishJSON({
          url: this.opts.url, body: job, delay: Math.max(1, Math.ceil(opts.delaySeconds)), ...dedupe,
        })
      : await this.client
          .queue({ queueName: this.opts.queueName ?? 'agent-runs' })
          .enqueueJSON({ url: this.opts.url, body: job, ...dedupe })) as QStashResult | undefined;
    if (opts?.key && res?.deduplicated) throw new DuplicateJobError(opts.key);
  }

  async cancel(_key: string): Promise<void> {}

  async find(_runId: string): Promise<QueuedJob | null> {
    throw new UnsupportedError('QStashQueue.find');
  }

  async stats(): Promise<QueueStats> {
    throw new UnsupportedError('QStashQueue.stats');
  }
}
