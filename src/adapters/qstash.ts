import type { EnqueueOptions, Queue } from '../ports/queue.js';
import type { RunJob } from '../core/types.js';

/** Minimal structural type of the @upstash/qstash (v2) client we use. */
export interface QStashLike {
  queue(a: { queueName: string }): {
    /** `delay` is in seconds — QStash holds the message that long (§2.8). */
    enqueueJSON(a: { url: string; body: unknown; delay?: number }): Promise<unknown>;
  };
}

export interface QStashQueueOptions {
  /** Fully-qualified consumer URL, e.g. https://app.example.com/api/queue/agent-run */
  url: string;
  /** Queue name for flow control (§2.8). Defaults to `agent-runs`. */
  queueName?: string;
}

/** Reference Queue adapter over Upstash QStash HTTP queues. */
export class QStashQueue implements Queue {
  constructor(private readonly client: QStashLike, private readonly opts: QStashQueueOptions) {}

  enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void> {
    return this.client
      .queue({ queueName: this.opts.queueName ?? 'agent-runs' })
      .enqueueJSON({
        url: this.opts.url,
        body: job,
        ...(opts?.delaySeconds ? { delay: opts.delaySeconds } : {}),
      })
      .then(() => undefined);
  }
}
