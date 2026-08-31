import type { Queue } from '../ports/queue.js';

/** Minimal structural type of the @upstash/qstash (v2) client we use. */
export interface QStashLike {
  queue(a: { queueName: string }): {
    enqueueJSON(a: { url: string; body: unknown }): Promise<unknown>;
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

  enqueue(job: { threadId: string; model: string }): Promise<void> {
    return this.client
      .queue({ queueName: this.opts.queueName ?? 'agent-runs' })
      .enqueueJSON({ url: this.opts.url, body: job })
      .then(() => undefined);
  }
}
