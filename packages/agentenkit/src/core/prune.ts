import type { RuntimePorts } from '../ports/runtime.js';

/** The types older releases kept in the event log that now live only on a
 *  run stream or the bus. Nothing reads them from the table any more, so
 *  they can go. An app's own types are never among them: there is no
 *  telling whether the app wanted one kept. */
export const STREAM_ONLY_TYPES: readonly string[] = [
  'CHUNK',
  'SUBAGENT_CHUNK',
  'STEP_COMMITTED',
  'STEP_FINISHED',
  'TEXT_RESULT',
  'SUBAGENT_STARTED',
  'SUBAGENT_COMPLETED',
  'SUBAGENT_FAILED',
  'MESSAGE_APPENDED',
  'STATE_CHANGE',
  'HEARTBEAT',
];

export interface PruneOptions {
  /** Count what would go, and delete nothing. */
  dryRun?: boolean;
  /** Rows per batch; each batch is its own short delete. Default 10,000. */
  batchSize?: number;
}

export interface PruneReport {
  /** Rows deleted (or, on a dry run, found) per type. */
  byType: Record<string, number>;
  total: number;
  batches: number;
}

/** Delete the stream-only rows older releases left in the event log (see
 *  STREAM_ONLY_TYPES), a batch at a time, so a large table never holds one
 *  long lock. Safe to stop and run again: each batch stands on its own.
 *  Needs a storage whose events port can prune; one that cannot throws. */
export async function pruneEvents(deps: RuntimePorts, opts: PruneOptions = {}): Promise<PruneReport> {
  const prune = deps.storage.events.prune;
  if (!prune) throw new Error('pruneEvents: this storage cannot prune its events');
  const limit = opts.batchSize ?? 10_000;
  const report: PruneReport = { byType: {}, total: 0, batches: 0 };
  for (;;) {
    const counts = await prune([...STREAM_ONLY_TYPES], { limit, dryRun: opts.dryRun });
    report.batches++;
    let n = 0;
    for (const [type, count] of Object.entries(counts)) {
      report.byType[type] = (report.byType[type] ?? 0) + count;
      n += count;
    }
    report.total += n;
    // A dry run counts everything in one pass; a batch smaller than the
    // limit was the last one.
    if (opts.dryRun || n < limit) return report;
  }
}
