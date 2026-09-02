import { tool, type ToolExecutionOptions } from 'ai';
import type { z } from 'zod';
import type { AgentRunState } from './state.js';
import type { ToolPublishEvent } from './publish.js';

/** What a tool's `execute` actually receives: the AI SDK's own options, plus
 *  the run's state (§2.10).
 *
 *  The platform injects `state` at call time, but the SDK's own
 *  `ToolExecutionOptions` has no field for it — and a plain `tool()` cannot be
 *  told about it, because narrowing the options parameter is rejected as
 *  unsound. Hence `agentTool`. */
export interface ToolContext extends ToolExecutionOptions {
  /** Whatever the caller attached to this run. Present for every tool of every
   *  run, including a nested one and a segment resumed after an approval. */
  state: AgentRunState;
  /** Publish an event of your own on this thread — a progress label, a
   *  preview URL, anything a client should react to. Durable by default, so a
   *  reconnecting client replays it; pass `{ durable: false }` for a notice.
   *  See the "Custom events" guide. */
  publishEvent: ToolPublishEvent;
  /** Present only when this call is the resumption of an approved park
   *  (§2.5): whatever the human sent back with the approval — answers to
   *  questions, a corrected value, a reason. Absent on a first, live call. */
  approval?: { payload?: unknown };
}

/** `tool()` with the run state typed.
 *
 * ```ts
 * const lookupInvoice = agentTool({
 *   description: 'Find one invoice',
 *   parameters: z.object({ invoiceId: z.string() }),
 *   execute: async ({ invoiceId }, { state }) =>
 *     db.invoice.findFirst({ where: { id: invoiceId, orgId: state.orgId } }),
 * });
 * ```
 *
 *  The result is an ordinary AI SDK tool — it composes with
 *  `markRequiresConfirmation` and can be passed anywhere `tool()` can. */
export function agentTool<PARAMETERS extends z.ZodTypeAny, RESULT>(spec: {
  description?: string;
  parameters: PARAMETERS;
  execute: (args: z.infer<PARAMETERS>, ctx: ToolContext) => PromiseLike<RESULT>;
}) {
  const { execute, ...rest } = spec;
  return tool({
    ...rest,
    execute: ((args: z.infer<PARAMETERS>, options: ToolExecutionOptions) =>
      execute(args, options as ToolContext)) as (
      args: z.infer<PARAMETERS>,
      options: ToolExecutionOptions,
    ) => PromiseLike<RESULT>,
  });
}
