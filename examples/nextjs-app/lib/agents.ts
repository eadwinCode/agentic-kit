import { z } from 'zod';
import { tool } from 'ai';
import { markRequiresConfirmation } from '@agent/core';
import { runtime } from './runtime';

/** Destructive demo tool — flagged for HITL (§2.5): the engine parks the call
 *  and asks for approval (INPUT_REQUIRED) instead of executing it. */
const sendEmail = markRequiresConfirmation(
  tool({
    description: 'Sends an email (destructive — requires user approval)',
    parameters: z.object({
      to: z.string().email(),
      subject: z.string(),
      body: z.string(),
    }),
    execute: async ({ to, subject, body }) => ({ status: 'SENT', to, subject, body }),
  }),
);

/** Registered agent handles (§4). The worker dispatches queue jobs back to
 *  these by name (`RunJob.agent`), and `run`/`stop` are bound per handle. */
export const chat = runtime.createStreamTextAgent({
  name: 'chat',
  model: 'gpt-4o',
  subagents: true, // opt-in: platform injects the scoped spawnSubagent tool (§2.7)
  tools: { sendEmail },
});
