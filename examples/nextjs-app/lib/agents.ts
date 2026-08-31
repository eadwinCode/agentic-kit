import { runtime } from './runtime';

/** Registered agent handles (§4). The worker dispatches queue jobs back to
 *  these by name (`RunJob.agent`), and `run`/`stop` are bound per handle. */
export const chat = runtime.createStreamTextAgent({
  name: 'chat',
  model: 'gpt-4o',
  subagents: true, // opt-in: platform injects the scoped spawnSubagent tool (§2.7)
});
