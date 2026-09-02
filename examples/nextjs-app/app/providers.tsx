'use client';

import type { ReactNode } from 'react';
import { AgentRunProvider, type AgentRunConfig } from 'use-agentrun';

/** Where this app's agent API lives, declared once. These happen to be the
 *  hook's defaults — spelled out here because they are the thing you change
 *  when your routes differ, and a component below can still override any one
 *  of them for a single view.
 *
 *  A route can also be a function when a path cannot express it, e.g.
 *    history: ({ threadId }) => `/api/threads/${threadId}/history`
 */
const config: AgentRunConfig = {
  routes: {
    run: '/api/agent/run',
    stop: '/api/agent/control',
    respond: '/api/agent/respond',
    stream: '/api/agent/stream',
    history: '/api/agent/history',
    usage: '/api/agent/usage',
    threads: '/api/threads',
    deleteThread: '/api/threads',
  },
  defaultModel: 'gpt-4o',
};

export function Providers({ children }: { children: ReactNode }) {
  return <AgentRunProvider config={config}>{children}</AgentRunProvider>;
}
