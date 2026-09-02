'use client';

import type { ReactNode } from 'react';
import { AgentRunContext } from './context.js';
import type { AgentRunConfig } from './config.js';

export interface AgentRunProviderProps {
  /** Applied to every `useAgentThread` below it. A hook's own options still
   *  win, section by section. */
  config?: AgentRunConfig;
  children: ReactNode;
}

/** Set the routes, labels and transport once for the whole app instead of at
 *  every call site. */
export function AgentRunProvider({ config, children }: AgentRunProviderProps) {
  return <AgentRunContext.Provider value={config ?? null}>{children}</AgentRunContext.Provider>;
}
