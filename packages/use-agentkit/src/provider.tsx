'use client';

import type { ReactNode } from 'react';
import { AgentKitContext } from './context.js';
import type { AgentKitConfig } from './config.js';

export interface AgentKitProviderProps {
  /** Applied to every `useAgentThread` below it. A hook's own options still
   *  win, section by section. */
  config?: AgentKitConfig;
  children: ReactNode;
}

/** Set the routes, labels and transport once for the whole app instead of at
 *  every call site. */
export function AgentKitProvider({ config, children }: AgentKitProviderProps) {
  return <AgentKitContext.Provider value={config ?? null}>{children}</AgentKitContext.Provider>;
}
