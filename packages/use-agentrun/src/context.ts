'use client';

import { createContext, useContext } from 'react';
import type { AgentRunConfig } from './config.js';

/** Holds the RAW config, not a resolved one: a hook's own options are merged
 *  over it before defaults are applied, so a component can override one route
 *  without restating the rest. */
export const AgentRunContext = createContext<AgentRunConfig | null>(null);

/** The config from the nearest provider, if there is one. */
export function useAgentRunConfig(): AgentRunConfig | null {
  return useContext(AgentRunContext);
}

/** Later wins, per section — so `{ routes: { run } }` replaces only `run`. */
export function mergeConfig(
  base: AgentRunConfig | null,
  over: AgentRunConfig,
): AgentRunConfig {
  if (!base) return over;
  return {
    ...base,
    ...over,
    routes: { ...base.routes, ...over.routes },
    labels: { ...base.labels, ...over.labels },
    format: { ...base.format, ...over.format },
  };
}
