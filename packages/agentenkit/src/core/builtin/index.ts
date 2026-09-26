import { jsonSchema, tool, type Tool } from 'ai';
import type { BuiltinToolPorts } from '../../ports/tools.js';
import { BUILTIN_TOOL_DEFINITIONS, BUILTIN_TOOL_NAMES, type BuiltinToolName } from './definitions.js';
import { toolRunOf } from './run.js';
import { runWebFetch, runWebSearch, type WebFetchOptions, type WebSearchOptions } from './web.js';

export interface BuiltinToolOptions {
  webSearch?: WebSearchOptions;
  webFetch?: WebFetchOptions;
}

/** The port each tool needs, as `setupAgentCore({ tools })` names it. */
const NEEDS: Record<BuiltinToolName, keyof BuiltinToolPorts> = {
  web_search: 'search',
  web_fetch: 'fetcher',
};

/** The built-in tools, ready for an agent's `tools`. Each is an ordinary AI
 *  SDK tool with a fixed name, description and input, so any model that can
 *  call tools can use it. A name this runtime does not know, or a tool whose
 *  port was not set up, throws here, at startup. */
export function buildBuiltinTools(
  ports: BuiltinToolPorts,
  names: readonly BuiltinToolName[],
  options: BuiltinToolOptions = {},
): Record<string, Tool> {
  const out: Record<string, Tool> = {};
  for (const name of names) {
    if (!(BUILTIN_TOOL_NAMES as readonly string[]).includes(name)) {
      throw new Error(`builtinTools: no built-in tool is called ${JSON.stringify(name)}`);
    }
    const port = NEEDS[name];
    if (!ports[port]) {
      throw new Error(`builtinTools: ${name} needs setupAgentCore({ tools: { ${port} } })`);
    }
    const def = BUILTIN_TOOL_DEFINITIONS[name];
    out[name] = tool({
      description: def.description,
      parameters: jsonSchema(def.inputSchema as Parameters<typeof jsonSchema>[0]),
      execute: async (args: unknown, opts: { abortSignal?: AbortSignal }): Promise<unknown> => {
        const run = toolRunOf(opts);
        if (!run) return { error: `${name} runs only inside an agentenkit run` };
        const a = (args ?? {}) as Record<string, unknown>;
        return name === 'web_search'
          ? runWebSearch(ports.search!, options.webSearch ?? {}, a, run, opts.abortSignal)
          : runWebFetch(ports.fetcher!, options.webFetch ?? {}, a, run, opts.abortSignal);
      },
    });
  }
  return out;
}

export { BUILTIN_TOOL_DEFINITIONS, BUILTIN_TOOL_NAMES, type BuiltinToolName } from './definitions.js';
export type { WebFetchOptions, WebSearchOptions } from './web.js';
