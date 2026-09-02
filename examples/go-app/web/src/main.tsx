import React from 'react';
import { createRoot } from 'react-dom/client';
import { AgentRunProvider, type AgentRunConfig } from 'use-agentenkit';
import { App } from './App';
import './styles.css';

/** Where the Go server's agent API lives. These are the hook's defaults,
 *  spelled out because they are the thing you change when your routes differ. */
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
};

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <AgentRunProvider config={config}>
      <App />
    </AgentRunProvider>
  </React.StrictMode>,
);
