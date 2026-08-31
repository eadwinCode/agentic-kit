'use client';

import { useState } from 'react';
import { useAgentThread } from '../hooks/useAgentThread';

const stateLabel: Record<string, string> = {
  IDLE: 'idle',
  RUNNING: 'running…',
  WAITING_FOR_INPUT: 'waiting for your approval',
  CANCELLED: 'stopped',
  COMPLETED: 'completed',
  FAILED: 'failed',
};

export default function Page() {
  const {
    threadId,
    entries,
    agentState,
    activity,
    historyLoading,
    pendingInput,
    subagents,
    run,
    stop,
    respondToInput,
  } = useAgentThread();
  const [prompt, setPrompt] = useState('');
  const busy =
    historyLoading || agentState === 'RUNNING' || agentState === 'WAITING_FOR_INPUT';

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!prompt.trim() || busy) return;
    void run(prompt.trim());
    setPrompt('');
  };

  return (
    <main>
      <header>
        <h1>
          <span className={`dot ${agentState.toLowerCase()}`} /> @agent/core example
        </h1>
        <p className="hint">
          {threadId ? (
            <>
              thread <code>{threadId}</code> · {stateLabel[agentState]} · open this page in a
              second tab — it stays in sync (§2.2)
            </>
          ) : (
            <>type a prompt and hit Run — execution is queue-dispatched and survives disconnects (§2.1)</>
          )}
        </p>
      </header>

      {(threadId || activity.phase !== 'idle') && (
        <section className={`activity ${activity.phase}`} aria-live="polite">
          <span className="activity-mark" aria-hidden="true" />
          <div>
            <strong>{activity.label}</strong>
            {activity.detail && <span>{activity.detail}</span>}
          </div>
        </section>
      )}

      <section className="thread">
        {entries.length === 0 && (
          <p className="empty">{historyLoading ? 'Loading previous messages…' : 'No messages yet.'}</p>
        )}
        {entries.map((e) =>
          e.kind === 'tool' ? (
            <p key={e.id} className="tool">
              {e.text}
            </p>
          ) : (
            <div key={e.id} className={`message ${e.role}`}>
              <span className="message-role">{e.role === 'user' ? 'You' : 'Agent'}</span>
              <div className="bubble">{e.text}</div>
            </div>
          ),
        )}

        {subagents.map((s) => (
          <div key={s.agentId} className={`subagent ${s.status.toLowerCase()}`}>
            <p className="subagent-head">
              ▸ {s.name} <span className={`dot ${s.status.toLowerCase()}`} /> {s.status}
            </p>
            {s.text && <pre>{s.text}</pre>}
          </div>
        ))}

        {agentState === 'WAITING_FOR_INPUT' && pendingInput && (
          <div className="hitl">
            <p>
              ⏸ approval required — <code>{pendingInput.toolName}</code>
              {pendingInput.agentId && (
                <>
                  {' '}
                  requested by subagent <code>{pendingInput.agentId}</code>
                </>
              )}
            </p>
            <pre>{truncate(JSON.stringify(pendingInput.arguments, null, 2), 400)}</pre>
            <div className="row">
              <button className="approve" onClick={() => void respondToInput(true)}>
                Approve
              </button>
              <button className="deny" onClick={() => void respondToInput(false)}>
                Deny
              </button>
            </div>
          </div>
        )}
      </section>

      <form className="composer" onSubmit={submit}>
        <input
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          placeholder={historyLoading ? 'loading conversation…' : busy ? 'run in progress…' : 'Ask the agent something…'}
          disabled={busy}
        />
        <button type="submit" disabled={busy || !prompt.trim()}>
          Run
        </button>
        <button type="button" className="stop" onClick={() => void stop()} disabled={!busy}>
          ■ Stop
        </button>
      </form>
    </main>
  );
}

function truncate(s: string, n = 220): string {
  return s.length > n ? `${s.slice(0, n)}…` : s;
}
