'use client';

import { useState } from 'react';
import { useAgentThread, type ThreadUsage } from '../hooks/useAgentThread';

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
    threads,
    threadsLoading,
    usage,
    selectThread,
    deleteThread,
    newThread,
    run,
    stop,
    respondToInput,
  } = useAgentThread();
  const [prompt, setPrompt] = useState('');
  // The message being edited, and its working text. Editing is a resend: the
  // turn and everything after it is replaced (§5.1).
  const [editing, setEditing] = useState<{ id: string; text: string } | null>(null);
  // One button, two jobs: while a run is live it stops; otherwise it sends.
  const running = agentState === 'RUNNING' || agentState === 'WAITING_FOR_INPUT';
  const canSend = !historyLoading && !running && prompt.trim().length > 0;

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!canSend) return;
    void run(prompt.trim());
    setPrompt('');
  };

  const submitEdit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!editing || running || !editing.text.trim()) return;
    void run(editing.text.trim(), 'gpt-4o', editing.id);
    setEditing(null);
  };

  return (
    <div className="app-shell">
      <aside className="thread-sidebar" aria-label="Conversation threads">
        <div className="thread-sidebar-head">
          <strong>Threads</strong>
          <button type="button" className="new-thread" onClick={newThread}>
            + New
          </button>
        </div>
        <nav className="thread-list">
          {threadsLoading && threads.length === 0 && (
            <span className="thread-list-empty">Loading threads…</span>
          )}
          {!threadsLoading && threads.length === 0 && (
            <span className="thread-list-empty">No threads yet.</span>
          )}
          {threads.map((thread) => (
            <div key={thread.id} className="thread-option-row">
              <button
                type="button"
                className={`thread-option ${thread.id === threadId ? 'active' : ''}`}
                onClick={() => selectThread(thread.id)}
                aria-current={thread.id === threadId ? 'page' : undefined}
              >
                <span className="thread-option-title">{thread.title || 'New Thread'}</span>
                <span className="thread-option-meta">
                  <span
                    className={`thread-status ${thread.state.toLowerCase()}`}
                    aria-label={`Status: ${stateLabel[thread.state] ?? thread.state.toLowerCase()}`}
                  >
                    <span className="thread-status-dot" aria-hidden="true" />
                    {stateLabel[thread.state] ?? thread.state.toLowerCase()}
                  </span>
                  <time dateTime={thread.updatedAt}>{formatRelativeDate(thread.updatedAt)}</time>
                </span>
              </button>
              <button
                type="button"
                className="thread-delete"
                title="Delete thread (cascades messages, events, usage)"
                onClick={() => void deleteThread(thread.id)}
              >
                ×
              </button>
            </div>
          ))}
        </nav>
      </aside>

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
            <>type a prompt and send — execution is queue-dispatched and survives disconnects (§2.1)</>
          )}
        </p>
        {usage && <UsageBar usage={usage} />}
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
              {editing?.id === e.id ? (
                <form className="bubble editing" onSubmit={submitEdit}>
                  <textarea
                    value={editing.text}
                    autoFocus
                    rows={Math.min(8, editing.text.split('\n').length + 1)}
                    onChange={(ev) => setEditing({ id: e.id, text: ev.target.value })}
                    onKeyDown={(ev) => {
                      if (ev.key === 'Escape') setEditing(null);
                      if (ev.key === 'Enter' && !ev.shiftKey) {
                        ev.preventDefault();
                        submitEdit(ev);
                      }
                    }}
                  />
                  <div className="edit-actions">
                    <span className="edit-note">replaces everything below</span>
                    <button type="button" onClick={() => setEditing(null)}>
                      Cancel
                    </button>
                    <button type="submit" className="send" disabled={!editing.text.trim()}>
                      Resend
                    </button>
                  </div>
                </form>
              ) : (
                <div className="bubble">
                  {e.text}
                  {e.role === 'user' && !e.id.startsWith('optimistic:') && (
                    <button
                      type="button"
                      className="edit-message"
                      title={running ? 'Stop the run to edit' : 'Edit and resend'}
                      aria-label="Edit this message and resend"
                      disabled={running}
                      onClick={() => setEditing({ id: e.id, text: e.text })}
                    >
                      ✎
                    </button>
                  )}
                </div>
              )}
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
          placeholder={
            historyLoading
              ? 'loading conversation…'
              : running
                ? 'stop the run to send this…'
                : 'Ask the agent something…'
          }
          disabled={historyLoading}
        />
        <button
          type={running ? 'button' : 'submit'}
          className={`composer-action ${running ? 'stop' : 'send'}`}
          onClick={running ? () => void stop() : undefined}
          disabled={!running && !canSend}
          title={running ? 'Stop this run' : 'Send'}
          aria-label={running ? 'Stop this run' : 'Send message'}
        >
          <span aria-hidden="true">{running ? '■' : '↑'}</span>
        </button>
      </form>
      </main>
    </div>
  );
}

/** Tokens spent (§4) and how full the next prompt would be (§2.6). The
 *  context numbers are the platform's own estimate, so the bar moves at the
 *  same moment the engine decides to compact. */
function UsageBar({ usage }: { usage: ThreadUsage }) {
  const { tokens, context } = usage;
  const pct = context.budgetTokens
    ? (context.usedTokens / context.budgetTokens) * 100
    : 0;
  const triggerPct = context.budgetTokens
    ? (context.compactAtTokens / context.budgetTokens) * 100
    : 0;
  const near = context.usedTokens >= context.compactAtTokens;

  return (
    <section className="usage" aria-label="Token and context usage">
      <div className="usage-item">
        <span className="usage-label">Tokens</span>
        <strong>{formatTokens(tokens.totalTokens)}</strong>
        <span className="usage-detail">
          {formatTokens(tokens.inputTokens)} in · {formatTokens(tokens.cachedInputTokens)} cached
          · {formatTokens(tokens.outputTokens)} out
        </span>
      </div>

      <div className={`usage-item ${near ? 'near' : ''}`}>
        <span className="usage-label">Context</span>
        <strong>{pct < 1 && pct > 0 ? '<1' : Math.round(pct)}%</strong>
        <span className="usage-detail">
          {formatTokens(context.usedTokens)} / {formatTokens(context.budgetTokens)} ·{' '}
          {context.messages} msg · compacts at {formatTokens(context.compactAtTokens)}
        </span>
        <div
          className="usage-bar"
          role="progressbar"
          aria-valuenow={Math.round(pct)}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-label={`Context ${Math.round(pct)} percent full`}
        >
          <span className="usage-bar-fill" style={{ width: `${Math.min(100, pct)}%` }} />
          <span
            className="usage-bar-mark"
            style={{ left: `${Math.min(100, triggerPct)}%` }}
            title={`Compaction runs above ${formatTokens(context.compactAtTokens)} tokens`}
          />
        </div>
      </div>
    </section>
  );
}

function formatTokens(n: number): string {
  return n >= 10_000
    ? new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(n)
    : n.toLocaleString();
}

function truncate(s: string, n = 220): string {
  return s.length > n ? `${s.slice(0, n)}…` : s;
}

function formatRelativeDate(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? ''
    : new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric' }).format(date);
}
