import { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import {
  formatCost,
  useAgentThread,
  type PendingInput,
  type StreamEvent,
  type SubagentStatus,
  type ThreadUsage,
} from 'use-agentenkit';
import { initialCustomState, isCustomEvent, reduceCustom } from './events';

const stateLabel: Record<string, string> = {
  IDLE: 'idle', RUNNING: 'running…', WAITING_FOR_INPUT: 'waiting for you',
  CANCELLED: 'stopped', COMPLETED: 'completed', FAILED: 'failed',
};

/** The run state (§2.10) this client attaches to every run. A real app takes
 *  it from its session; the server never reads it, the tools do. */
const runState = { orgId: 'acme', userId: 'u_demo' };

interface Credit {
  remaining: number;
  allowance: number;
}

export function App() {
  const [custom, dispatch] = useReducer(reduceCustom, initialCustomState);
  // The thread's token allowance, from the demo's billing endpoint. Read on
  // thread change, after each run, and after the two billing buttons, so
  // what the pre-check will decide is visible before a message is sent.
  const [credit, setCredit] = useState<Credit | null>(null);
  // onEvent and refreshCredit are created before the hook hands the thread
  // id back; a ref lets them read the current one at call time.
  const threadIdRef = useRef<string | undefined>(undefined);
  const refreshCredit = useCallback(async (id: string | undefined) => {
    if (!id) return setCredit(null);
    try {
      const res = await fetch(`/api/demo/credit?threadId=${encodeURIComponent(id)}`);
      // A thread that was closed while the request was in flight (a stale
      // id from persistence, a 404 on history) must not paint its balance.
      if (res.ok && threadIdRef.current === id) setCredit((await res.json()) as Credit);
    } catch {
      // the panel is best-effort
    }
  }, []);
  const {
    threadId, entries, agentState, activity, historyLoading, pendingInputs, subagents,
    threads, threadsLoading, usage, selectThread, deleteThread, newThread, run, stop, respondToInput,
  } = useAgentThread({
    // Every event, replayed and live, before the hook's own reducer. Returning
    // true for the app's own types keeps them out of the built-in one.
    onEvent: (event) => {
      dispatch({ type: 'event', event });
      // A run ending, or being refused, moves the balance
      if (event.type === 'STATE_CHANGE' || event.type === 'RUN_REFUSED' || event.type === 'CREDIT_RESTORED') {
        void refreshCredit(threadIdRef.current);
      }
      return isCustomEvent(event.type);
    },
  });
  // The snapshot only replays the ACTIVE run's events. Durable custom events
  // from earlier runs (a preview rendered yesterday) are still in the log, so
  // on thread change replay the whole log through the same reducer.
  threadIdRef.current = threadId;
  useEffect(() => {
    dispatch({ type: 'clear' });
    void refreshCredit(threadId);
    if (!threadId) return;
    let cancelled = false;
    void fetch(`/api/agent/events?threadId=${encodeURIComponent(threadId)}&since=-1`)
      .then((res) => (res.ok ? res.json() : { events: [] }))
      .then(({ events }) => {
        if (cancelled) return;
        for (const event of events as StreamEvent[]) dispatch({ type: 'event', event });
      })
      .catch(() => undefined);
    return () => { cancelled = true; };
  }, [threadId, refreshCredit]);

  const [prompt, setPrompt] = useState('');
  const [editing, setEditing] = useState<{ id: string; text: string } | null>(null);
  const [openAgents, setOpenAgents] = useState<Record<string, boolean>>({});
  // Thought blocks a user has toggled by hand; anything absent falls back to
  // the default for its state: open while it streams, folded once done.
  const [openThoughts, setOpenThoughts] = useState<Record<string, boolean>>({});

  const running = agentState === 'RUNNING' || agentState === 'WAITING_FOR_INPUT';
  const waiting = agentState === 'WAITING_FOR_INPUT';
  // Exhausted by the button, or by spending: either way the next message
  // will be refused, and the panel says so before it happens.
  const limited = credit !== null && credit.remaining <= 0;
  // A thought is still being written while it is the newest entry of a live
  // run; once the answer starts, another entry follows it.
  const lastEntry = entries.at(-1);
  const streamingThoughtId = running && lastEntry?.kind === 'reasoning' ? lastEntry.id : undefined;
  const thoughtOpen = (id: string) => openThoughts[id] ?? id === streamingThoughtId;
  // The composer stays open under a credit limit: sending is how the user
  // meets the billing check, and the chat shows the refusal.
  const canSend = !historyLoading && !running && prompt.trim().length > 0;

  const send = (text: string, extra: Record<string, unknown> = {}) => run(text, { state: runState, ...extra });

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!canSend) return;
    void send(prompt.trim());
    setPrompt('');
  };
  const submitEdit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!editing || running || !editing.text.trim()) return;
    void send(editing.text.trim(), { editMessageId: editing.id });
    setEditing(null);
  };

  // The billing side of the demo. Limiting stops a live run and refuses new
  // ones until cleared; both sides tell the client through events, so this
  // component never sets creditLimit itself.
  const simulateCreditLimit = async () => {
    if (!threadId) return;
    await fetch('/api/demo/credit-limit', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ threadId }),
    });
    await refreshCredit(threadId);
  };
  const clearCreditLimit = async () => {
    if (!threadId) return;
    await fetch(`/api/demo/credit-limit?threadId=${encodeURIComponent(threadId)}`, { method: 'DELETE' });
    await refreshCredit(threadId);
  };

  return (
    <div className="app-shell">
      <aside className="thread-sidebar" aria-label="Conversation threads">
        <div className="thread-sidebar-head">
          <strong>Threads</strong>
          <button type="button" className="new-thread" onClick={newThread}>+ New</button>
        </div>
        <nav className="thread-list">
          {threadsLoading && threads.length === 0 && <span className="thread-list-empty">Loading threads…</span>}
          {!threadsLoading && threads.length === 0 && <span className="thread-list-empty">No threads yet.</span>}
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
                  <span className={`thread-status ${thread.state.toLowerCase()}`}>
                    <span className="thread-status-dot" aria-hidden="true" />
                    {stateLabel[thread.state] ?? thread.state.toLowerCase()}
                  </span>
                  <time dateTime={thread.updatedAt}>{formatRelativeDate(thread.updatedAt)}</time>
                </span>
              </button>
              <button type="button" className="thread-delete" title="Delete thread" onClick={() => void deleteThread(thread.id)}>×</button>
            </div>
          ))}
        </nav>
      </aside>

      <main>
        <header>
          <h1><span className={`dot ${agentState.toLowerCase()}`} /> agentenkit · Go example</h1>
          <p className="hint">
            {threadId ? (
              <>thread <code>{threadId}</code> · {stateLabel[agentState]} · open this page in a second tab, it stays in sync</>
            ) : (
              <>Go server, React client. Try: <em>weather in Paris and Rome</em>, <em>render a logo for a coffee brand</em>, <em>ask me questions about my design</em>, <em>send an email to the client</em>, <em>research goroutines</em></>
            )}
          </p>
          {usage && <UsageBar usage={usage} />}
        </header>

        {(threadId || activity.phase !== 'idle') && (
          <section className={`activity ${activity.phase}`} aria-live="polite">
            <span className="activity-mark" aria-hidden="true" />
            <div>
              <strong>{custom.progress ?? activity.label}</strong>
              {!custom.progress && activity.detail && <span>{activity.detail}</span>}
            </div>
          </section>
        )}

        <section className="thread">
          {entries.length === 0 && (
            <p className="empty">{historyLoading ? 'Loading previous messages…' : 'No messages yet.'}</p>
          )}
          {entries.map((e) =>
            e.kind === 'reasoning' ? (
              <ThoughtBlock
                key={e.id}
                text={e.text}
                streaming={e.id === streamingThoughtId}
                isOpen={thoughtOpen(e.id)}
                onToggle={() => setOpenThoughts((prev) => ({ ...prev, [e.id]: !thoughtOpen(e.id) }))}
              />
            ) : e.kind === 'tool' ? (
              <p key={e.id} className="tool">{e.text}</p>
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
                        if (ev.key === 'Enter' && !ev.shiftKey) { ev.preventDefault(); submitEdit(ev); }
                      }}
                    />
                    <div className="edit-actions">
                      <span className="edit-note">replaces everything below</span>
                      <button type="button" onClick={() => setEditing(null)}>Cancel</button>
                      <button type="submit" className="send" disabled={!editing.text.trim()}>Resend</button>
                    </div>
                  </form>
                ) : (
                  <div className="bubble">
                    {e.text}
                    {e.role === 'user' && !e.id.startsWith('optimistic:') && (
                      <button
                        type="button" className="edit-message" disabled={running}
                        title={running ? 'Stop the run to edit' : 'Edit and resend'}
                        onClick={() => setEditing({ id: e.id, text: e.text })}
                      >✎</button>
                    )}
                  </div>
                )}
              </div>
            ),
          )}

          {custom.notice && (
            <p className={`notice ${custom.notice.kind}`} role="status">
              {custom.notice.kind === 'credit' ? '⛔' : '⏳'} {custom.notice.text}
            </p>
          )}

          {waiting && pendingInputs.length > 1 && (
            <p className="hitl-count">{pendingInputs.length} approvals open — the run continues once every one is answered</p>
          )}

          {subagents
            .filter((s) => s.status === 'RUNNING' || s.status === 'WAITING_FOR_INPUT')
            .map((s) => {
              const open = openAgents[s.agentId] ?? s.status !== 'RUNNING';
              const asks = waiting ? pendingInputs.filter((req) => req.agentId === s.agentId) : [];
              return (
                <div key={s.agentId} className={`subagent ${s.status.toLowerCase()}`}>
                  <button type="button" className="subagent-head" aria-expanded={open}
                    onClick={() => setOpenAgents((prev) => ({ ...prev, [s.agentId]: !open }))}>
                    <span className="subagent-caret" aria-hidden="true">{open ? '▾' : '▸'}</span>
                    {s.name}
                    {s.depth > 1 && <span className="subagent-depth">depth {s.depth}</span>}
                    <span className={`dot ${s.status.toLowerCase()}`} />
                    {subagentLabel[s.status]}
                  </button>
                  {open && s.text && <pre>{s.text}</pre>}
                  {open && s.error && <p className="subagent-error">✕ {s.error}</p>}
                  {asks.map((req) => <Approval key={req.toolCallId} req={req} onRespond={respondToInput} />)}
                </div>
              );
            })}

          {waiting && pendingInputs.filter((req) => !req.agentId).map((req) => (
            <Approval key={req.toolCallId} req={req} onRespond={respondToInput} />
          ))}
        </section>

        <form className="composer" onSubmit={submit}>
          <input
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            placeholder={historyLoading ? 'loading conversation…' : running ? 'stop the run to send this…' : 'Ask the agent something…'}
            disabled={historyLoading}
          />
          <button
            type={running ? 'button' : 'submit'}
            className={`composer-action ${running ? 'stop' : 'send'}`}
            onClick={running ? () => void stop() : undefined}
            disabled={!running && !canSend}
            title={running ? 'Stop this run' : 'Send'}
          >
            <span aria-hidden="true">{running ? '■' : '↑'}</span>
          </button>
        </form>
      </main>

      <aside className="events-panel" aria-label="Custom events">
        <div className="events-head">
          <strong>Custom events</strong>
          {limited ? (
            <button type="button" className="new-thread" disabled={!threadId} onClick={() => void clearCreditLimit()}
              title="Lifts the limit: new runs are accepted again">
              Clear limit
            </button>
          ) : (
            <button type="button" className="new-thread" disabled={!threadId} onClick={() => void simulateCreditLimit()}
              title="Sets the thread's allowance to zero, like a billing webhook would: the next message meets the pre-check">
              Simulate credit limit
            </button>
          )}
        </div>

        {credit && (
          <div className={`card ${limited ? 'credit' : ''}`}>
            <strong>Credits</strong>
            <span>{credit.remaining.toLocaleString()} of {credit.allowance.toLocaleString()} tokens left on this thread</span>
            <div className="usage-bar" role="progressbar" aria-valuenow={credit.remaining} aria-valuemin={0} aria-valuemax={credit.allowance}>
              <span className="usage-bar-fill" style={{ width: `${Math.min(100, (credit.remaining / credit.allowance) * 100)}%` }} />
            </div>
            {limited && (
              <span>
                Exhausted: the next message meets the billing check and is refused
                {custom.creditLimit ? ` · resets ${formatRelativeDate(custom.creditLimit.resetAt)}` : ''}
              </span>
            )}
          </div>
        )}
        {custom.error && <div className="card error"><strong>Run failed</strong><span>{custom.error}</span></div>}
        {custom.progress && <div className="card progress"><span className="activity-mark" /> {custom.progress}</div>}
        {custom.emailSent && (
          <div className="card"><strong>Email sent</strong><span>to {custom.emailSent.to} · {custom.emailSent.subject}</span></div>
        )}
        {custom.answers && (
          <div className="card">
            <strong>Your answers</strong>
            {Object.entries(custom.answers).map(([q, a]) => <span key={q}>{q} <em>{a}</em></span>)}
          </div>
        )}
        {custom.preview ? (
          <figure className="preview">
            <img src={custom.preview.url} alt={custom.preview.brief} />
            <figcaption>{custom.preview.brief}</figcaption>
          </figure>
        ) : (
          <p className="thread-list-empty">No design yet. Ask for one.</p>
        )}

        <div className="events-log">
          <strong>Event log</strong>
          {custom.log.length === 0 && <span className="thread-list-empty">Nothing published yet.</span>}
          {custom.log.map((e, i) => (
            <div key={`${e.seq}-${i}`} className="event-row">
              <code>{e.type}</code>
              <span className="event-seq">{e.seq === 0 ? 'notice' : `#${e.seq}`}</span>
              <span className="event-payload">{truncate(JSON.stringify(e.payload), 90)}</span>
            </div>
          ))}
        </div>
      </aside>
    </div>
  );
}

const subagentLabel: Record<SubagentStatus, string> = {
  RUNNING: 'working', WAITING_FOR_INPUT: 'waiting for you', COMPLETED: 'done', FAILED: 'failed', CANCELLED: 'stopped',
};

/** One approval (§2.5). A question tool gets a form: the answers ride the
 *  approval's payload back into the tool. Anything else gets approve/deny. */
function Approval({ req, onRespond }: {
  req: PendingInput;
  onRespond: (toolCallId: string, approved: boolean, payload?: unknown) => void | Promise<unknown>;
}) {
  const args = (req.arguments ?? {}) as Record<string, any>;
  const questions: string[] = req.toolName === 'askDesignQuestions' && Array.isArray(args.questions) ? args.questions : [];
  const [answers, setAnswers] = useState<Record<string, string>>({});
  const asker = req.agentName ? <> asked by <strong>{req.agentName}</strong></> : ' asked by the main agent';

  if (questions.length > 0) {
    const complete = questions.every((q) => answers[q]?.trim());
    return (
      <form className="hitl questions" onSubmit={(e) => { e.preventDefault(); if (complete) void onRespond(req.toolCallId, true, answers); }}>
        <p>❓ the agent has questions{asker}</p>
        {questions.map((q) => (
          <label key={q}>
            <span>{q}</span>
            <input value={answers[q] ?? ''} onChange={(e) => setAnswers((prev) => ({ ...prev, [q]: e.target.value }))} />
          </label>
        ))}
        <div className="row">
          <button type="submit" className="approve" disabled={!complete}>Send answers</button>
          <button type="button" className="deny" onClick={() => void onRespond(req.toolCallId, false)}>Skip</button>
        </div>
      </form>
    );
  }
  return (
    <div className="hitl">
      <p>⏸ approval required — <code>{req.toolName}</code>{asker}</p>
      <pre>{truncate(JSON.stringify(req.arguments, null, 2), 400)}</pre>
      <div className="row">
        <button className="approve" onClick={() => void onRespond(req.toolCallId, true)}>Approve</button>
        <button className="deny" onClick={() => void onRespond(req.toolCallId, false)}>Deny</button>
      </div>
    </div>
  );
}

/** The model's thinking. Shown live while it is being written, then folded
 *  to a single line: it is context for the answer, not the answer. */
function ThoughtBlock({ text, streaming, isOpen, onToggle }: {
  text: string;
  streaming: boolean;
  isOpen: boolean;
  onToggle: () => void;
}) {
  return (
    <div className={`thought${streaming ? ' streaming' : ''}${isOpen ? ' open' : ''}`}>
      <button type="button" className="thought-header" onClick={onToggle}>
        <span className="thought-caret" aria-hidden>{isOpen ? '▾' : '▸'}</span>
        <span className="thought-title">{streaming ? 'Thinking' : 'Thought'}</span>
        {!isOpen && <span className="thought-peek">{truncate(text.replace(/\s+/g, ' '), 90)}</span>}
      </button>
      {isOpen && <div className="thought-body">{text}</div>}
    </div>
  );
}

function UsageBar({ usage }: { usage: ThreadUsage }) {
  const { tokens, context } = usage;
  const pct = context.budgetTokens ? (context.usedTokens / context.budgetTokens) * 100 : 0;
  const triggerPct = context.budgetTokens ? (context.compactAtTokens / context.budgetTokens) * 100 : 0;
  // What the thread has spent (§4). The runtime prices every model call before
  // it stores the usage row, so this comes off the same store the token counts
  // do. Null when nothing was priced — a server with no pricer configured
  // leaves the slot empty rather than claiming $0.00.
  const cost = formatCost(tokens);
  return (
    <section className="usage" aria-label="Token, cost and context usage">
      <div className="usage-item">
        <span className="usage-label">Tokens</span>
        <strong>{formatTokens(tokens.totalTokens)}</strong>
        <span className="usage-detail">{formatTokens(tokens.inputTokens)} in · {formatTokens(tokens.cachedInputTokens)} cached · {formatTokens(tokens.outputTokens)} out</span>
      </div>
      {cost && (
        <div className="usage-item">
          <span className="usage-label">Cost</span>
          <strong>{cost}</strong>
          <span className="usage-detail">
            {tokens.lines?.map((l) => `${l.agentName ?? 'agent'} · ${l.calls} call${l.calls === 1 ? '' : 's'}`).join(' · ') || 'across every model call'}
            {tokens.unpriced ? ` · ${tokens.unpriced} unpriced` : ''}
          </span>
        </div>
      )}
      <div className={`usage-item ${context.usedTokens >= context.compactAtTokens ? 'near' : ''}`}>
        <span className="usage-label">Context</span>
        <strong>{pct < 1 && pct > 0 ? '<1' : Math.round(pct)}%</strong>
        <span className="usage-detail">{formatTokens(context.usedTokens)} / {formatTokens(context.budgetTokens)} · {context.messages} msg</span>
        <div className="usage-bar" role="progressbar" aria-valuenow={Math.round(pct)} aria-valuemin={0} aria-valuemax={100}>
          <span className="usage-bar-fill" style={{ width: `${Math.min(100, pct)}%` }} />
          <span className="usage-bar-mark" style={{ left: `${Math.min(100, triggerPct)}%` }} />
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
  return Number.isNaN(date.getTime()) ? '' : new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric' }).format(date);
}
