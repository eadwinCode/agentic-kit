import { useCallback, useEffect, useState } from 'react';
import { formatCost, useAgentThread, type EntryPart, type PendingInput } from 'use-agentenkit';

/** What the server says about itself at /api/info: the one thing that
 *  differs between the four tool examples. */
interface Info {
  title: string;
  subtitle: string;
  /** The built-in tools this server gave its agent. */
  tools: string[];
  /** Prompts to try, shown on an empty thread. */
  prompts: string[];
  /** Setup notes: a missing key, the sandbox in use. */
  notes: string[];
  runtime: 'ts' | 'go';
}

const stateLabel: Record<string, string> = {
  IDLE: 'idle', QUEUED: 'queued', RUNNING: 'running', WAITING_FOR_INPUT: 'waiting for you',
  CANCELLED: 'stopped', COMPLETED: 'done', FAILED: 'failed',
};

type ToolCall = Extract<EntryPart, { type: 'tool-call' }>;

export function App() {
  const [info, setInfo] = useState<Info | null>(null);
  // What a sandbox command printed, live, by tool call: bash and
  // code_execution send it as tool.output events while they run. Live only,
  // so a reload shows the result, not this.
  const [liveOutput, setLiveOutput] = useState<Record<string, string>>({});
  const onCustom = useCallback((name: string, value: unknown) => {
    if (name !== 'tool.output') return;
    const { toolCallId, text } = value as { toolCallId?: string; text?: string };
    if (!toolCallId || !text) return;
    setLiveOutput((prev) => ({ ...prev, [toolCallId]: ((prev[toolCallId] ?? '') + text).slice(-4_000) }));
  }, []);
  const {
    threadId, entries, agentState, activity, historyLoading, pendingInputs, threads, usage, error,
    selectThread, deleteThread, newThread, run, stop, respondToInput,
  } = useAgentThread({ onCustom });
  const [prompt, setPrompt] = useState('');

  useEffect(() => {
    void fetch('/api/info').then((r) => r.json()).then(setInfo).catch(() => undefined);
  }, []);
  useEffect(() => {
    if (info) document.title = info.title;
  }, [info]);

  const running = agentState === 'QUEUED' || agentState === 'RUNNING' || agentState === 'WAITING_FOR_INPUT';
  const send = (text: string) => {
    if (!text.trim() || running) return;
    void run(text.trim());
    setPrompt('');
  };
  const cost = formatCost(usage?.tokens);

  return (
    <div className="layout">
      <aside className="sidebar">
        <button type="button" className="new" onClick={() => newThread()}>+ New thread</button>
        <ul>
          {threads.map((t) => (
            <li key={t.id} className={t.id === threadId ? 'active' : ''}>
              <button type="button" onClick={() => selectThread(t.id)}>
                <span className="title">{t.title || 'New thread'}</span>
                <span className="meta">{stateLabel[t.state] ?? t.state.toLowerCase()}</span>
              </button>
              <button type="button" className="delete" aria-label="Delete thread" onClick={() => void deleteThread(t.id)}>×</button>
            </li>
          ))}
        </ul>
      </aside>

      <main>
        <header>
          <div>
            <h1>{info?.title ?? 'agentenkit tools'}</h1>
            {info && <p className="subtitle">{info.subtitle}</p>}
          </div>
          <div className="header-meta">
            {info && <span className="runtime">{info.runtime === 'go' ? 'Go' : 'TypeScript'}</span>}
            {cost && <span className="cost" title="This thread's spend">{cost}</span>}
          </div>
        </header>
        {info && (
          <div className="tools">
            {info.tools.map((t) => <code key={t}>{t}</code>)}
            {info.notes.map((n) => <span key={n} className="note">{n}</span>)}
          </div>
        )}

        <section className="thread">
          {entries.length === 0 && !historyLoading && (
            <div className="empty">
              <p>Try one of these:</p>
              {(info?.prompts ?? []).map((p) => (
                <button key={p} type="button" className="suggestion" onClick={() => send(p)} disabled={running}>{p}</button>
              ))}
            </div>
          )}
          {entries.map((e) =>
            e.kind === 'tool' ? (
              <div key={e.id} className="tool-entry">
                {e.parts.filter((p): p is ToolCall => p.type === 'tool-call').map((call) => (
                  <ToolCallView key={call.toolCallId} call={call} live={liveOutput[call.toolCallId]} />
                ))}
              </div>
            ) : e.kind === 'reasoning' ? (
              <details key={e.id} className="thought"><summary>Thinking</summary><p>{e.text}</p></details>
            ) : (
              <div key={e.id} className={`message ${e.role}`}>
                <span className="role">{e.role === 'user' ? 'You' : 'Agent'}</span>
                <div className="bubble">{e.text}</div>
              </div>
            ),
          )}
          {pendingInputs.map((req) => <Approval key={req.toolCallId} req={req} onRespond={respondToInput} />)}
          {running && agentState !== 'WAITING_FOR_INPUT' && <p className="activity">{activity.label}…</p>}
          {error && <p className="error">✕ {error}</p>}
        </section>

        <form className="composer" onSubmit={(ev) => { ev.preventDefault(); send(prompt); }}>
          <textarea
            value={prompt}
            placeholder="Ask the agent…"
            rows={2}
            onChange={(ev) => setPrompt(ev.target.value)}
            onKeyDown={(ev) => {
              if (ev.key === 'Enter' && !ev.shiftKey) {
                ev.preventDefault();
                send(prompt);
              }
            }}
          />
          {running ? (
            <button type="button" className="stop" onClick={() => void stop()}>Stop</button>
          ) : (
            <button type="submit" disabled={!prompt.trim()}>Send</button>
          )}
        </form>
      </main>
    </div>
  );
}

/** One tool call: its name, what it was sent, what came back, and, while a
 *  sandbox command runs, its output as it comes. */
function ToolCallView({ call, live }: { call: ToolCall; live?: string }) {
  const state = call.state === 'done' ? '✓' : call.state === 'error' ? '✕' : '…';
  return (
    <details className={`tool-call ${call.state}`} open={call.state === 'running'}>
      <summary>
        <span className="tool-state">{state}</span> <code>{call.toolName}</code>
        <span className="tool-args">{summarize(call.args)}</span>
      </summary>
      {live && call.state === 'running' && <pre className="live">{live}</pre>}
      <h4>Input</h4>
      <pre>{JSON.stringify(call.args, null, 2)}</pre>
      {call.result !== undefined && (
        <>
          <h4>Result</h4>
          <pre>{truncate(JSON.stringify(call.result, null, 2), 6_000)}</pre>
        </>
      )}
    </details>
  );
}

function Approval({ req, onRespond }: {
  req: PendingInput;
  onRespond: (toolCallId: string, approved: boolean) => Promise<boolean>;
}) {
  return (
    <div className="approval">
      <p>
        ⏸ <code>{req.toolName}</code> waits for your approval
        {req.agentName ? <> (asked by <strong>{req.agentName}</strong>)</> : null}
      </p>
      <pre>{truncate(JSON.stringify(req.arguments, null, 2), 2_000)}</pre>
      <div className="row">
        <button type="button" className="approve" onClick={() => void onRespond(req.toolCallId, true)}>Approve</button>
        <button type="button" className="deny" onClick={() => void onRespond(req.toolCallId, false)}>Deny</button>
      </div>
    </div>
  );
}

/** The one input a reader wants to see in the collapsed line. */
function summarize(args: unknown): string {
  const a = (args ?? {}) as Record<string, unknown>;
  // text_editor: "view notes.txt"
  if (typeof a.path === 'string' && typeof a.command === 'string') return `${a.command} ${a.path}`;
  const text = a.query ?? a.command ?? a.url ?? a.id ?? a.path ?? a.code;
  return typeof text === 'string' ? truncate(text.split('\n')[0]!, 80) : '';
}

function truncate(s: string, n: number): string {
  return s.length > n ? `${s.slice(0, n)}…` : s;
}
