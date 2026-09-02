'use client';

import { useCallback, useEffect, useState } from 'react';

type State = 'IDLE' | 'RUNNING' | 'WAITING_FOR_INPUT' | 'CANCELLED' | 'COMPLETED' | 'FAILED';
interface Tokens {
  inputTokens: number; cachedInputTokens: number; outputTokens: number; totalTokens: number;
}

/** What started a thread (§2.9): the first dispatched run's parameters,
 *  recorded once. The payload fields follow `recordPayloads`. */
interface ThreadStart {
  runId: string; agent: string; model: string; at: string;
  prompt?: string | null; tokenBudget?: number | null;
  state?: Record<string, unknown> | null;
  providerOptions?: Record<string, unknown> | null;
}

interface ThreadSummary extends Tokens {
  id: string; state: State; model: string;
  firstSeenAt: string; updatedAt: string;
  runs: number; steps: number; durationMs: number;
  tokens: Tokens; prompt: string | null;
  startedWith?: ThreadStart | null;
}

interface RunRecord extends Tokens {
  id: string; threadId: string; parentRunId: string | null; depth: number;
  agent: string; model: string; state: State;
  stopReason: string | null; error: string | null;
  startedAt: string; endedAt: string | null;
  durationMs: number | null; queuedMs: number | null;
  steps: number; prompt: string | null; tokenBudget: number | null;
  runState: Record<string, unknown> | null;
  providerOptions?: Record<string, unknown> | null;
}

interface StepRecord extends Tokens {
  runId: string; threadId: string; agentId: string | null; index: number;
  durationMs: number; finishReason: string; tools: string[];
  text: string | null;
  toolCalls?: Array<{ toolName: string; args: unknown; result: unknown }>;
  at: string;
}

interface ThreadDetail { thread: ThreadSummary; runs: RunRecord[]; steps: StepRecord[] }
interface Percentiles { p50: number; p95: number; max: number }
interface Overview {
  runs: {
    total: number; failed: number; tokens: Tokens;
    duration: Percentiles | null; queued: Percentiles | null;
  };
  threads: Partial<Record<State, number>>;
  active: RunRecord[];
}

const ms = (n: number | null | undefined) =>
  n == null ? '—' : n < 1000 ? `${n}ms` : `${(n / 1000).toFixed(n < 10_000 ? 1 : 0)}s`;
const num = (n: number) =>
  n >= 10_000
    ? new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(n)
    : n.toLocaleString();
const ago = (iso: string) => {
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return `${Math.floor(s)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
};
/** The token split, everywhere tokens appear — a total alone hides where the
 *  spend actually is, and prompt tokens usually dominate. */
const split = (t: Tokens) =>
  `${num(t.inputTokens)} in${t.cachedInputTokens ? ` · ${num(t.cachedInputTokens)} cached` : ''} · ${num(t.outputTokens)} out`;

const FILTERS: Array<{ label: string; state?: State[] }> = [
  { label: 'All' },
  { label: 'In flight', state: ['RUNNING', 'WAITING_FOR_INPUT'] },
  { label: 'Completed', state: ['COMPLETED'] },
  { label: 'Failed', state: ['FAILED'] },
  { label: 'Stopped', state: ['CANCELLED'] },
];

export default function AdminPage() {
  const [overview, setOverview] = useState<Overview | null>(null);
  const [threads, setThreads] = useState<ThreadSummary[]>([]);
  const [thread, setThread] = useState<ThreadDetail | null>(null);
  const [step, setStep] = useState<StepRecord | null>(null);
  const [filter, setFilter] = useState(0);
  const [hours, setHours] = useState(24);
  const [live, setLive] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const params = new URLSearchParams({ hours: String(hours) });
      for (const s of FILTERS[filter]!.state ?? []) params.append('state', s);
      const [o, t] = await Promise.all([
        fetch(`/api/admin/overview?hours=${hours}`).then((x) => x.json()),
        fetch(`/api/admin/threads?${params}`).then((x) => x.json()),
      ]);
      setOverview(o);
      setThreads(t.threads ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [filter, hours]);

  useEffect(() => { void load(); }, [load]);

  // Polling, not SSE: this view spans many threads and the event stream is
  // per-thread (§2.2). Paused while a thread is open so it cannot shift
  // underneath a step you are reading.
  useEffect(() => {
    if (!live || thread) return;
    const id = setInterval(() => void load(), 3_000);
    return () => clearInterval(id);
  }, [live, thread, load]);

  const openThread = async (id: string) => {
    const res = await fetch(`/api/admin/threads/${id}`);
    setStep(null);
    setThread(res.ok ? await res.json() : null);
  };

  const s = overview?.runs;

  return (
    <main className="admin">
      <header>
        <h1>
          {thread ? (
            <button type="button" className="crumb" onClick={() => { setThread(null); setStep(null); }}>
              Threads
            </button>
          ) : 'Threads'}
          {thread && <span className="crumb-sep">/ {thread.thread.id.slice(0, 8)}</span>}
        </h1>
        <div className="admin-controls">
          {!thread && (
            <select value={hours} onChange={(e) => setHours(Number(e.target.value))}>
              <option value={1}>last hour</option>
              <option value={24}>last 24h</option>
              <option value={168}>last 7d</option>
            </select>
          )}
          <button type="button" className={live ? 'live on' : 'live'} onClick={() => setLive((v) => !v)}>
            <span className="live-dot" aria-hidden="true" /> {live ? 'Live' : 'Paused'}
          </button>
          <a href="/">← chat</a>
        </div>
      </header>

      {error && <p className="admin-error">✕ {error}</p>}

      {!thread && (
        <>
          <section className="tiles">
            <Tile label="In flight" value={num(overview?.active.length ?? 0)} accent={(overview?.active.length ?? 0) > 0} />
            <Tile label="Runs" value={num(s?.total ?? 0)} detail={`${num(s?.failed ?? 0)} failed`} bad={(s?.failed ?? 0) > 0} />
            <Tile label="Duration p50 / p95" value={ms(s?.duration?.p50)} detail={`p95 ${ms(s?.duration?.p95)}`} />
            <Tile label="Queue wait p95" value={ms(s?.queued?.p95)} detail="enqueue → pickup" />
            <Tile
              label="Tokens"
              value={num(s?.tokens.totalTokens ?? 0)}
              detail={s ? split(s.tokens) : undefined}
            />
          </section>

          <nav className="admin-filters">
            {FILTERS.map((f, i) => (
              <button key={f.label} type="button" className={i === filter ? 'active' : ''} onClick={() => setFilter(i)}>
                {f.label}
              </button>
            ))}
          </nav>

          <table className="runs">
            <thead>
              <tr>
                <th>Thread</th><th>State</th><th>Runs</th><th>Steps</th>
                <th>Tokens</th><th>Duration</th><th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {threads.length === 0 && (
                <tr><td colSpan={7} className="empty">No threads in this window.</td></tr>
              )}
              {threads.map((t) => (
                <tr key={t.id} onClick={() => void openThread(t.id)}>
                  <td className="thread-cell">
                    <code>{t.id.slice(0, 8)}</code>
                    {t.startedWith && (
                      <span className="thread-agent">
                        {t.startedWith.agent} · {t.startedWith.model}
                        {t.startedWith.tokenBudget != null && ` · budget ${num(t.startedWith.tokenBudget)}`}
                      </span>
                    )}
                    {t.prompt && <span className="thread-prompt">{t.prompt}</span>}
                  </td>
                  <td><span className={`badge ${t.state.toLowerCase()}`}>{t.state.toLowerCase()}</span></td>
                  <td>{t.runs}</td>
                  <td>{t.steps}</td>
                  <td>
                    {num(t.tokens.totalTokens)}
                    <span className="sub">{split(t.tokens)}</span>
                  </td>
                  <td>{ms(t.durationMs)}</td>
                  <td title={new Date(t.updatedAt).toLocaleString()}>{ago(t.updatedAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {thread && <ThreadView detail={thread} onStep={setStep} selected={step} />}
      {step && <StepPanel step={step} runs={thread?.runs ?? []} onClose={() => setStep(null)} />}
    </main>
  );
}

function Tile({ label, value, detail, accent, bad }: {
  label: string; value: string; detail?: string; accent?: boolean; bad?: boolean;
}) {
  return (
    <div className={`tile ${accent ? 'accent' : ''} ${bad ? 'bad' : ''}`}>
      <span className="tile-label">{label}</span>
      <strong>{value}</strong>
      {detail && <span className="tile-detail">{detail}</span>}
    </div>
  );
}

/** A thread is many runs, and each run is many steps. Steps are grouped under
 *  the run that produced them so a delegated run reads as its own block rather
 *  than interleaving with its parent's. */
function ThreadView({ detail, onStep, selected }: {
  detail: ThreadDetail;
  onStep: (s: StepRecord) => void;
  selected: StepRecord | null;
}) {
  const { thread, runs, steps } = detail;
  const slowest = Math.max(1, ...steps.map((s) => s.durationMs));

  return (
    <>
      <section className="tiles">
        <Tile label="Runs" value={num(thread.runs)} detail={`${num(thread.steps)} steps`} />
        <Tile label="Tokens" value={num(thread.tokens.totalTokens)} detail={split(thread.tokens)} />
        <Tile label="Time in runs" value={ms(thread.durationMs)} />
        <Tile label="Model" value={thread.model} />
      </section>

      {(thread.startedWith || thread.prompt) && (
        <StartedWith start={thread.startedWith ?? null} prompt={thread.prompt} />
      )}

      {runs.map((run) => {
        const own = steps.filter((s) => s.runId === run.id);
        return (
          <section key={run.id} className="run-block">
            <header>
              {run.depth > 0 && <span className="depth">↳{run.depth}</span>}
              <strong>{run.agent}</strong>
              <span className={`badge ${run.state.toLowerCase()}`}>{run.state.toLowerCase()}</span>
              <span className="tile-detail">
                {ms(run.durationMs)} · {num(run.totalTokens)} tok ({split(run)})
              </span>
            </header>
            {run.error && <p className="admin-error">✕ {run.error}</p>}
            {own.length === 0 ? (
              <p className="tile-detail">No steps recorded.</p>
            ) : (
              <ol className="steps">
                {own.map((s) => (
                  <li key={`${s.runId}-${s.index}`}>
                    <button
                      type="button"
                      className={`step-row ${selected === s ? 'selected' : ''}`}
                      onClick={() => onStep(s)}
                    >
                      <span className="step-index">{s.index}</span>
                      <span className="step-bar">
                        <span style={{ width: `${(s.durationMs / slowest) * 100}%` }} />
                      </span>
                      <span className="step-meta">
                        {ms(s.durationMs)} · {s.finishReason} · {num(s.totalTokens)} tok
                        {' ('}{split(s)}{')'}
                        {s.tools.length > 0 && ` · ${s.tools.join(', ')}`}
                      </span>
                    </button>
                  </li>
                ))}
              </ol>
            )}
          </section>
        );
      })}
    </>
  );
}

/** The parameters that started the thread (§2.9): who asked, on which
 *  model, with what budget, state and provider options, and the prompt. A
 *  thread recorded before this existed only has its prompt. */
function StartedWith({ start, prompt }: { start: ThreadStart | null; prompt: string | null }) {
  const text = start?.prompt ?? prompt;
  return (
    <section className="started">
      <h3>Started with</h3>
      {start && (
        <dl className="started-params">
          <dt>Agent</dt><dd>{start.agent}</dd>
          <dt>Model</dt><dd>{start.model}</dd>
          <dt>Run</dt><dd><code>{start.runId.slice(0, 8)}</code></dd>
          <dt>At</dt><dd title={new Date(start.at).toLocaleString()}>{ago(start.at)}</dd>
          <dt>Token budget</dt><dd>{start.tokenBudget != null ? num(start.tokenBudget) : 'none'}</dd>
          {start.state && Object.keys(start.state).length > 0 && (
            <><dt>Run state</dt><dd><code>{JSON.stringify(start.state)}</code></dd></>
          )}
          {start.providerOptions && Object.keys(start.providerOptions).length > 0 && (
            <><dt>Provider options</dt><dd><code>{JSON.stringify(start.providerOptions)}</code></dd></>
          )}
          {!start.prompt && start.state === undefined && (
            <><dt>Payloads</dt><dd className="tile-detail">not recorded (recordPayloads is off)</dd></>
          )}
        </dl>
      )}
      {text && <pre className="payload wide">{text}</pre>}
    </section>
  );
}

/** One step, in full: what it cost, what it said, and what its tools did. */
function StepPanel({ step, runs, onClose }: {
  step: StepRecord; runs: RunRecord[]; onClose: () => void;
}) {
  const run = runs.find((r) => r.id === step.runId);
  return (
    <aside className="run-panel">
      <header>
        <div>
          <strong>Step {step.index}</strong>
          {run && <span className="tile-detail">{run.agent}</span>}
          <span className="badge">{step.finishReason}</span>
        </div>
        <button type="button" onClick={onClose} aria-label="Close">×</button>
      </header>

      <dl className="run-facts">
        <div><dt>Duration</dt><dd>{ms(step.durationMs)}</dd></div>
        <div><dt>Total tokens</dt><dd>{num(step.totalTokens)}</dd></div>
        <div><dt>Input</dt><dd>{num(step.inputTokens)}</dd></div>
        <div><dt>Cached input</dt><dd>{num(step.cachedInputTokens)}</dd></div>
        <div><dt>Output</dt><dd>{num(step.outputTokens)}</dd></div>
        <div><dt>At</dt><dd>{new Date(step.at).toLocaleTimeString()}</dd></div>
      </dl>

      {step.text && (
        <>
          <h3>Said</h3>
          <pre className="payload wide">{step.text}</pre>
        </>
      )}

      {(step.toolCalls ?? []).length > 0 && (
        <>
          <h3>Tools</h3>
          {(step.toolCalls ?? []).map((t, i) => (
            <details key={i} className="tool-call" open={i === 0}>
              <summary>⚙ {t.toolName}</summary>
              <pre className="payload wide">
                {JSON.stringify({ args: t.args, result: t.result }, null, 2)}
              </pre>
            </details>
          ))}
        </>
      )}

      {!step.text && (step.toolCalls ?? []).length === 0 && (
        <p className="tile-detail">
          Nothing recorded for this step — `recordPayloads` is off, so only
          timings and counts were kept.
        </p>
      )}
    </aside>
  );
}
