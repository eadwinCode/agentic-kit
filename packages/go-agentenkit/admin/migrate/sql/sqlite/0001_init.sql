-- The admin schema as it stood before the migrator existed (§2.9).
--
-- Written so that running it against a database that already holds these
-- tables changes nothing: an existing install picks the migrator up without a
-- separate baseline step, and one that stopped on an older release gets the
-- columns it missed. SQLite has no ADD COLUMN IF NOT EXISTS, so the columns
-- added after the first release are handled by the store's own PRAGMA check
-- rather than here.
--
-- Never edit a released migration. Add the next number instead.

CREATE TABLE IF NOT EXISTS agentic_runs (
  id TEXT PRIMARY KEY, threadId TEXT NOT NULL, parentRunId TEXT,
  depth INTEGER NOT NULL DEFAULT 0, agent TEXT NOT NULL, model TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'RUNNING', stopReason TEXT, error TEXT,
  startedAt INTEGER NOT NULL, endedAt INTEGER, durationMs INTEGER, queuedMs INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0, steps INTEGER NOT NULL DEFAULT 0,
  inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
  outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
  result TEXT, prompt TEXT, tokenBudget INTEGER, runState TEXT, providerOptions TEXT
);

CREATE INDEX IF NOT EXISTS agentic_runs_thread ON agentic_runs(threadId, startedAt);
CREATE INDEX IF NOT EXISTS agentic_runs_state ON agentic_runs(state, startedAt);
CREATE INDEX IF NOT EXISTS agentic_runs_parent ON agentic_runs(parentRunId);

CREATE TABLE IF NOT EXISTS agentic_threads (
  id TEXT PRIMARY KEY, state TEXT NOT NULL, model TEXT NOT NULL,
  firstSeenAt INTEGER NOT NULL, updatedAt INTEGER NOT NULL, startedWith TEXT
);

CREATE INDEX IF NOT EXISTS agentic_threads_state ON agentic_threads(state, updatedAt);

CREATE TABLE IF NOT EXISTS agentic_steps (
  runId TEXT NOT NULL, threadId TEXT, agentId TEXT, "index" INTEGER NOT NULL,
  durationMs INTEGER NOT NULL, finishReason TEXT NOT NULL,
  inputTokens INTEGER NOT NULL DEFAULT 0, cachedInputTokens INTEGER NOT NULL DEFAULT 0,
  outputTokens INTEGER NOT NULL DEFAULT 0, totalTokens INTEGER NOT NULL DEFAULT 0,
  tools TEXT, text TEXT, toolCalls TEXT, at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS agentic_steps_run ON agentic_steps(runId, "index");
CREATE INDEX IF NOT EXISTS agentic_steps_thread ON agentic_steps(threadId, at);
