/** The admin schema as it stood before the migrator existed (§2.9).
 *
 *  Written so that running it against a database that already holds these
 *  tables changes nothing: an existing install picks the migrator up without a
 *  separate baseline step, and one that stopped on an older release gets the
 *  columns it missed, through the ADD COLUMN IF NOT EXISTS statements below.
 *
 *  Never edit a released migration. Add the next number instead. */
export const sql = `
CREATE TABLE IF NOT EXISTS agentic_threads (
  id TEXT PRIMARY KEY, state TEXT NOT NULL, model TEXT NOT NULL,
  "firstSeenAt" TIMESTAMPTZ NOT NULL DEFAULT now(),
  "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT now(), "startedWith" JSONB
);

ALTER TABLE agentic_threads ADD COLUMN IF NOT EXISTS "startedWith" JSONB;
CREATE INDEX IF NOT EXISTS agentic_threads_state ON agentic_threads(state, "updatedAt");

CREATE TABLE IF NOT EXISTS agentic_runs (
  id TEXT PRIMARY KEY, "threadId" TEXT NOT NULL, "parentRunId" TEXT,
  depth INT NOT NULL DEFAULT 0, agent TEXT NOT NULL, model TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'RUNNING', "stopReason" TEXT, error TEXT,
  "startedAt" TIMESTAMPTZ NOT NULL DEFAULT now(), "endedAt" TIMESTAMPTZ,
  "durationMs" INT, "queuedMs" INT,
  attempts INT NOT NULL DEFAULT 0, steps INT NOT NULL DEFAULT 0,
  "inputTokens" INT NOT NULL DEFAULT 0, "cachedInputTokens" INT NOT NULL DEFAULT 0,
  "outputTokens" INT NOT NULL DEFAULT 0, "totalTokens" INT NOT NULL DEFAULT 0,
  result JSONB, prompt TEXT, "tokenBudget" INT, "runState" JSONB, "providerOptions" JSONB
);

ALTER TABLE agentic_runs ADD COLUMN IF NOT EXISTS "providerOptions" JSONB;
ALTER TABLE agentic_runs ADD COLUMN IF NOT EXISTS prompt TEXT;
ALTER TABLE agentic_runs ADD COLUMN IF NOT EXISTS "tokenBudget" INT;
ALTER TABLE agentic_runs ADD COLUMN IF NOT EXISTS "runState" JSONB;

CREATE INDEX IF NOT EXISTS agentic_runs_thread ON agentic_runs("threadId", "startedAt" DESC);
CREATE INDEX IF NOT EXISTS agentic_runs_state ON agentic_runs(state, "startedAt" DESC);
CREATE INDEX IF NOT EXISTS agentic_runs_parent ON agentic_runs("parentRunId");

CREATE TABLE IF NOT EXISTS agentic_steps (
  "runId" TEXT NOT NULL, "threadId" TEXT, "agentId" TEXT, "index" INT NOT NULL,
  "durationMs" INT NOT NULL, "finishReason" TEXT NOT NULL,
  "inputTokens" INT NOT NULL DEFAULT 0, "cachedInputTokens" INT NOT NULL DEFAULT 0,
  "outputTokens" INT NOT NULL DEFAULT 0, "totalTokens" INT NOT NULL DEFAULT 0,
  tools JSONB, text TEXT, "toolCalls" JSONB,
  at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE agentic_steps ADD COLUMN IF NOT EXISTS text TEXT;
ALTER TABLE agentic_steps ADD COLUMN IF NOT EXISTS "threadId" TEXT;
ALTER TABLE agentic_steps ADD COLUMN IF NOT EXISTS "toolCalls" JSONB;

CREATE INDEX IF NOT EXISTS agentic_steps_run ON agentic_steps("runId", "index");
CREATE INDEX IF NOT EXISTS agentic_steps_thread ON agentic_steps("threadId", at);
`;
