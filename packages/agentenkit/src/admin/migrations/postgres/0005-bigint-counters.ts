/** 0005_bigint_counters: the same SQL, byte for byte, as the Go runtime's
 *  admin/migrate/sql/postgres/0005_bigint_counters.sql, so the two record the same checksum
 *  for it. Never edit a released migration. Add the next number instead. */
export const sql = `-- Durations and token counts outgrow INT. A run parked on an approval for
-- more than 24.8 days has a durationMs past 2^31, and a busy thread's token
-- counters get there too. BIGINT holds both. SQLite needs nothing: its
-- integers are already 64-bit.

ALTER TABLE agentic_runs ALTER COLUMN "durationMs" TYPE BIGINT, ALTER COLUMN "queuedMs" TYPE BIGINT, ALTER COLUMN "inputTokens" TYPE BIGINT, ALTER COLUMN "cachedInputTokens" TYPE BIGINT, ALTER COLUMN "outputTokens" TYPE BIGINT, ALTER COLUMN "totalTokens" TYPE BIGINT;
ALTER TABLE agentic_steps ALTER COLUMN "durationMs" TYPE BIGINT, ALTER COLUMN "inputTokens" TYPE BIGINT, ALTER COLUMN "cachedInputTokens" TYPE BIGINT, ALTER COLUMN "outputTokens" TYPE BIGINT, ALTER COLUMN "totalTokens" TYPE BIGINT;
`;
