/** 0004_run_settle_claim: the same SQL, byte for byte, as the Go runtime's
 *  admin/migrate/sql/sqlite/0004_run_settle_claim.sql, so the two record the same checksum
 *  for it. Never edit a released migration. Add the next number instead. */
export const sql = `-- A run's settle hook is claimed before it runs (§5.6): one conditional write
-- sets settlingAt and a token, and only the writer that won calls the hook.
-- The token lets the winner, and only the winner, mark the run settled or
-- drop the claim. A claim left behind by a settler that died is taken over
-- once it is old enough. The partial index is what the late-settle sweep
-- reads: ended runs that never settled.

ALTER TABLE agentic_runs ADD COLUMN settlingAt INTEGER;
ALTER TABLE agentic_runs ADD COLUMN settleToken TEXT;
CREATE INDEX IF NOT EXISTS agentic_runs_unsettled ON agentic_runs (endedAt) WHERE settledAt IS NULL;
`;
