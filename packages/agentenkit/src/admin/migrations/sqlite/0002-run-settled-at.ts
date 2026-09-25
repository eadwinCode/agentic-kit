/** 0002_run_settled_at: the same SQL, byte for byte, as the Go runtime's
 *  admin/migrate/sql/sqlite/0002_run_settled_at.sql, so the two record the same checksum
 *  for it. Never edit a released migration. Add the next number instead. */
export const sql = `-- A run settles exactly once (§5.6): the spec's OnSettle runs either in the
-- worker that ends the run or in a stop that ends it while no worker holds
-- it. The time it ran is recorded on the run, so whichever side comes
-- second can see the other already did it.

ALTER TABLE agentic_runs ADD COLUMN settledAt INTEGER;
`;
