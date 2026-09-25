/** 0003_run_enqueued_at: the same SQL, byte for byte, as the Go runtime's
 *  admin/migrate/sql/sqlite/0003_run_enqueued_at.sql, so the two record the same checksum
 *  for it. Never edit a released migration. Add the next number instead. */
export const sql = `-- A dispatched run opens QUEUED and becomes RUNNING when a worker picks it
-- up (§2.8). The enqueue time is kept on the record so a listing can tell a
-- run that is waiting from one that is working, and so the queue-wait
-- percentiles include the runs still waiting, not only the ones that got
-- through. The money and step caps are kept too, so a run re-dispatched
-- from its record keeps the caps it was admitted with.

ALTER TABLE agentic_runs ADD COLUMN enqueuedAt INTEGER;
ALTER TABLE agentic_runs ADD COLUMN costBudgetMicros INTEGER;
ALTER TABLE agentic_runs ADD COLUMN maxSteps INTEGER;
`;
