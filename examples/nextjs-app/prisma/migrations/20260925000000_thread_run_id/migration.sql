-- The thread's current run, for compare-and-set state changes (agentenkit 0.4.0).
ALTER TABLE "Thread" ADD COLUMN "runId" TEXT;
