-- A run is QUEUED between enqueue and pickup (agentenkit 0.3.0).
ALTER TYPE "ExecutionState" ADD VALUE 'QUEUED' BEFORE 'RUNNING';
