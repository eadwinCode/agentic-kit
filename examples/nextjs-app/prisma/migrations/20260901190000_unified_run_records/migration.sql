/*
  Warnings:

  - The `SubagentRun` table is dropped. Its rows are development data only.
    A subagent IS a run (§2.7, §2.9) — same loop, same steps, same duration,
    same tokens — so it moves into `AgentRun`, distinguished by `depth > 0`
    and a `parentRunId` rather than by living in a separate table.

*/
-- DropForeignKey
ALTER TABLE "SubagentRun" DROP CONSTRAINT "SubagentRun_threadId_fkey";

-- DropTable
DROP TABLE "SubagentRun";

-- CreateTable
CREATE TABLE "AgentRun" (
    "id" TEXT NOT NULL,
    "threadId" TEXT NOT NULL,
    "parentRunId" TEXT,
    "depth" INTEGER NOT NULL DEFAULT 0,
    "agent" TEXT NOT NULL,
    "model" TEXT NOT NULL,
    "state" "ExecutionState" NOT NULL DEFAULT 'RUNNING',
    "stopReason" TEXT,
    "error" TEXT,
    "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "endedAt" TIMESTAMP(3),
    "durationMs" INTEGER,
    "queuedMs" INTEGER,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "steps" INTEGER NOT NULL DEFAULT 0,
    "inputTokens" INTEGER NOT NULL DEFAULT 0,
    "cachedInputTokens" INTEGER NOT NULL DEFAULT 0,
    "outputTokens" INTEGER NOT NULL DEFAULT 0,
    "totalTokens" INTEGER NOT NULL DEFAULT 0,
    "result" JSONB,

    CONSTRAINT "AgentRun_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "AgentRun_threadId_startedAt_idx" ON "AgentRun"("threadId", "startedAt");

-- CreateIndex
CREATE INDEX "AgentRun_state_startedAt_idx" ON "AgentRun"("state", "startedAt");

-- CreateIndex
CREATE INDEX "AgentRun_parentRunId_idx" ON "AgentRun"("parentRunId");

-- AddForeignKey
ALTER TABLE "AgentRun" ADD CONSTRAINT "AgentRun_threadId_fkey" FOREIGN KEY ("threadId") REFERENCES "Thread"("id") ON DELETE CASCADE ON UPDATE CASCADE;
