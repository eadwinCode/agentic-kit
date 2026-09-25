-- The usage row is one priced model call (agentenkit §4): who made it, which
-- model, how it ended, and what it cost. schema.prisma has carried these
-- columns for a while; this migration brings a fresh database in line, so
-- `prisma migrate deploy` gives the usage table the adapter writes to.

-- AlterTable
ALTER TABLE "TokenUsage" ADD COLUMN     "agentName" TEXT,
ADD COLUMN     "cacheWriteInputTokens" INTEGER NOT NULL DEFAULT 0,
ADD COLUMN     "costCurrency" TEXT,
ADD COLUMN     "costMicros" BIGINT,
ADD COLUMN     "costSource" TEXT,
ADD COLUMN     "estimated" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "kind" TEXT NOT NULL DEFAULT 'step',
ADD COLUMN     "model" TEXT,
ADD COLUMN     "modelId" TEXT,
ADD COLUMN     "outcome" TEXT NOT NULL DEFAULT 'finished',
ADD COLUMN     "providerMetadata" JSONB,
ADD COLUMN     "reasoningTokens" INTEGER NOT NULL DEFAULT 0,
ADD COLUMN     "runId" TEXT,
ADD COLUMN     "step" INTEGER NOT NULL DEFAULT 0;

-- CreateIndex
CREATE INDEX "TokenUsage_runId_createdAt_idx" ON "TokenUsage"("runId", "createdAt");

