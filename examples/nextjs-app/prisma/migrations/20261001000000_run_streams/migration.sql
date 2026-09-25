-- CreateTable
CREATE TABLE "RunStream" (
    "id" TEXT NOT NULL,
    "threadId" TEXT NOT NULL,
    "runId" TEXT NOT NULL,
    "closed" BOOLEAN NOT NULL DEFAULT false,
    "endEvent" TEXT,
    "expiresAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "RunStream_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "RunStreamEvent" (
    "pos" BIGSERIAL NOT NULL,
    "streamId" TEXT NOT NULL,
    "event" TEXT NOT NULL,

    CONSTRAINT "RunStreamEvent_pkey" PRIMARY KEY ("pos")
);

-- CreateIndex
CREATE INDEX "RunStream_expiresAt_idx" ON "RunStream"("expiresAt");

-- CreateIndex
CREATE INDEX "RunStream_threadId_idx" ON "RunStream"("threadId");

-- CreateIndex
CREATE INDEX "RunStreamEvent_streamId_pos_idx" ON "RunStreamEvent"("streamId", "pos");

