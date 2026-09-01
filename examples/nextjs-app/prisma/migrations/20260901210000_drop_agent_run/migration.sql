/*
  Warnings:

  - The `AgentRun` table is dropped. Run records are no longer the caller's
    data: agentic-kit keeps its own operational history (§2.9) in its own
    store — SQLite in development, Postgres via AGENTIC_KIT_ADMIN_DATABASE_URL
    in production — so a dashboard never reads this database at all.

*/
-- DropForeignKey
ALTER TABLE "AgentRun" DROP CONSTRAINT "AgentRun_threadId_fkey";

-- DropTable
DROP TABLE "AgentRun";
