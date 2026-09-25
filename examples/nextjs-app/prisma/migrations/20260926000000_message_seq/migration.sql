-- Messages are ordered by an explicit seq, not createdAt (agentenkit 0.5.x):
-- a run appends a tool call and its result inside one millisecond, and a
-- clock tie could swap them. Existing rows are numbered in the order they
-- were read before: createdAt, then id.
CREATE SEQUENCE "Message_seq_seq";
ALTER TABLE "Message" ADD COLUMN "seq" BIGINT;
UPDATE "Message" m SET "seq" = s.rn
  FROM (SELECT id, row_number() OVER (ORDER BY "createdAt", id) AS rn FROM "Message") s
  WHERE m.id = s.id;
SELECT setval('"Message_seq_seq"', COALESCE((SELECT MAX("seq") FROM "Message"), 0) + 1, false);
ALTER TABLE "Message" ALTER COLUMN "seq" SET DEFAULT nextval('"Message_seq_seq"');
ALTER TABLE "Message" ALTER COLUMN "seq" SET NOT NULL;
ALTER SEQUENCE "Message_seq_seq" OWNED BY "Message"."seq";
CREATE INDEX "Message_threadId_seq_idx" ON "Message"("threadId", "seq");
