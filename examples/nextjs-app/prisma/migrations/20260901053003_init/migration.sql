/*
  Warnings:

  - You are about to drop the column `completionTokens` on the `TokenUsage` table. All the data in the column will be lost.
  - You are about to drop the column `costUSD` on the `TokenUsage` table. All the data in the column will be lost.
  - You are about to drop the column `model` on the `TokenUsage` table. All the data in the column will be lost.
  - You are about to drop the column `promptTokens` on the `TokenUsage` table. All the data in the column will be lost.
  - Added the required column `totalTokens` to the `TokenUsage` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "TokenUsage" DROP COLUMN "completionTokens",
DROP COLUMN "costUSD",
DROP COLUMN "model",
DROP COLUMN "promptTokens",
ADD COLUMN     "totalTokens" INTEGER NOT NULL;
