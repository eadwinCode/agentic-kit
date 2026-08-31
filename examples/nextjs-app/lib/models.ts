import { createOpenAI } from '@ai-sdk/openai';
import type { ModelRegistry } from '@agent/core';

/** §2.3 — users register any `ai`-SDK models here. Keys feed the UI dropdown
 *  and `config.billingRates` prices them (§4). */
export const modelRegistry: ModelRegistry = {
  'gpt-4o': createOpenAI({
    apiKey: process.env.OPENAI_API_KEY,
    compatibility: 'strict',
  })('gpt-4o'),
  'gpt-4o-mini': createOpenAI({
    apiKey: process.env.OPENAI_API_KEY,
    compatibility: 'strict',
  })('gpt-4o-mini'),
};
