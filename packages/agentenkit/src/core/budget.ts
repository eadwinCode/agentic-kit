import type { CheckCostBudgetExhaustedFn, CheckTokenBudgetExhaustedFn } from '../ports/runtime.js';

/** The token budget check the loop uses unless the host gives its own
 *  (§2.1): the run's `tokenBudget`, against what the run has used. The
 *  step that crossed the line is kept in full; the next one never starts.
 *  Published before the run stops, so a client learns why the moment it
 *  does. */
export const defaultCheckTokenBudgetExhausted: CheckTokenBudgetExhaustedFn = async (c) => {
  if (!c.tokenBudget || c.tokensUsed < c.tokenBudget) return false;
  await c.publish('TOKEN_BUDGET_EXHAUSTED', {
    agentId: c.agentId,
    tokensUsed: c.tokensUsed,
    tokenBudget: c.tokenBudget,
  });
  return true;
};

/** The money budget check the loop uses unless the host gives its own (§4):
 *  the run's `costBudgetMicros`, against the same shared ledger. A nested
 *  run's calls count against it, and a run that parked or retried starts
 *  from what it already spent (see seedRunLedger).
 *
 *  It only ever sees priced calls: with no pricer configured nothing is
 *  ever spent and the cap never fires. */
export const defaultCheckCostBudgetExhausted: CheckCostBudgetExhaustedFn = async (c) => {
  if (!c.costBudgetMicros || c.costMicros < c.costBudgetMicros) return false;
  await c.publish('COST_BUDGET_EXHAUSTED', {
    agentId: c.agentId,
    costMicros: c.costMicros,
    costBudgetMicros: c.costBudgetMicros,
    ...(c.currency ? { currency: c.currency } : {}),
  });
  return true;
};
