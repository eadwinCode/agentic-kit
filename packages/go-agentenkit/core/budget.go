package core

import (
	"context"
	"fmt"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// DefaultCheckTokenBudgetExhausted is the token budget check the loop uses
// unless the host gives its own (§2.1): the run's TokenBudget, against what
// the run has used. The step that crossed the line is kept in full; the
// next one never starts. Published before the run stops, so a client learns
// why the moment it does.
func DefaultCheckTokenBudgetExhausted(ctx context.Context, c ports.TokenBudgetCheck) (bool, error) {
	if c.TokenBudget <= 0 || c.TokensUsed < c.TokenBudget {
		return false, nil
	}
	_ = c.Publish(ctx, "TOKEN_BUDGET_EXHAUSTED", map[string]any{
		"agentId": nullable(c.AgentID), "tokensUsed": c.TokensUsed, "tokenBudget": c.TokenBudget,
	})
	return true, nil
}

// DefaultCheckCostBudgetExhausted is the money budget check the loop uses
// unless the host gives its own (§4): the run's CostBudgetMicros, against
// the same shared ledger. A nested run's calls count against it, and a run
// that parked or retried starts from what it already spent (see
// SeedRunLedger).
//
// It only ever sees priced calls: with no Pricer configured nothing is ever
// spent and the cap never fires.
func DefaultCheckCostBudgetExhausted(ctx context.Context, c ports.CostBudgetCheck) (bool, error) {
	if c.CostBudgetMicros <= 0 || c.CostMicros < c.CostBudgetMicros {
		return false, nil
	}
	exhausted := map[string]any{
		"agentId": nullable(c.AgentID), "costMicros": c.CostMicros, "costBudgetMicros": c.CostBudgetMicros,
	}
	if c.Currency != "" { // left out when unset, as TS does
		exhausted["currency"] = c.Currency
	}
	_ = c.Publish(ctx, "COST_BUDGET_EXHAUSTED", exhausted)
	return true, nil
}

// budgetExhausted runs one budget check. An error or a panic is logged and
// read as false, as a throw is in TS: a check that fails must never fail a
// run.
func budgetExhausted(ctx context.Context, deps ports.RuntimePorts, name, runID string, check func() (bool, error)) (stop bool) {
	defer func() {
		if r := recover(); r != nil {
			Logger(deps).Error(name+" check failed", "run", runID, "err", fmt.Sprint(r))
			stop = false
		}
	}()
	stop, err := check()
	if err != nil {
		Logger(deps).Error(name+" check failed", "run", runID, "err", err)
		return false
	}
	return stop
}
