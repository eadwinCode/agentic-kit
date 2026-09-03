package agentenkit_test

import (
	"testing"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// A total is one currency. Rows priced in another unit are not added to it:
// they count as unpriced, so the figure stays a floor in a single unit
// rather than a sum of dollars and euros under one label.
func TestUsageAggregatorKeepsOneCurrency(t *testing.T) {
	var agg ports.UsageAggregator
	agg.Add(ports.NewUsage{AgentName: "a", Model: "m", InputTokens: 10, Cost: &ports.Cost{Micros: 100, Currency: "USD"}})
	agg.Add(ports.NewUsage{AgentName: "a", Model: "m", InputTokens: 10, Cost: &ports.Cost{Micros: 900, Currency: "EUR"}})
	agg.Add(ports.NewUsage{AgentName: "a", Model: "m", InputTokens: 10})
	total := agg.Totals()
	if total.Currency != "USD" || total.CostMicros != 100 {
		t.Fatalf("total = %s %d, want USD 100", total.Currency, total.CostMicros)
	}
	if total.Unpriced != 2 {
		t.Fatalf("unpriced = %d, want 2 (one unpriced, one in another currency)", total.Unpriced)
	}
	if len(total.Lines) != 1 || total.Lines[0].CostMicros != 100 || total.Lines[0].Calls != 3 {
		t.Fatalf("lines = %+v", total.Lines)
	}
}

// The SQL adapters read GROUP BY agent, model and currency; the merger folds
// those groups back into one line per agent and model with the same rule.
func TestUsageLineMergerFoldsGroupsAndKeepsOneCurrency(t *testing.T) {
	var m ports.UsageLineMerger
	line := func(cost int64, calls int) ports.UsageLine {
		return ports.UsageLine{AgentName: "a", Model: "m", InputTokens: 5, Calls: calls, CostMicros: cost}
	}
	m.Add(line(100, 2), "USD", 5, 0)
	m.Add(line(0, 1), "", 5, 1)      // the unpriced rows of the same agent and model
	m.Add(line(700, 1), "EUR", 5, 0) // priced in another unit
	total := m.Totals()
	if total.Currency != "USD" || total.CostMicros != 100 {
		t.Fatalf("total = %s %d, want USD 100", total.Currency, total.CostMicros)
	}
	if total.Unpriced != 2 || total.TotalTokens != 15 {
		t.Fatalf("unpriced = %d tokens = %d", total.Unpriced, total.TotalTokens)
	}
	if len(total.Lines) != 1 || total.Lines[0].Calls != 4 || total.Lines[0].CostMicros != 100 {
		t.Fatalf("lines = %+v", total.Lines)
	}
}
