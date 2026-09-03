// Package pricing holds the pricers that ship with the platform (§4).
//
// A Pricer turns one model call into money. The runtime calls it after every
// call, before the usage row is stored, so cost lives on the row next to the
// tokens instead of being worked out by whoever reads it later.
//
// Three cover almost everyone:
//
//	Table   — a price list, keyed by model. The common case.
//	Receipt — read the cost the provider already computed and sent back.
//	Chain   — try several in order; the first that answers wins.
//
// Anything else is a ports.Pricer of your own, or ports.PricerFunc around a
// plain function.
package pricing

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// USD is the currency the shipped pricers use unless told otherwise.
const USD = "USD"

// ModelPrice is what one model costs, per MILLION tokens, in the table's
// currency. A price of 3.0 for input means $3.00 per million input tokens,
// which is how every provider publishes them, so a price list can be typed
// straight off their pricing page.
type ModelPrice struct {
	InputPerMillion      float64
	CacheReadPerMillion  float64
	CacheWritePerMillion float64
	OutputPerMillion     float64
	// ReasoningPerMillion is usually 0. Most providers already count
	// reasoning tokens inside the output count, so charging them again here
	// bills the same tokens twice. Set it only when your provider reports
	// reasoning tokens SEPARATELY from output.
	ReasoningPerMillion float64
}

// Table is a price list keyed by model (§4): the registry key a run asked
// for ("claude-sonnet-4@high"), or the wire id the provider reported back
// ("claude-sonnet-4-20250514"). Both work; a lookup tries them in that
// order, then the registry key with any "@variant" suffix removed.
//
// A model the table does not know is not priced. Its row is stored with no
// cost and UsageTotals.Unpriced counts it, so a missing price shows up as a
// gap in the bill rather than as a silent zero.
type Table map[string]ModelPrice

// Price implements ports.Pricer in the table's default currency, USD.
func (t Table) Price(ctx context.Context, u ports.NewUsage) (*ports.Cost, error) {
	return t.priceIn(USD, u)
}

// In returns the same table priced in another currency. The numbers are not
// converted: it only changes the code that goes onto every Cost, for a table
// whose prices are already in that currency.
func (t Table) In(currency string) ports.Pricer {
	return ports.PricerFunc(func(_ context.Context, u ports.NewUsage) (*ports.Cost, error) {
		return t.priceIn(currency, u)
	})
}

// Lookup finds a model's price, trying the registry key, the wire id, then
// the registry key without its "@variant" suffix.
func (t Table) Lookup(model, modelID string) (ModelPrice, bool) {
	if p, ok := t[model]; ok {
		return p, true
	}
	if p, ok := t[modelID]; ok {
		return p, true
	}
	if base, _, found := strings.Cut(model, "@"); found {
		if p, ok := t[base]; ok {
			return p, true
		}
	}
	return ModelPrice{}, false
}

func (t Table) priceIn(currency string, u ports.NewUsage) (*ports.Cost, error) {
	p, ok := t.Lookup(u.Model, u.ModelID)
	if !ok {
		return nil, nil // unknown model: let the next pricer try
	}
	// tokens/1_000_000 × pricePerMillion is the cost in currency units, and
	// micros is that × 1_000_000. The two cancel: tokens × pricePerMillion
	// IS the micro-unit cost, with no float scaling in between.
	micros := float64(u.InputTokens)*p.InputPerMillion +
		float64(u.CacheReadInputTokens)*p.CacheReadPerMillion +
		float64(u.CacheWriteInputTokens)*p.CacheWritePerMillion +
		float64(u.OutputTokens)*p.OutputPerMillion +
		float64(u.ReasoningTokens)*p.ReasoningPerMillion
	return &ports.Cost{
		Micros: int64(math.Round(micros)), Currency: currency, Source: "table",
	}, nil
}

// ReceiptReader pulls a cost out of what the provider attached to the
// finish: a gateway receipt, a billing header, whatever your provider sends.
// It returns false when this call carried no receipt, and the next pricer in
// a Chain gets its turn.
type ReceiptReader func(meta map[string]any) (micros int64, ok bool)

// Receipt prices a call from the number the provider already computed, in
// USD. This is the most accurate pricer there is: no price list to keep in
// step with a provider's changes, and discounts and gateway markups are
// already inside the figure.
func Receipt(read ReceiptReader) ports.Pricer { return ReceiptIn(USD, read) }

// ReceiptIn is Receipt in another currency.
func ReceiptIn(currency string, read ReceiptReader) ports.Pricer {
	return ports.PricerFunc(func(_ context.Context, u ports.NewUsage) (*ports.Cost, error) {
		if read == nil || u.ProviderMetadata == nil {
			return nil, nil
		}
		micros, ok := read(u.ProviderMetadata)
		if !ok {
			return nil, nil
		}
		return &ports.Cost{Micros: micros, Currency: currency, Source: "receipt"}, nil
	})
}

// Chain tries each pricer in order and takes the first answer. Put the
// accurate one first and the fallback last:
//
//	pricing.Chain(
//	    pricing.Receipt(gatewayReceiptMicros), // the real figure, when sent
//	    table,                                 // otherwise the price list
//	)
//
// A pricer that errors is skipped rather than believed. If none answers,
// Chain returns the first error it saw, or nil, nil when they simply had
// nothing to say.
func Chain(pricers ...ports.Pricer) ports.Pricer {
	return ports.PricerFunc(func(ctx context.Context, u ports.NewUsage) (*ports.Cost, error) {
		var firstErr error
		for _, p := range pricers {
			if p == nil {
				continue
			}
			cost, err := p.Price(ctx, u)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if cost != nil {
				return cost, nil
			}
		}
		return nil, firstErr
	})
}

// Fixed prices every call the same, whatever the model. Useful for a credit
// system that charges per call rather than per token, and for tests.
func Fixed(micros int64, currency string) ports.Pricer {
	if currency == "" {
		currency = USD
	}
	return ports.PricerFunc(func(_ context.Context, _ ports.NewUsage) (*ports.Cost, error) {
		return &ports.Cost{Micros: micros, Currency: currency, Source: "table"}, nil
	})
}

// Micros converts a currency amount to micros: Micros(0.25) is 250_000.
func Micros(amount float64) int64 { return int64(math.Round(amount * 1_000_000)) }

// Amount converts micros back to a currency amount: Amount(250_000) is 0.25.
func Amount(micros int64) float64 { return float64(micros) / 1_000_000 }

// Format renders micros for a human: "0.2500 USD". Money for display only;
// never do arithmetic on the string.
func Format(micros int64, currency string) string {
	if currency == "" {
		currency = USD
	}
	return fmt.Sprintf("%.4f %s", Amount(micros), currency)
}
