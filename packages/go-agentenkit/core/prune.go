package core

import (
	"context"
	"errors"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// StreamOnlyTypes are the types older releases kept in the event log that
// now live only on a run stream or the bus. Nothing reads them from the
// table any more, so they can go. An app's own types are never among them:
// there is no telling whether the app wanted one kept. The TS runtime has
// the same list.
var StreamOnlyTypes = []string{
	"CHUNK", "SUBAGENT_CHUNK", "STEP_COMMITTED", "STEP_FINISHED", "TEXT_RESULT",
	"SUBAGENT_STARTED", "SUBAGENT_COMPLETED", "SUBAGENT_FAILED",
	"MESSAGE_APPENDED", "STATE_CHANGE", "HEARTBEAT",
}

// PruneOptions tunes PruneEvents.
type PruneOptions struct {
	// DryRun counts what would go, and deletes nothing.
	DryRun bool
	// BatchSize is rows per batch; each batch is its own short delete.
	// Zero means 10,000.
	BatchSize int
}

// PruneReport says what PruneEvents did.
type PruneReport struct {
	// ByType is rows deleted (or, on a dry run, found) per type.
	ByType  map[string]int64 `json:"byType"`
	Total   int64            `json:"total"`
	Batches int              `json:"batches"`
}

// PruneEvents deletes the stream-only rows older releases left in the event
// log (see StreamOnlyTypes), a batch at a time, so a large table never holds
// one long lock. Safe to stop and run again: each batch stands on its own.
// Needs a storage whose event store is an EventPruner.
func PruneEvents(ctx context.Context, deps ports.RuntimePorts, opts PruneOptions) (PruneReport, error) {
	pruner := deps.Storage.Events.Pruner()
	if pruner == nil {
		return PruneReport{}, errors.New("PruneEvents: this storage cannot prune its events")
	}
	limit := opts.BatchSize
	if limit <= 0 {
		limit = 10_000
	}
	report := PruneReport{ByType: map[string]int64{}}
	for {
		counts, err := pruner.Prune(ctx, StreamOnlyTypes, limit, opts.DryRun)
		if err != nil {
			return report, err
		}
		report.Batches++
		var n int64
		for typ, c := range counts {
			report.ByType[typ] += c
			n += c
		}
		report.Total += n
		// A dry run counts everything in one pass; a batch smaller than the
		// limit was the last one.
		if opts.DryRun || n < int64(limit) {
			return report, nil
		}
	}
}
