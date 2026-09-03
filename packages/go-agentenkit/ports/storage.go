package ports

import "context"

// Storage is the persistence port for a caller's OWN data (§3.2): threads,
// messages, events, usage. DTOs cross the boundary; ORM types never leak into
// core. Implement it once for Postgres, MongoDB, DynamoDB, or use a shipped
// adapter.
//
// Operational history (run records, timings, step markers) is NOT here. The
// platform owns and stores that itself (§2.9); callers only read it back
// through the runtime's Admin API.
//
// Every method receives the run's StorageContext as its last argument, so an
// implementation can scope a query to a tenant, stamp a row with who caused
// it, or route to a different database entirely (§2.10).
type Storage interface {
	Threads() ThreadStore
	Messages() MessageStore
	Events() EventStore
	Usage() UsageStore
}

// ThreadInit is what a new thread starts with.
type ThreadInit struct {
	// Model defaults to "gpt-4o" when empty.
	Model string
}

// ThreadStore is the threads section of Storage.
type ThreadStore interface {
	// Get returns nil, nil when the thread does not exist.
	Get(ctx context.Context, threadID string, sc StorageContext) (*ThreadDTO, error)
	Create(ctx context.Context, init ThreadInit, sc StorageContext) (*ThreadDTO, error)
	// List returns threads most recent first.
	List(ctx context.Context, sc StorageContext) ([]ThreadDTO, error)
	SetState(ctx context.Context, threadID string, state ExecutionState, sc StorageContext) error
	// Delete removes the thread AND everything that follows it: messages,
	// events, usage rows. Returns an error if the thread does not exist.
	Delete(ctx context.Context, threadID string, sc StorageContext) error
	// ClaimState is a compare-and-set: true iff THIS caller performed the
	// transition. Must be atomic: a single conditional UPDATE or equivalent
	// (§2.5, §2.8).
	ClaimState(ctx context.Context, threadID string, from, to ExecutionState, sc StorageContext) (bool, error)
}

// MessageScope decides WHOSE turns List returns (§2.7):
//   - nil: every row on the thread, for UI hydration (§2.2)
//   - MainAgent (AgentID ""): the main agent's stream. Compaction (§2.6) and
//     the edit lookup (§5.1) must use this.
//   - AgentScope("sub_1"): that nested run's own stream.
type MessageScope struct {
	AgentID string
}

// MainAgent scopes a message listing to the main agent's stream.
var MainAgent = &MessageScope{}

// AgentScope scopes a message listing to one nested run's stream.
func AgentScope(agentID string) *MessageScope { return &MessageScope{AgentID: agentID} }

// MessageStore is the messages section of Storage.
type MessageStore interface {
	Append(ctx context.Context, threadID string, m NewMessage, sc StorageContext) (*MessageDTO, error)
	// List returns turns oldest first, filtered by scope (see MessageScope).
	List(ctx context.Context, threadID string, scope *MessageScope, sc StorageContext) ([]MessageDTO, error)
	// DeleteFrom deletes messageID and every message after it, in the same
	// order List returns (§5.1 edit + resend). Returns how many rows went; 0
	// when the id is not in this thread.
	DeleteFrom(ctx context.Context, threadID, messageID string, sc StorageContext) (int, error)
}

// EventStore is the events section of Storage.
type EventStore interface {
	Append(ctx context.Context, threadID string, e AgentEvent, sc StorageContext) error
	// ListSince returns all events after the cursor, ascending by seq (§2.2).
	ListSince(ctx context.Context, threadID string, sinceSeq int64, sc StorageContext) ([]AgentEvent, error)
	// Latest returns the most recent event of a type, or nil, nil.
	Latest(ctx context.Context, threadID, typ string, sc StorageContext) (*AgentEvent, error)
	// ListByType returns every event of a type, ascending by seq.
	ListByType(ctx context.Context, threadID, typ string, sc StorageContext) ([]AgentEvent, error)
}

// UsageStore is the usage section of Storage: one row per model call (§4).
//
// The platform writes a row after EVERY call it makes on a thread, priced by
// the runtime's Pricer before it gets here. An implementation stores the row
// as given; it never prices anything itself.
type UsageStore interface {
	Record(ctx context.Context, threadID string, u NewUsage, sc StorageContext) error
	// Total sums recorded calls: tokens and money (§4). An empty filter sums
	// the whole thread; UsageFilter{RunID: id} sums one dispatched run,
	// nested runs included, which is what a settle hook bills from.
	//
	// The result also carries Lines: the same spend grouped by agent and
	// model, so one call serves the thread header, the settle hook and the
	// admin reads alike. A thread with no usage rows returns zeroes.
	// Unpriced counts the calls with no cost on them, so a reader can tell
	// "nothing was spent" apart from "nothing was priced".
	Total(ctx context.Context, threadID string, filter UsageFilter, sc StorageContext) (UsageTotals, error)
}
