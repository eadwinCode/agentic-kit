package ports

import "context"

// AgentRunState is whatever a caller needs carried through a run: tenant,
// user, request id, feature flags (§2.10). The platform never reads it; it
// only guarantees that everything acting on behalf of a run can see it.
//
// In TypeScript this is an augmentable interface. In Go it is a map; read
// your own keys back with a small helper of your own.
type AgentRunState map[string]any

// StorageContext is passed to every domain storage call so an
// implementation can scope a query, stamp a row, or route to a tenant's
// database (§2.10).
type StorageContext struct {
	State AgentRunState
	// RunID is the run this call serves. Empty for reads outside a run.
	RunID string
}

// BoundStorage is the Storage shape core works against: the caller's
// implementation with this run's context already attached. Core keeps
// calling `Messages.Append(ctx, threadID, msg)` and the implementation still
// receives the state (§2.10).
type BoundStorage struct {
	Threads  BoundThreads
	Messages BoundMessages
	Events   BoundEvents
	Usage    BoundUsage
	// Ctx is the context every call carries.
	Ctx StorageContext
}

// BindStorage attaches a run's context to every storage method once.
func BindStorage(storage Storage, sc StorageContext) BoundStorage {
	if sc.State == nil {
		sc.State = AgentRunState{}
	}
	return BoundStorage{
		Threads:  BoundThreads{s: storage.Threads(), sc: sc},
		Messages: BoundMessages{s: storage.Messages(), sc: sc},
		Events:   BoundEvents{s: storage.Events(), sc: sc},
		Usage:    BoundUsage{s: storage.Usage(), sc: sc},
		Ctx:      sc,
	}
}

// BoundThreads is ThreadStore with the StorageContext bound.
type BoundThreads struct {
	s  ThreadStore
	sc StorageContext
}

func (b BoundThreads) Get(ctx context.Context, threadID string) (*ThreadDTO, error) {
	return b.s.Get(ctx, threadID, b.sc)
}
func (b BoundThreads) Create(ctx context.Context, init ThreadInit) (*ThreadDTO, error) {
	return b.s.Create(ctx, init, b.sc)
}
func (b BoundThreads) List(ctx context.Context) ([]ThreadDTO, error) { return b.s.List(ctx, b.sc) }
func (b BoundThreads) SetState(ctx context.Context, threadID string, state ExecutionState) error {
	return b.s.SetState(ctx, threadID, state, b.sc)
}
func (b BoundThreads) Delete(ctx context.Context, threadID string) error {
	return b.s.Delete(ctx, threadID, b.sc)
}
func (b BoundThreads) ClaimState(ctx context.Context, threadID string, from, to ExecutionState) (bool, error) {
	return b.s.ClaimState(ctx, threadID, from, to, b.sc)
}

// BoundMessages is MessageStore with the StorageContext bound.
type BoundMessages struct {
	s  MessageStore
	sc StorageContext
}

func (b BoundMessages) Append(ctx context.Context, threadID string, m NewMessage) (*MessageDTO, error) {
	return b.s.Append(ctx, threadID, m, b.sc)
}
func (b BoundMessages) List(ctx context.Context, threadID string, scope *MessageScope) ([]MessageDTO, error) {
	return b.s.List(ctx, threadID, scope, b.sc)
}
func (b BoundMessages) DeleteFrom(ctx context.Context, threadID, messageID string) (int, error) {
	return b.s.DeleteFrom(ctx, threadID, messageID, b.sc)
}

// BoundEvents is EventStore with the StorageContext bound.
type BoundEvents struct {
	s  EventStore
	sc StorageContext
}

func (b BoundEvents) Append(ctx context.Context, threadID string, e AgentEvent) error {
	return b.s.Append(ctx, threadID, e, b.sc)
}
func (b BoundEvents) ListSince(ctx context.Context, threadID string, sinceSeq int64) ([]AgentEvent, error) {
	return b.s.ListSince(ctx, threadID, sinceSeq, b.sc)
}
func (b BoundEvents) Latest(ctx context.Context, threadID, typ string) (*AgentEvent, error) {
	return b.s.Latest(ctx, threadID, typ, b.sc)
}
func (b BoundEvents) ListByType(ctx context.Context, threadID, typ string) ([]AgentEvent, error) {
	return b.s.ListByType(ctx, threadID, typ, b.sc)
}

// BoundUsage is UsageStore with the StorageContext bound.
type BoundUsage struct {
	s  UsageStore
	sc StorageContext
}

func (b BoundUsage) Record(ctx context.Context, threadID string, u NewUsage) error {
	return b.s.Record(ctx, threadID, u, b.sc)
}

// Total sums the thread's calls. Pass UsageFilter{RunID: id} for one run.
func (b BoundUsage) Total(ctx context.Context, threadID string, filter UsageFilter) (UsageTotals, error) {
	return b.s.Total(ctx, threadID, filter, b.sc)
}
