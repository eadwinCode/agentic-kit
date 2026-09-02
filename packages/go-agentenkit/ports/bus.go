package ports

import "context"

// EventBus is the pub/sub port: live fan-out + death notices (§2.2, §2.5).
//
// Delivery is at-most-once. The §2.5 death-notice/heartbeat/watchdog pattern
// exists precisely because pub/sub drops. Stronger buses may simplify the
// heartbeat but reclamation stays (§3.4).
type EventBus interface {
	Publish(ctx context.Context, threadID string, event AgentEvent) error
	// Subscribe returns an unsubscribe function. The handler may be called
	// from any goroutine. The reference adapters run the §2.5 heartbeat while
	// any subscriber is attached.
	Subscribe(ctx context.Context, threadID string, handler func(AgentEvent)) (func() error, error)
}
