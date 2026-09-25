package postgres

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/internal/sqlstreams"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/admin/migrate"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// StreamsChannel is the default channel a stream's changes are announced
// on: the payload is the stream id. One channel for every stream, so a
// process holds one LISTEN connection however many streams it reads.
const StreamsChannel = "agentenkit_streams"

// StreamsOptions tunes NewRunStreams. Zero values take the defaults.
type StreamsOptions struct {
	// Listener wakes readers here when another process writes. Without one
	// they see its writes on their next poll.
	Listener Listener
	// Channel is the NOTIFY channel. Default StreamsChannel.
	Channel string
	// Poll is the longest a reader waits without news. Default 1s.
	Poll time.Duration
}

// RunStreams is RunStreams over Postgres: the <prefix>streams and
// <prefix>stream_events tables. A write sends NOTIFY in its transaction, so
// readers in other processes wake when it commits.
type RunStreams struct {
	*sqlstreams.Streams
	stop     context.CancelFunc
	stopOnce sync.Once
}

var _ ports.RunStreams = (*RunStreams)(nil)

// NewRunStreams migrates the tables and, with a Listener, starts listening.
// The prefix matches the storage's (default "agentenkit_").
func NewRunStreams(ctx context.Context, db *sql.DB, opts StreamsOptions, options ...Option) (*RunStreams, error) {
	s := &Storage{prefix: "agentenkit_"}
	for _, o := range options {
		o(s)
	}
	if opts.Poll <= 0 {
		opts.Poll = time.Second
	}
	if opts.Channel == "" {
		opts.Channel = StreamsChannel
	}
	streams, events := s.prefix+"streams", s.prefix+"stream_events"
	schema := sqlstreams.Schema(streams, events, "BIGSERIAL PRIMARY KEY", "BOOLEAN", "TIMESTAMPTZ")
	if err := migrateSchema(ctx, db, s.prefix, "streams", []migrate.Migration{
		migrate.NewMigration("streams_0001_init", schema...),
	}); err != nil {
		return nil, err
	}
	channel := opts.Channel
	r := &RunStreams{Streams: &sqlstreams.Streams{
		DB: db,
		D: sqlstreams.Dialect{
			Placeholder: func(n int) string { return "$" + strconv.Itoa(n) },
			Time:        func(t time.Time) any { return t },
			Now:         "now()",
			ForUpdate:   " FOR UPDATE",
			Notify: func(ctx context.Context, tx *sql.Tx, streamID string) error {
				_, err := tx.ExecContext(ctx, `SELECT pg_notify($1, $2)`, channel, streamID)
				return err
			},
		},
		Streams: streams,
		Events:  events,
		Poll:    opts.Poll,
	}}
	if opts.Listener != nil {
		lctx, cancel := context.WithCancel(context.Background())
		r.stop = cancel
		go func() { _ = opts.Listener.Listen(lctx, channel, r.Wake) }()
	}
	return r, nil
}

// Shutdown stops listening.
func (r *RunStreams) Shutdown() {
	r.stopOnce.Do(func() {
		if r.stop != nil {
			r.stop()
		}
	})
}
