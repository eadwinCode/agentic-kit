package sqlite

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/internal/sqlstreams"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// RunStreams is RunStreams over SQLite: the run_streams and
// run_stream_events tables, the same ones the TS adapter uses. Readers in
// this process wake on its own appends; one in another process sees them
// on its next poll.
type RunStreams struct{ *sqlstreams.Streams }

var _ ports.RunStreams = (*RunStreams)(nil)

// nowMsSQL is the current time in epoch milliseconds, the form expiresAt
// is kept in.
const nowMsSQL = `CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`

// NewRunStreams creates the tables if they are missing. A zero poll means
// 250ms.
func NewRunStreams(ctx context.Context, db *sql.DB, poll time.Duration) (*RunStreams, error) {
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}
	for _, stmt := range sqlstreams.Schema("run_streams", "run_stream_events",
		"INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER", "INTEGER") {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, err
		}
	}
	return &RunStreams{&sqlstreams.Streams{
		DB: db,
		D: sqlstreams.Dialect{
			Placeholder: func(n int) string { return "?" + strconv.Itoa(n) },
			Time:        func(t time.Time) any { return t.UnixMilli() },
			Now:         nowMsSQL,
		},
		Streams: "run_streams",
		Events:  "run_stream_events",
		Poll:    poll,
	}}, nil
}
