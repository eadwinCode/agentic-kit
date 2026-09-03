// Package pgxlisten is the postgres bus's Listener over pgx v5: one
// dedicated connection that LISTENs and hands every notification over.
// Kept apart from adapters/postgres so a program on lib/pq does not compile
// pgx just to use the storage.
package pgxlisten

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Listener holds a connection string and reconnects on its own.
type Listener struct {
	connString string
	// Backoff is the wait before reconnecting after a failure. Zero means
	// one second.
	Backoff time.Duration
	// OnError is told about a dropped connection, if set.
	OnError func(err error)
}

// New makes a listener for a connection string (postgres://...).
func New(connString string) *Listener { return &Listener{connString: connString} }

// Listen connects, LISTENs on channel, and calls handler for every
// notification until ctx ends. A dropped connection is retried after
// Backoff; what was missed meanwhile is the log's job to carry, not the
// bus's (§2.2).
func (l *Listener) Listen(ctx context.Context, channel string, handler func(payload string)) error {
	backoff := l.Backoff
	if backoff <= 0 {
		backoff = time.Second
	}
	for {
		err := l.once(ctx, channel, handler)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && l.OnError != nil {
			l.OnError(err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
	}
}

func (l *Listener) once(ctx context.Context, channel string, handler func(payload string)) error {
	conn, err := pgx.Connect(ctx, l.connString)
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return err
	}
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		handler(n.Payload)
	}
}
