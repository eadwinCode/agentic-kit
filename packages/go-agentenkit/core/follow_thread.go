package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// ThreadCursor is where a client is: the thread record's last seq, and its
// place in a run stream. On the wire (an SSE id:, or a cursor query) it is
// one string, "<seq> <streamId> <offset>", with "-" for a part it does not
// have. The TS runtime reads and writes the same form.
type ThreadCursor struct {
	Seq      int64
	StreamID string
	Offset   string
}

// FormatCursor is a cursor's wire form.
func FormatCursor(c ThreadCursor) string {
	dash := func(s string) string {
		if s == "" {
			return "-"
		}
		return s
	}
	return strconv.FormatInt(c.Seq, 10) + " " + dash(c.StreamID) + " " + dash(c.Offset)
}

// ParseCursor reads a cursor. A bare number is a record seq (what older
// clients send); anything unreadable is no cursor.
func ParseCursor(raw string) (ThreadCursor, bool) {
	parts := strings.Fields(raw)
	if len(parts) == 0 {
		return ThreadCursor{}, false
	}
	seq, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return ThreadCursor{}, false
	}
	c := ThreadCursor{Seq: seq}
	if len(parts) > 1 && parts[1] != "-" {
		c.StreamID = parts[1]
	}
	if len(parts) > 2 && parts[2] != "-" {
		c.Offset = parts[2]
	}
	return c, true
}

// Frame kinds.
const (
	FrameKindThread   = "thread"
	FrameKindStream   = "stream"
	FrameKindSnapshot = "snapshot"
)

// FollowFrame is one frame of a thread's follow: a thread event (a record
// entry, or a notice), an item from its run stream, or a SNAPSHOT when the
// stream the client was reading is gone. Its JSON is the TS runtime's.
type FollowFrame struct {
	Kind     string
	Event    *ports.AgentEvent
	StreamID string
	Item     *ports.StreamItem
	Snapshot *ports.ThreadSnapshot
}

// MarshalJSON writes the frame as {"kind": ..., ...}.
func (f FollowFrame) MarshalJSON() ([]byte, error) {
	switch f.Kind {
	case FrameKindThread:
		return json.Marshal(struct {
			Kind  string            `json:"kind"`
			Event *ports.AgentEvent `json:"event"`
		}{f.Kind, f.Event})
	case FrameKindStream:
		return json.Marshal(struct {
			Kind     string            `json:"kind"`
			StreamID string            `json:"streamId"`
			Item     *ports.StreamItem `json:"item"`
		}{f.Kind, f.StreamID, f.Item})
	case FrameKindSnapshot:
		return json.Marshal(struct {
			Kind     string                `json:"kind"`
			Snapshot *ports.ThreadSnapshot `json:"snapshot"`
		}{f.Kind, f.Snapshot})
	}
	return nil, fmt.Errorf("unknown frame kind %q", f.Kind)
}

// FollowThreadOptions tunes FollowThread.
type FollowThreadOptions struct {
	// Cursor is where the client is. Nil follows from now: the record from
	// the start, and the run stream in flight from what the messages lack.
	Cursor *ThreadCursor
	// LastMessageID is the last message the client has, so a SNAPSHOT
	// carries only newer ones.
	LastMessageID string
}

// FrameStream is a thread's follow. Read Frames() until it closes, then
// check Err().
type FrameStream struct {
	ch  chan FollowFrame
	mu  sync.Mutex
	err error
}

// Frames is the channel of frames. It closes when the context is cancelled
// or the follow failed.
func (s *FrameStream) Frames() <-chan FollowFrame { return s.ch }

// Err reports why the follow stopped, once Frames() is closed. Nil for a
// plain cancellation.
func (s *FrameStream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// FollowThread is a thread live, as one sequence of frames (§2.2): its
// record entries and notices (subscribe first, replay after the cursor,
// never skip a seq; see FollowEvents), and its run streams. The stream the
// cursor names is read on from its offset; a RUN_STARTED moves the read to
// the new segment's stream; a stream that is gone becomes one SNAPSHOT
// frame, after which the follow carries on from the snapshot's stream.
// Cancel ctx, or the follow outlives the client.
func FollowThread(ctx context.Context, deps ports.RuntimePorts, threadID string, opts FollowThreadOptions) (*FrameStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	out := &FrameStream{ch: make(chan FollowFrame, 64)}
	var senders sync.WaitGroup
	fail := func(err error) {
		out.mu.Lock()
		if out.err == nil {
			out.err = err
		}
		out.mu.Unlock()
		cancel()
	}
	send := func(f FollowFrame) bool {
		select {
		case out.ch <- f:
			return true
		case <-ctx.Done():
			return false
		}
	}

	// The stream being read, and the read's own stop.
	var readMu sync.Mutex
	var readingID string
	var stopRead context.CancelFunc
	var readStream func(streamID, after string, fromCursor bool)
	readStream = func(streamID, after string, fromCursor bool) {
		readMu.Lock()
		if readingID == streamID {
			readMu.Unlock()
			return
		}
		if stopRead != nil {
			stopRead()
		}
		rctx, stop := context.WithCancel(ctx)
		readingID, stopRead = streamID, stop
		readMu.Unlock()
		senders.Add(1)
		go func() {
			defer senders.Done()
			for item, err := range deps.Streams.Read(rctx, streamID, after) {
				if err != nil {
					if !errors.Is(err, ports.ErrStreamGone) {
						fail(err)
						return
					}
					// Only a stream the client was already reading needs a
					// catch-up: one a RUN_STARTED named was never on its screen.
					if fromCursor && rctx.Err() == nil {
						snap, err := ThreadSnapshotOf(ctx, deps, threadID, opts.LastMessageID)
						if err != nil {
							fail(err)
							return
						}
						if snap != nil && send(FollowFrame{Kind: FrameKindSnapshot, Snapshot: snap}) &&
							snap.Stream != nil && snap.Stream.End == nil {
							readStream(snap.Stream.StreamID, snap.Stream.Offset, false)
						}
					}
					return
				}
				if !send(FollowFrame{Kind: FrameKindStream, StreamID: streamID, Item: &item}) {
					return
				}
			}
		}()
	}

	var since int64 = -1
	if opts.Cursor != nil {
		since = opts.Cursor.Seq
	}
	if deps.Streams != nil {
		if opts.Cursor != nil && opts.Cursor.StreamID != "" {
			readStream(opts.Cursor.StreamID, opts.Cursor.Offset, true)
		} else {
			// No stream cursor: start from what the messages lack, as a
			// snapshot would.
			current, err := SnapshotStreamOf(ctx, deps, threadID)
			if err != nil {
				cancel()
				return nil, err
			}
			if current != nil {
				senders.Add(1)
				go func() {
					defer senders.Done()
					for i := range current.Items {
						if !send(FollowFrame{Kind: FrameKindStream, StreamID: current.StreamID, Item: &current.Items[i]}) {
							return
						}
					}
					if current.End == nil {
						readStream(current.StreamID, current.Offset, false)
					}
				}()
			}
		}
	}

	thread, err := FollowEvents(ctx, deps, threadID, FollowOptions{Since: since})
	if err != nil {
		cancel()
		return nil, err
	}
	senders.Add(1)
	go func() {
		defer senders.Done()
		for e := range thread.Events() {
			if !send(FollowFrame{Kind: FrameKindThread, Event: &e}) {
				return
			}
			if e.Type == "RUN_STARTED" && e.Seq > 0 && deps.Streams != nil {
				var p struct {
					StreamID string `json:"streamId"`
				}
				if json.Unmarshal(e.Payload, &p) == nil && p.StreamID != "" {
					readStream(p.StreamID, "", false)
				}
			}
		}
		if err := thread.Err(); err != nil {
			fail(err)
		}
	}()

	go func() {
		<-ctx.Done()
		senders.Wait()
		close(out.ch)
	}()
	return out, nil
}

// FollowFrameSSE is the SSE frame for a follow frame. A frame that moves the
// client's cursor carries it as its id:, so a browser that reconnects sends
// it back as Last-Event-ID; a notice carries none, and leaves it as it was.
func FollowFrameSSE(f FollowFrame, cursor *ThreadCursor) string {
	moved := false
	switch f.Kind {
	case FrameKindThread:
		if f.Event.Seq > 0 {
			cursor.Seq, moved = f.Event.Seq, true
		}
	case FrameKindStream:
		cursor.StreamID, cursor.Offset, moved = f.StreamID, f.Item.Offset, true
	case FrameKindSnapshot:
		cursor.Seq, cursor.StreamID, cursor.Offset = f.Snapshot.LastEventSeq, "", ""
		if s := f.Snapshot.Stream; s != nil {
			cursor.StreamID, cursor.Offset = s.StreamID, s.Offset
		}
		moved = true
	}
	data, _ := json.Marshal(f)
	if !moved {
		return fmt.Sprintf("data: %s\n\n", data)
	}
	return fmt.Sprintf("id: %s\ndata: %s\n\n", FormatCursor(*cursor), data)
}

// FollowSSE is a thread's follow, encoded as Server-Sent Events. It is a
// WriterTo and an http.Handler rather than a Response, because half the
// ecosystem has its own response type.
type FollowSSE struct {
	Headers map[string]string
	frames  *FrameStream
	cursor  ThreadCursor
	retryMs int
}

// ToFollowSSE wraps a FrameStream for SSE; cursor is where the client was.
func ToFollowSSE(frames *FrameStream, cursor *ThreadCursor, retryMs int) *FollowSSE {
	at := ThreadCursor{Seq: -1}
	if cursor != nil {
		at = *cursor
	}
	return &FollowSSE{Headers: SSEHeaders, frames: frames, cursor: at, retryMs: retryMs}
}

// Err reports why the underlying follow stopped.
func (s *FollowSSE) Err() error { return s.frames.Err() }

// WriteTo writes frames until the follow ends, flushing after each one when
// w is an http.Flusher.
func (s *FollowSSE) WriteTo(w io.Writer) (int64, error) {
	flusher, _ := w.(http.Flusher)
	var total int64
	write := func(frame string) error {
		n, err := io.WriteString(w, frame)
		total += int64(n)
		if err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}
	if s.retryMs > 0 {
		if err := write(fmt.Sprintf("retry: %d\n\n", s.retryMs)); err != nil {
			return total, err
		}
	}
	for f := range s.frames.Frames() {
		if err := write(FollowFrameSSE(f, &s.cursor)); err != nil {
			return total, err
		}
	}
	return total, s.frames.Err()
}

// ServeHTTP sets the SSE headers and writes the stream. Create the follow
// with the request's context so a hang-up stops it.
func (s *FollowSSE) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	for k, v := range s.Headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = s.WriteTo(w)
}
