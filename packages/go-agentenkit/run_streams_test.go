package agentenkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"net/http"
	"net/http/httptest"
	"os"

	goredis "github.com/redis/go-redis/v9"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/redis"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/upstash"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The promise every RunStreams adapter keeps, run against each one. The TS
// package runs the same cases under the same names (test/run-streams-suite.ts).

type streamsSuiteOptions struct {
	// other is a second handle on the same store, as another process would
	// have. A reader on one must see appends made through the other, with
	// no in-process wake-up to help.
	other func(t *testing.T) ports.RunStreams
	// settle is how long a live wait may take to notice an append.
	settle time.Duration
}

var streamSeq atomic.Int64

func freshStreamID() string {
	return fmt.Sprintf("s%d-%d:1", time.Now().UnixNano(), streamSeq.Add(1))
}

var suiteMeta = ports.StreamMeta{ThreadID: "t1", RunID: "r1"}

func textDelta(d string) ports.StreamEvent {
	return &ports.TextMessageContentEvent{MessageID: "m1", Delta: d}
}

var runFinished = &ports.RunFinishedEvent{Status: "finished"}

func deltasOf(items []ports.StreamItem) []string {
	out := []string{}
	for _, i := range items {
		if d, ok := i.Event.(*ports.TextMessageContentEvent); ok {
			out = append(out, d.Delta)
		} else {
			out = append(out, i.Event.StreamEventType())
		}
	}
	return out
}

// collectStream reads a stream to its end in the background.
func collectStream(ctx context.Context, s ports.RunStreams, id, after string) <-chan streamResult {
	done := make(chan streamResult, 1)
	go func() {
		var r streamResult
		for item, err := range s.Read(ctx, id, after) {
			if err != nil {
				r.err = err
				break
			}
			r.items = append(r.items, item)
		}
		done <- r
	}()
	return done
}

type streamResult struct {
	items []ports.StreamItem
	err   error
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func runStreamsSuite(t *testing.T, name string, make func(t *testing.T) ports.RunStreams, opts streamsSuiteOptions) {
	settle := opts.settle
	if settle == 0 {
		settle = 20 * time.Millisecond
	}
	ctx := context.Background()
	hour := time.Hour

	t.Run(name, func(t *testing.T) {
		t.Run("append returns increasing offsets", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			a, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("a"), textDelta("b")})
			must(t, err)
			b, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("c")})
			must(t, err)
			offsets := append(a, b...)
			for i := range offsets {
				snap, err := s.Snapshot(ctx, id, offsets[i])
				must(t, err)
				got := []string{}
				for _, it := range snap.Items {
					got = append(got, it.Offset)
				}
				if want := offsets[i+1:]; !reflect.DeepEqual(got, append([]string{}, want...)) {
					t.Fatalf("after %s: %v, want %v", offsets[i], got, want)
				}
			}
		})

		t.Run("opening an open stream is a no-op", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			_, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("a")})
			must(t, err)
			must(t, s.Open(ctx, id, suiteMeta, hour))
			snap, err := s.Snapshot(ctx, id, "")
			must(t, err)
			if got := deltasOf(snap.Items); !reflect.DeepEqual(got, []string{"a"}) || snap.Meta != suiteMeta {
				t.Fatal(got, snap.Meta)
			}
		})

		t.Run("read from null replays everything, then tails", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			_, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("a"), textDelta("b")})
			must(t, err)
			got := collectStream(ctx, s, id, "")
			time.Sleep(settle)
			_, err = s.Append(ctx, id, []ports.StreamEvent{textDelta("c")})
			must(t, err)
			must(t, s.Close(ctx, id, runFinished, hour))
			r := <-got
			must(t, r.err)
			if d := deltasOf(r.items); !reflect.DeepEqual(d, []string{"a", "b", "c", "RUN_FINISHED"}) {
				t.Fatal(d)
			}
		})

		t.Run("read after an offset skips what came before", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			offs, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("a"), textDelta("b")})
			must(t, err)
			must(t, s.Close(ctx, id, runFinished, hour))
			r := <-collectStream(ctx, s, id, offs[0])
			must(t, r.err)
			if d := deltasOf(r.items); !reflect.DeepEqual(d, []string{"b", "RUN_FINISHED"}) {
				t.Fatal(d)
			}
		})

		t.Run("a read ends on close and the end is the last item", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			got := collectStream(ctx, s, id, "")
			time.Sleep(settle)
			_, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("a")})
			must(t, err)
			must(t, s.Close(ctx, id, &ports.RunErrorEvent{Status: "error", Error: "boom"}, hour))
			r := <-got
			must(t, r.err)
			last, ok := r.items[len(r.items)-1].Event.(*ports.RunErrorEvent)
			if len(r.items) != 2 || !ok || last.Status != "error" || last.Error != "boom" {
				t.Fatal(deltasOf(r.items))
			}
		})

		t.Run("a late reader of a closed stream gets the end at once", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			_, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("a")})
			must(t, err)
			must(t, s.Close(ctx, id, runFinished, hour))
			r := <-collectStream(ctx, s, id, "")
			must(t, r.err)
			if d := deltasOf(r.items); !reflect.DeepEqual(d, []string{"a", "RUN_FINISHED"}) {
				t.Fatal(d)
			}
			snap, err := s.Snapshot(ctx, id, "")
			must(t, err)
			if end, ok := snap.End.(*ports.RunFinishedEvent); !ok || end.Status != "finished" {
				t.Fatal(snap.End)
			}
			// Reading from the end item on: nothing more will ever come.
			r = <-collectStream(ctx, s, id, snap.Items[len(snap.Items)-1].Offset)
			if r.err != nil || len(r.items) != 0 {
				t.Fatal(r)
			}
		})

		t.Run("snapshot never waits on an open stream", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			_, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("a")})
			must(t, err)
			snap, err := s.Snapshot(ctx, id, "")
			must(t, err)
			if d := deltasOf(snap.Items); !reflect.DeepEqual(d, []string{"a"}) || snap.End != nil {
				t.Fatal(d, snap.End)
			}
		})

		t.Run("append after close is refused", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			must(t, s.Close(ctx, id, runFinished, hour))
			if _, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("late")}); !errors.Is(err, ports.ErrStreamClosed) {
				t.Fatal(err)
			}
		})

		t.Run("a second close is a no-op", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			must(t, s.Close(ctx, id, runFinished, hour))
			must(t, s.Close(ctx, id, &ports.RunErrorEvent{Status: "lost", Error: "late sweep"}, hour))
			snap, err := s.Snapshot(ctx, id, "")
			must(t, err)
			if d := deltasOf(snap.Items); !reflect.DeepEqual(d, []string{"RUN_FINISHED"}) {
				t.Fatal(d) // the first end stands
			}
		})

		t.Run("a stream past its grace window reads as gone", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			must(t, s.Close(ctx, id, runFinished, time.Second))
			time.Sleep(1100 * time.Millisecond)
			snap, err := s.Snapshot(ctx, id, "")
			if err != nil || snap != nil {
				t.Fatal(snap, err)
			}
			if r := <-collectStream(ctx, s, id, ""); !errors.Is(r.err, ports.ErrStreamGone) {
				t.Fatal(r.err)
			}
		})

		t.Run("a read of a deleted stream ends as gone", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			_, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("a")})
			must(t, err)
			got := collectStream(ctx, s, id, "")
			time.Sleep(settle)
			must(t, s.Delete(ctx, id))
			if r := <-got; !errors.Is(r.err, ports.ErrStreamGone) {
				t.Fatal(r.err)
			}
			must(t, s.Delete(ctx, id)) // a missing one is a no-op
		})

		t.Run("a stream never opened is gone", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			snap, err := s.Snapshot(ctx, id, "")
			if err != nil || snap != nil {
				t.Fatal(snap, err)
			}
			if _, err := s.Append(ctx, id, []ports.StreamEvent{textDelta("a")}); !errors.Is(err, ports.ErrStreamGone) {
				t.Fatal(err)
			}
			if err := s.Close(ctx, id, runFinished, hour); !errors.Is(err, ports.ErrStreamGone) {
				t.Fatal(err)
			}
		})

		t.Run("an aborted read ends quietly", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			rctx, cancel := context.WithCancel(ctx)
			got := collectStream(rctx, s, id, "")
			time.Sleep(settle)
			cancel()
			if r := <-got; r.err != nil || len(r.items) != 0 {
				t.Fatal(r)
			}
		})

		t.Run("every event keeps its fields", func(t *testing.T) {
			s, id := make(t), freshStreamID()
			must(t, s.Open(ctx, id, suiteMeta, hour))
			events := []ports.StreamEvent{
				&ports.ToolCallEndEvent{ToolCallID: "c1", ToolName: "search", Args: json.RawMessage(`{"q":"x","n":2}`)},
				&ports.CustomEvent{Name: "SEARCH_PROGRESS", Value: json.RawMessage(`{"done":3,"of":10}`)},
				&ports.SubagentEventEvent{SubagentID: "sub_1", Event: textDelta("nested")},
			}
			_, err := s.Append(ctx, id, events)
			must(t, err)
			snap, err := s.Snapshot(ctx, id, "")
			must(t, err)
			for i, it := range snap.Items {
				want, _ := ports.EncodeStreamEvent(events[i])
				got, _ := ports.EncodeStreamEvent(it.Event)
				if string(got) != string(want) {
					t.Errorf("%s != %s", got, want)
				}
			}
		})

		if opts.other != nil {
			t.Run("a missed wake-up only delays, never drops", func(t *testing.T) {
				reader, writer, id := make(t), opts.other(t), freshStreamID()
				must(t, writer.Open(ctx, id, suiteMeta, hour))
				got := collectStream(ctx, reader, id, "")
				time.Sleep(settle)
				// Written through another handle: no wake-up reaches the
				// reader, only its own poll.
				_, err := writer.Append(ctx, id, []ports.StreamEvent{textDelta("a")})
				must(t, err)
				must(t, writer.Close(ctx, id, runFinished, hour))
				r := <-got
				must(t, r.err)
				if d := deltasOf(r.items); !reflect.DeepEqual(d, []string{"a", "RUN_FINISHED"}) {
					t.Fatal(d)
				}
			})
		}
	})
}

func TestRunStreams(t *testing.T) {
	runStreamsSuite(t, "memory", func(*testing.T) ports.RunStreams { return memory.NewRunStreams() }, streamsSuiteOptions{})

	if os.Getenv("TEST_REDIS_ADDR") == "" {
		t.Log("TEST_REDIS_ADDR not set: skipping the redis and upstash cases")
		return
	}
	shared := goredis.NewClient(&goredis.Options{Addr: os.Getenv("TEST_REDIS_ADDR")})
	t.Cleanup(func() { shared.Close() })
	runStreamsSuite(t, "redis", func(*testing.T) ports.RunStreams {
		return redis.NewRunStreams(shared, redis.StreamsOptions{})
	}, streamsSuiteOptions{
		// Another process: its own client, so no in-process wake-up reaches
		// the reader. The tail still sees the XADD, as it would across
		// processes.
		other: func(t *testing.T) ports.RunStreams {
			c := goredis.NewClient(&goredis.Options{Addr: os.Getenv("TEST_REDIS_ADDR")})
			t.Cleanup(func() { c.Close() })
			return redis.NewRunStreams(c, redis.StreamsOptions{})
		},
	})

	// Upstash's REST API, played by a small server in front of the same
	// Redis: a JSON command array in, {"result": ...} out.
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var cmd []any
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		res, err := shared.Do(r.Context(), cmd...).Result()
		if err != nil && !errors.Is(err, goredis.Nil) {
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": res})
	}))
	t.Cleanup(rest.Close)
	upstashRedis := &upstash.Redis{URL: rest.URL, Token: "test"}
	makeUpstash := func(*testing.T) ports.RunStreams { return upstash.NewRunStreams(upstashRedis, 50*time.Millisecond) }
	runStreamsSuite(t, "upstash", makeUpstash, streamsSuiteOptions{other: makeUpstash, settle: 100 * time.Millisecond})
}
