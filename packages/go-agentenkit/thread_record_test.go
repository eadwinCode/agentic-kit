package agentenkit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	pgstorage "github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/postgres"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/sqlite"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/core"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Workstream S5: the thread record keeps only what must outlive a run. The
// TS package runs the same cases under the same names
// (test/thread-record.test.ts).

func TestThreadRecord(t *testing.T) {
	t.Run("a run writes no CHUNK to the thread record", func(t *testing.T) {
		h, _ := streamRuntime(t, scripted(step{text: "looking", calls: []call{{"c1", "look", `{}`}}}, step{text: "done"}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
			Name: "chat", Model: "gpt-4o",
			Tools: []agentenkit.Tool{tool("look", func(context.Context, map[string]any) (string, error) { return "seen", nil })},
		})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
		h.handleNext(t)
		stored, _ := h.storage.Events().ListSince(h.ctx, ran.ThreadID, -1, agentenkit.StorageContext{})
		var types []string
		for _, e := range stored {
			types = append(types, e.Type)
			mustEqual(t, core.RecordEventTypes[e.Type], true, e.Type+" is a record type")
		}
		mustStrings(t, types, []string{"RUN_STARTED", "RUN_ENDED"}, "the record")
	})

	t.Run("each segment starts and ends in the record", func(t *testing.T) {
		h, _ := streamRuntime(t, scripted(step{text: "hi"}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
		h.handleNext(t)
		got, _ := h.storage.Events().List(h.ctx, ran.ThreadID, ports.ThreadEventFilter{Types: []string{"RUN_STARTED", "RUN_ENDED"}}, agentenkit.StorageContext{})
		mustEqual(t, len(got), 2, "two entries")
		started, ended := got[0], got[1]
		mustEqual(t, started.RunID+" "+ended.RunID, ran.RunID+" "+ran.RunID, "both belong to the run")
		mustEqual(t, payload(started)["streamId"], ran.RunID+":1", "the stream")
		mustEqual(t, payload(started)["segment"], float64(1), "segment")
		mustEqual(t, payload(ended)["streamId"], ran.RunID+":1", "the same stream")
		mustEqual(t, payload(ended)["status"], "finished", "status")
		mustEqual(t, ended.Seq > started.Seq, true, "in order")
	})

	stores := []struct {
		name string
		open func(t *testing.T) ports.Storage
	}{
		{"memory", func(*testing.T) ports.Storage { return memory.NewStorage() }},
		{"sqlite", func(t *testing.T) ports.Storage {
			db, err := sqlite.Open(filepath.Join(t.TempDir(), "record.sqlite"))
			must(t, err)
			t.Cleanup(func() { db.Close() })
			s, err := sqlite.New(db)
			must(t, err)
			return s
		}},
	}
	if os.Getenv("TEST_ADMIN_PG") != "" {
		stores = append(stores, struct {
			name string
			open func(t *testing.T) ports.Storage
		}{"postgres", func(t *testing.T) ports.Storage {
			db := openPostgres(t)
			t.Cleanup(func() { db.Close() })
			s, err := pgstorage.New(context.Background(), db)
			must(t, err)
			return s
		}})
	}
	for _, st := range stores {
		t.Run("the store mints seqs in order, one per entry ("+st.name+")", func(t *testing.T) {
			s := st.open(t)
			ctx, sc := context.Background(), ports.StorageContext{}
			th, err := s.Threads().Create(ctx, ports.ThreadInit{}, sc)
			must(t, err)
			var wg sync.WaitGroup
			seqs := make([]int64, 5)
			for i := range 5 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					runID := ""
					if i%2 == 1 {
						runID = "r1"
					}
					e, err := s.Events().Append(ctx, th.ID, ports.NewThreadEvent{
						Type: "CONTEXT_COMPACTED", Payload: json.RawMessage(`{}`), RunID: runID,
					}, sc)
					if err != nil {
						t.Error(err)
					}
					seqs[i] = e.Seq
				}()
			}
			wg.Wait()
			sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
			mustEqual(t, fmt.Sprint(seqs), "[1 2 3 4 5]", "seqs")
			three := int64(3)
			count := func(f ports.ThreadEventFilter) int {
				got, err := s.Events().List(ctx, th.ID, f, sc)
				must(t, err)
				return len(got)
			}
			mustEqual(t, count(ports.ThreadEventFilter{After: &three}), 2, "after")
			mustEqual(t, count(ports.ThreadEventFilter{Limit: 2}), 2, "limit")
			mustEqual(t, count(ports.ThreadEventFilter{RunID: "r1"}), 2, "by run")
			mustEqual(t, count(ports.ThreadEventFilter{Types: []string{"INPUT_REQUIRED"}}), 0, "by type")
		})
	}

	t.Run("the snapshot carries the latest run stream, not every run", func(t *testing.T) {
		h, _ := streamRuntime(t, scripted(step{text: "one"}, step{text: "two"}, step{text: "three"}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat", Model: "gpt-4o"})
		var ran agentenkit.RunResult
		for _, prompt := range []string{"a", "b", "c"} {
			ran = h.run(t, chat, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: prompt})
			h.handleNext(t)
		}
		snap, err := h.rt.GetThreadSnapshot(h.ctx, ran.ThreadID, nil)
		must(t, err)
		mustEqual(t, snap.Stream.StreamID, ran.RunID+":1", "the latest run's stream")
		mustEqual(t, snap.Stream.RunID, ran.RunID, "its run")
		mustEqual(t, snap.Stream.End.(*ports.RunFinishedEvent).Status, "finished", "it ended")
		for _, i := range snap.Stream.Items {
			if _, ok := i.Event.(*ports.TextMessageContentEvent); ok {
				t.Fatal("the finished step's text is in the messages, not the items")
			}
		}
		mustEqual(t, snap.Stream.Offset != "", true, "an offset to read on from")
	})

	t.Run("a durable CUSTOM goes to the thread record; a plain one to the stream", func(t *testing.T) {
		h, streams := streamRuntime(t, scripted(step{calls: []call{{"c1", "go", `{}`}}}, step{text: "done"}))
		chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{
			Name: "chat", Model: "gpt-4o",
			Tools: []agentenkit.Tool{agentenkit.AgentTool("go", "go", func(ctx context.Context, _ map[string]any, tc agentenkit.ToolContext) (string, error) {
				if _, err := tc.PublishEvent(ctx, "INVOICE_CREATED", map[string]any{"id": "inv_1"}, agentenkit.PublishOptions{Durable: true}); err != nil {
					return "", err
				}
				_, err := tc.PublishEvent(ctx, "PROGRESS", map[string]any{"pct": 50}, agentenkit.PublishOptions{})
				return "ok", err
			})},
		})
		ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi"})
		h.handleNext(t)
		stored, _ := h.storage.Events().ListSince(h.ctx, ran.ThreadID, -1, agentenkit.StorageContext{})
		var record []string
		for _, e := range stored {
			record = append(record, e.Type)
		}
		mustStrings(t, record, []string{"RUN_STARTED", "INVOICE_CREATED", "RUN_ENDED"}, "only the durable one is kept")
		var custom []string
		for _, i := range streamItems(t, streams, ran.RunID+":1") {
			if c, ok := i.Event.(*ports.CustomEvent); ok {
				custom = append(custom, c.Name)
			}
		}
		// Each reaches a tab once: the durable one as its record entry, the
		// plain one on the stream.
		mustStrings(t, custom, []string{"PROGRESS"}, "only the plain one is on the stream")
	})
}
