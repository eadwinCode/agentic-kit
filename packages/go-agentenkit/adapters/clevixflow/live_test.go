package clevixflow

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	flow "github.com/eadwinCode/flow-sdk"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/memory"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

func TestNames(t *testing.T) {
	if got := StreamID("run_1:2"); got != "agentenkit.run_1:2" {
		t.Errorf("StreamID = %s", got)
	}
	if got := ThreadChannel("t-1"); got != "agentenkit.thread.t-1" {
		t.Errorf("ThreadChannel = %s", got)
	}
	hashed := regexp.MustCompile(`^agentenkit\.(thread\.)?h\.[0-9a-f]{64}$`)
	for _, got := range []string{ThreadChannel("a b/c"), StreamID(strings.Repeat("x", 300))} {
		if !hashed.MatchString(got) {
			t.Errorf("not hashed: %s", got)
		}
	}
}

func TestNewNeedsAKv(t *testing.T) {
	c, _ := newClient(t)
	if _, err := NewBus(c, BusOptions{}); err == nil {
		t.Error("NewBus with no Kv: no error")
	}
	if _, err := NewStreams(c, StreamsOptions{}); err == nil {
		t.Error("NewStreams with no Kv: no error")
	}
}

// liveClient is a client on a real engine: TEST_FLOW_BASE_URL,
// TEST_FLOW_APP_ID and TEST_FLOW_SIGNING_KEY. The test skips without them.
func liveClient(t *testing.T) *flow.Client {
	t.Helper()
	base, app, key := os.Getenv("TEST_FLOW_BASE_URL"), os.Getenv("TEST_FLOW_APP_ID"), os.Getenv("TEST_FLOW_SIGNING_KEY")
	if base == "" || app == "" || key == "" {
		t.Skip("TEST_FLOW_BASE_URL, TEST_FLOW_APP_ID or TEST_FLOW_SIGNING_KEY not set: skipping")
	}
	c, err := flow.NewClient(flow.ClientOpts{AppID: app, BaseURL: flow.StrPtr(base), SigningKey: flow.StrPtr(key), NoSyncOnBoot: true})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func delta(s string) ports.StreamEvent {
	return &ports.TextMessageContentEvent{MessageID: "m", Delta: s}
}

func deltas(t *testing.T, items []ports.StreamItem) []string {
	t.Helper()
	var out []string
	for _, i := range items {
		e, ok := i.Event.(*ports.TextMessageContentEvent)
		if !ok {
			t.Fatalf("item %s is %T", i.Offset, i.Event)
		}
		out = append(out, e.Delta)
	}
	return out
}

func TestLiveBigItemGoesThroughTheKv(t *testing.T) {
	ctx := context.Background()
	s, err := NewStreams(liveClient(t), StreamsOptions{Kv: memory.NewKv()})
	if err != nil {
		t.Fatal(err)
	}
	id := "big-" + strconv.FormatInt(time.Now().UnixNano(), 10) + ":1"
	if err := s.Open(ctx, id, ports.StreamMeta{ThreadID: "t", RunID: "r"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	defer s.Delete(ctx, id)
	big := strings.Repeat("x", 100_000)
	offsets, err := s.Append(ctx, id, []ports.StreamEvent{delta("y"), delta(big), delta("y")})
	if err != nil || len(offsets) != 3 {
		t.Fatalf("offsets = %v, err = %v", offsets, err)
	}
	snap, err := s.Snapshot(ctx, id, "")
	if err != nil {
		t.Fatal(err)
	}
	got := deltas(t, snap.Items)
	if len(got) != 3 || got[0] != "y" || got[1] != big || got[2] != "y" {
		t.Fatalf("items = %d, lengths %d", len(got), len(got[1]))
	}
	if snap.Meta.ThreadID != "t" || snap.Meta.RunID != "r" {
		t.Fatalf("meta = %+v", snap.Meta)
	}
}

func TestLiveAppendOfManyGoesInBatches(t *testing.T) {
	ctx := context.Background()
	s, err := NewStreams(liveClient(t), StreamsOptions{Kv: memory.NewKv()})
	if err != nil {
		t.Fatal(err)
	}
	id := "many-" + strconv.FormatInt(time.Now().UnixNano(), 10) + ":1"
	if err := s.Open(ctx, id, ports.StreamMeta{ThreadID: "t", RunID: "r"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	defer s.Delete(ctx, id)
	var events []ports.StreamEvent
	for i := range 250 {
		events = append(events, delta(strconv.Itoa(i)))
	}
	if offsets, err := s.Append(ctx, id, events); err != nil || len(offsets) != 250 {
		t.Fatalf("offsets = %d, err = %v", len(offsets), err)
	}
	snap, err := s.Snapshot(ctx, id, "")
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range deltas(t, snap.Items) {
		if d != strconv.Itoa(i) {
			t.Fatalf("item %d is %s", i, d)
		}
	}
}

func TestLiveBus(t *testing.T) {
	ctx := context.Background()
	bus, err := NewBus(liveClient(t), BusOptions{Kv: memory.NewKv()})
	if err != nil {
		t.Fatal(err)
	}
	threadID := "bus-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	var mu sync.Mutex
	var a, b []ports.AgentEvent
	offA, err := bus.Subscribe(ctx, threadID, func(e ports.AgentEvent) { mu.Lock(); a = append(a, e); mu.Unlock() })
	if err != nil {
		t.Fatal(err)
	}
	offB, err := bus.Subscribe(ctx, threadID, func(e ports.AgentEvent) { mu.Lock(); b = append(b, e); mu.Unlock() })
	if err != nil {
		t.Fatal(err)
	}
	event := func(seq int64, text string) ports.AgentEvent {
		payload, _ := json.Marshal(map[string]string{"text": text})
		return ports.AgentEvent{ThreadID: threadID, Seq: seq, Type: "STATE_CHANGE", Payload: payload, CreatedAt: time.Now()}
	}
	wait := func(cond func() bool) {
		for i := 0; i < 50; i++ {
			mu.Lock()
			ok := cond()
			mu.Unlock()
			if ok {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	seqs := func(list []ports.AgentEvent) (out []int64) {
		for _, e := range list {
			out = append(out, e.Seq)
		}
		return out
	}
	if err := bus.Publish(ctx, threadID, event(1, "small")); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, threadID, event(2, strings.Repeat("z", 100_000))); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return len(a) >= 2 && len(b) >= 2 })
	mu.Lock()
	if got := seqs(a); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("a got %v", got)
	}
	if len(a[1].Payload) < 100_000 {
		t.Fatalf("the big event lost its payload: %d bytes", len(a[1].Payload))
	}
	mu.Unlock()
	_ = offA()
	if err := bus.Publish(ctx, threadID, event(3, "after")); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return len(b) >= 3 })
	mu.Lock()
	defer mu.Unlock()
	if got := seqs(b); len(got) != 3 {
		t.Fatalf("b got %v", got)
	}
	if got := seqs(a); len(got) != 2 {
		t.Fatalf("a got events after it left: %v", got)
	}
	_ = offB()
}
