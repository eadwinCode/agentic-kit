package clevixflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	flow "github.com/eadwinCode/flow-sdk"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The platform's worker is what Functions takes.
var _ Worker = (*agentenkit.WorkerAPI)(nil)

const testKey = "signkey-abababababababababababababababababababababababababababababababab"

type call struct {
	method string
	path   string
	query  url.Values
	body   map[string]any
}

// engine is a fake engine that gives canned answers, one per call.
type engine struct {
	mu      sync.Mutex
	calls   []call
	answers []any
}

func (e *engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, _ := io.ReadAll(r.Body)
	c := call{method: r.Method, path: r.URL.Path, query: r.URL.Query()}
	if len(b) > 0 {
		_ = json.Unmarshal(b, &c.body)
	}
	e.calls = append(e.calls, c)
	if len(e.answers) == 0 {
		http.Error(w, `{"error":"no answer left"}`, http.StatusInternalServerError)
		return
	}
	answer := e.answers[0]
	e.answers = e.answers[1:]
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(answer)
}

func newClient(t *testing.T, answers ...any) (*flow.Client, *engine) {
	t.Helper()
	e := &engine{answers: answers}
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)
	c, err := flow.NewClient(flow.ClientOpts{
		AppID: "app", SigningKey: flow.StrPtr(testKey), SigningKeyFallback: flow.StrPtr(""),
		EventKey: flow.StrPtr(""), BaseURL: flow.StrPtr(srv.URL), Dev: flow.BoolPtr(true), NoSyncOnBoot: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, e
}

var job = ports.RunJob{ThreadID: "t1", RunID: "r1", DispatchID: "d1", Model: "m"}

func started() any { return map[string]any{"started": true, "runId": "run_1", "eventId": "evt_1"} }

func TestEnqueueStartsOneRun(t *testing.T) {
	c, e := newClient(t, started())
	if err := New(c, Options{}).Enqueue(context.Background(), job, &ports.EnqueueOptions{Key: "hitl-resume:c1", Priority: ports.PriorityLow}); err != nil {
		t.Fatal(err)
	}
	if len(e.calls) != 1 || e.calls[0].method != http.MethodPost || e.calls[0].path != "/v1/functions/agentenkit-run/start" {
		t.Fatalf("calls = %+v", e.calls)
	}
	data := e.calls[0].body["data"].(map[string]any)
	if data["key"] != "hitl-resume:c1" || data["priority"] != float64(-600) || data["notBefore"] != nil {
		t.Fatalf("data = %v", data)
	}
	if j := data["job"].(map[string]any); j["runId"] != "r1" || j["threadId"] != "t1" {
		t.Fatalf("job = %v", j)
	}
	// No id: a key must be free again once its run ends.
	if _, ok := e.calls[0].body["id"]; ok {
		t.Fatalf("an id was sent: %v", e.calls[0].body)
	}
}

func TestEnqueueUnkeyedUsesDispatchID(t *testing.T) {
	c, e := newClient(t, started())
	if err := New(c, Options{FunctionID: "mine"}).Enqueue(context.Background(), job, nil); err != nil {
		t.Fatal(err)
	}
	data := e.calls[0].body["data"].(map[string]any)
	if e.calls[0].path != "/v1/functions/mine/start" || data["key"] != "d1" || data["priority"] != float64(0) {
		t.Fatalf("call = %+v", e.calls[0])
	}
}

func TestEnqueueDelayIsNotBefore(t *testing.T) {
	c, e := newClient(t, started())
	before := time.Now().UnixMilli()
	if err := New(c, Options{}).Enqueue(context.Background(), job, &ports.EnqueueOptions{Delay: 30 * time.Second}); err != nil {
		t.Fatal(err)
	}
	nb := int64(e.calls[0].body["data"].(map[string]any)["notBefore"].(float64))
	if nb < before+30_000 || nb > time.Now().UnixMilli()+30_000 {
		t.Fatalf("notBefore = %d, want about %d", nb, before+30_000)
	}
}

func TestEnqueueSingleton(t *testing.T) {
	ctx := context.Background()
	held := map[string]any{"started": false, "reason": "singleton", "activeRunId": "run_0"}

	c, _ := newClient(t, held)
	if err := New(c, Options{}).Enqueue(ctx, job, &ports.EnqueueOptions{Key: "k"}); !errors.Is(err, ports.ErrDuplicateJob) {
		t.Fatalf("keyed: err = %v, want ErrDuplicateJob", err)
	}

	c, _ = newClient(t, held)
	if err := New(c, Options{}).Enqueue(ctx, job, nil); err != nil {
		t.Fatalf("unkeyed: err = %v, want nil", err)
	}

	// The run that held the key ended in between: start again.
	c, e := newClient(t, map[string]any{"started": false, "reason": "singleton", "activeRunId": nil}, started())
	if err := New(c, Options{}).Enqueue(ctx, job, &ports.EnqueueOptions{Key: "k"}); err != nil || len(e.calls) != 2 {
		t.Fatalf("ended in between: err = %v, calls = %d", err, len(e.calls))
	}
}

func TestEnqueuePausedOrMissing(t *testing.T) {
	c, _ := newClient(t, map[string]any{"started": false, "reason": "paused"})
	if err := New(c, Options{}).Enqueue(context.Background(), job, nil); err == nil || !strings.Contains(err.Error(), "paused") {
		t.Fatalf("paused: err = %v", err)
	}
	c, _ = newClient(t, map[string]any{"started": false, "reason": "not_found"})
	if err := New(c, Options{}).Enqueue(context.Background(), job, nil); err == nil || !strings.Contains(err.Error(), "no function") {
		t.Fatalf("not_found: err = %v", err)
	}
}

func TestEnqueueTooLarge(t *testing.T) {
	c, e := newClient(t)
	big := job
	big.State = ports.AgentRunState{"blob": strings.Repeat("x", 1_000_001)}
	if err := New(c, Options{}).Enqueue(context.Background(), big, nil); !errors.Is(err, ports.ErrPayloadTooLarge) {
		t.Fatalf("err = %v, want ErrPayloadTooLarge", err)
	}
	if len(e.calls) != 0 {
		t.Fatalf("the engine was called: %+v", e.calls)
	}
}

func TestCancel(t *testing.T) {
	c, e := newClient(t, map[string]any{"cancelled": []string{"run_1"}, "notActive": []string{}, "more": false, "next": nil})
	if err := New(c, Options{}).Cancel(context.Background(), "hitl-expiry:c1"); err != nil {
		t.Fatal(err)
	}
	b := e.calls[0].body
	if e.calls[0].path != "/v1/runs/cancel" || b["function"] != "agentenkit-run" || b["singletonKey"] != "hitl-expiry:c1" {
		t.Fatalf("call = %+v", e.calls[0])
	}
}

func TestFind(t *testing.T) {
	c, e := newClient(t, map[string]any{"items": []any{
		map[string]any{"id": "run_new", "functionId": "agentenkit-run", "status": "sleeping", "createdAt": 2000, "endedAt": nil, "tags": map[string]string{"runId": "r1", "threadId": "t1", "kind": "expiry"}},
		map[string]any{"id": "run_old", "functionId": "agentenkit-run", "status": "running", "createdAt": 1000, "endedAt": nil, "tags": map[string]string{"runId": "r1", "threadId": "t1"}},
	}, "next": nil}, map[string]any{"items": []any{}, "next": nil})
	q := New(c, Options{})
	found, err := q.Find(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	got := e.calls[0]
	if got.path != "/v1/runs" || got.query.Get("function") != "agentenkit-run" || got.query.Get("tag.runId") != "r1" ||
		got.query.Get("status") != "queued,running,sleeping,waiting,waiting_for_app,paused" {
		t.Fatalf("call = %+v", got)
	}
	want := ports.QueuedJob{ID: "run_old", RunID: "r1", ThreadID: "t1", RunAt: time.UnixMilli(1000), Position: -1}
	if found == nil || *found != want {
		t.Fatalf("found = %+v, want %+v", found, want)
	}
	if found, err := q.Find(context.Background(), "r1"); err != nil || found != nil {
		t.Fatalf("no run: found = %+v, err = %v", found, err)
	}
}

func TestStats(t *testing.T) {
	now := time.Now().UnixMilli()
	c, _ := newClient(t,
		map[string]any{"queued": 3, "running": 2, "sleeping": 4, "waiting": 1, "waiting_for_app": 1, "paused": 0, "completed": 9, "failed": 5, "cancelled": 1, "since": 0},
		map[string]any{"items": []any{map[string]any{"id": "run_q", "functionId": "agentenkit-run", "status": "queued", "createdAt": now - 5000, "endedAt": nil, "tags": map[string]string{}}}, "next": nil},
	)
	s, err := New(c, Options{}).Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.Ready != 4 || s.Delayed != 5 || s.InFlight != 2 || s.Dead != 5 || s.Paused || s.OldestReadyMs < 5000 {
		t.Fatalf("stats = %+v", s)
	}
}

// fakeWorker records what the function hands it.
type fakeWorker struct {
	mu      sync.Mutex
	handled []ports.RunJob
	dead    []ports.RunJob
	tries   []int
	causes  []error
}

func (w *fakeWorker) Handler() func(context.Context, ports.RunJob) error {
	return func(_ context.Context, job ports.RunJob) error {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.handled = append(w.handled, job)
		return nil
	}
}

func (w *fakeWorker) HandleDeadJob(_ context.Context, job ports.RunJob, attempts int, cause error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dead = append(w.dead, job)
	w.tries = append(w.tries, attempts)
	w.causes = append(w.causes, cause)
}

func TestFunctionsConfig(t *testing.T) {
	c, _ := newClient(t)
	fns, err := Functions(c, &fakeWorker{}, FunctionOpts{Concurrency: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 || fns[0].ID() != "agentenkit-run" || fns[1].ID() != "agentenkit-run-failure" {
		t.Fatalf("functions = %v, %v", fns[0].ID(), fns[1].ID())
	}
	got, _ := json.Marshal(fns[0].Config())
	var cfg map[string]any
	_ = json.Unmarshal(got, &cfg)
	for _, want := range []string{
		`"singleton":{"key":"event.data.key","mode":"skip"}`,
		`"priority":{"run":"event.data.priority"}`,
		`"concurrency":[{"limit":5,"key":"event.data.job.partitionKey"}]`,
		`"triggers":[{"event":"agentenkit/job"}]`,
		`"tags":{"kind":"event.data.job.kind","runId":"event.data.job.runId","threadId":"event.data.job.threadId"}`,
	} {
		if !strings.Contains(string(got), want) {
			t.Errorf("config has no %s:\n%s", want, got)
		}
	}
	if step := cfg["timeouts"].(map[string]any)["step"]; step != "840s" {
		t.Errorf("step timeout = %v, want 840s", step)
	}
	failure, _ := json.Marshal(fns[1].Config())
	if !strings.Contains(string(failure), `"if":"event.data.function_id == 'app-agentenkit-run'"`) {
		t.Errorf("failure trigger: %s", failure)
	}
}

// invoke calls one function the way the engine does, with no steps done yet.
func invoke(t *testing.T, c *flow.Client, fnID string, event any) []map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"fnId": fnID, "runId": "run_1", "event": event, "events": []any{event},
		"memo": map[string]any{}, "attempt": 0, "maxAttempts": 5,
	})
	w := httptest.NewRecorder()
	c.Serve().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "https://app.example.com/api/flow", bytes.NewReader(body)))
	res := w.Result()
	b, _ := io.ReadAll(res.Body)
	var ops []map[string]any
	if res.StatusCode == http.StatusPartialContent {
		if err := json.Unmarshal(b, &ops); err != nil {
			t.Fatalf("body is not a list of ops: %s", b)
		}
	} else if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", res.StatusCode, b)
	}
	return ops
}

func jobEvent(data JobData) map[string]any {
	return map[string]any{"id": "evt_1", "name": "inngest/function.invoked", "data": data, "ts": 1}
}

func TestFunctionRunsTheJobInOneStep(t *testing.T) {
	c, _ := newClient(t)
	w := &fakeWorker{}
	if _, err := Functions(c, w, FunctionOpts{}); err != nil {
		t.Fatal(err)
	}
	ops := invoke(t, c, "agentenkit-run", jobEvent(JobData{Job: job, Key: "d1"}))
	if len(ops) != 1 || ops[0]["name"] != "handle" {
		t.Fatalf("ops = %v", ops)
	}
	if len(w.handled) != 1 || w.handled[0].RunID != "r1" {
		t.Fatalf("handled = %+v", w.handled)
	}
}

func TestFunctionSleepsUntilNotBeforeFirst(t *testing.T) {
	c, _ := newClient(t)
	w := &fakeWorker{}
	if _, err := Functions(c, w, FunctionOpts{}); err != nil {
		t.Fatal(err)
	}
	ops := invoke(t, c, "agentenkit-run", jobEvent(JobData{Job: job, Key: "d1", NotBefore: time.Now().Add(time.Hour).UnixMilli()}))
	if len(ops) != 1 || ops[0]["name"] != "delay" {
		t.Fatalf("ops = %v", ops)
	}
	if len(w.handled) != 0 {
		t.Fatalf("the job ran before its time: %+v", w.handled)
	}
}

func TestFailureHandsTheJobToHandleDeadJob(t *testing.T) {
	c, _ := newClient(t)
	w := &fakeWorker{}
	if _, err := Functions(c, w, FunctionOpts{}); err != nil {
		t.Fatal(err)
	}
	trigger, _ := json.Marshal(jobEvent(JobData{Job: job, Key: "d1"}))
	invoke(t, c, "agentenkit-run-failure", map[string]any{
		"id": "evt_f", "name": "inngest/function.failed", "ts": 1,
		"data": map[string]any{
			"function_id": "app-agentenkit-run", "run_id": "run_1", "attempts": 5,
			"error": map[string]any{"name": "Error", "message": "store down"}, "event": json.RawMessage(trigger),
		},
	})
	if len(w.dead) != 1 || w.dead[0].RunID != "r1" || w.tries[0] != 5 || !strings.Contains(w.causes[0].Error(), "store down") {
		t.Fatalf("dead = %+v, tries = %v, causes = %v", w.dead, w.tries, w.causes)
	}
}
