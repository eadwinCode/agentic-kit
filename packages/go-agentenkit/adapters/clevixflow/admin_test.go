package clevixflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

func runItem(id, status string, createdAt, endedAt int64, tags map[string]string) map[string]any {
	return map[string]any{"id": id, "functionId": "agentenkit-run", "status": status, "createdAt": createdAt, "endedAt": endedAt, "tags": tags}
}

func page(next string, items ...any) map[string]any {
	if items == nil {
		items = []any{}
	}
	return map[string]any{"items": items, "next": next}
}

func TestEnqueueHoldsWhilePaused(t *testing.T) {
	c, e := newClient(t, started())
	if err := New(c, Options{}).Enqueue(context.Background(), job, nil); err != nil {
		t.Fatal(err)
	}
	if e.calls[0].body["whenPaused"] != "hold" {
		t.Fatalf("body = %v", e.calls[0].body)
	}
}

func TestNamespace(t *testing.T) {
	c, e := newClient(t, started())
	if err := New(c, Options{Namespace: "staging"}).Enqueue(context.Background(), job, nil); err != nil {
		t.Fatal(err)
	}
	if e.calls[0].path != "/v1/functions/staging-agentenkit-run/start" {
		t.Fatalf("path = %s", e.calls[0].path)
	}
	fns, err := Functions(c, &fakeWorker{}, FunctionOpts{Namespace: "staging"})
	if err != nil {
		t.Fatal(err)
	}
	if fns[0].ID() != "staging-agentenkit-run" || fns[1].ID() != "staging-agentenkit-run-failure" {
		t.Fatalf("functions = %s, %s", fns[0].ID(), fns[1].ID())
	}
}

func TestMaxDepth(t *testing.T) {
	ctx := context.Background()
	counts := func(queued int) any {
		return map[string]any{"queued": queued, "waiting_for_app": 1, "paused": 1, "since": 0}
	}
	c, e := newClient(t, counts(1))
	q := New(c, Options{MaxDepth: 3})
	if err := q.Enqueue(ctx, job, nil); !errors.Is(err, ports.ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
	if len(e.calls) != 1 {
		t.Fatalf("a run was started: %+v", e.calls)
	}

	// Below the cap it starts.
	c, e = newClient(t, counts(0), started())
	if err := New(c, Options{MaxDepth: 3}).Enqueue(ctx, job, nil); err != nil || len(e.calls) != 2 {
		t.Fatalf("err = %v, calls = %d", err, len(e.calls))
	}

	// A retry always goes in, with no count.
	c, e = newClient(t, started())
	retry := job
	retry.Kind = ports.JobRetry
	if err := New(c, Options{MaxDepth: 3}).Enqueue(ctx, retry, nil); err != nil || len(e.calls) != 1 {
		t.Fatalf("err = %v, calls = %d", err, len(e.calls))
	}
}

func TestPauseAndResume(t *testing.T) {
	ctx := context.Background()
	c, e := newClient(t, map[string]any{"paused": true, "runs": 2, "more": false}, map[string]any{"paused": false, "runs": 2, "more": false})
	q := New(c, Options{})
	if err := q.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if e.calls[0].path != "/v1/functions/agentenkit-run/pause" || e.calls[1].path != "/v1/functions/agentenkit-run/resume" {
		t.Fatalf("calls = %+v", e.calls)
	}
}

func TestPurge(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UnixMilli()
	if _, err := New(nil, Options{}).Purge(ctx, ports.PurgeFilter{IncludeDead: true}); err == nil {
		t.Fatal("an empty filter was taken")
	}

	retry := ports.JobRetry
	c, e := newClient(t,
		page("cur",
			runItem("run_a", "queued", now-60_000, 0, map[string]string{"threadId": "t1", "kind": "retry"}),
			runItem("run_new", "queued", now, 0, map[string]string{"threadId": "t1", "kind": "retry"}),
		),
		map[string]any{"cancelled": []string{"run_a"}, "notActive": []string{}},
		page("", runItem("run_b", "sleeping", now-60_000, 0, map[string]string{"threadId": "t1", "kind": ""})),
		page("", runItem("run_f", "failed", now-60_000, now-50_000, map[string]string{"threadId": "t1", "kind": "retry"})),
		map[string]any{"deleted": true},
	)
	n, err := New(c, Options{}).Purge(ctx, ports.PurgeFilter{ThreadID: "t1", Kind: &retry, OlderThan: 30 * time.Second, IncludeDead: true})
	if err != nil || n != 2 {
		t.Fatalf("n = %d, err = %v", n, err)
	}
	if q := e.calls[0].query; q.Get("tag.threadId") != "t1" || q.Get("status") != "queued,sleeping,waiting,waiting_for_app,paused" {
		t.Fatalf("list query = %v", q)
	}
	if e.calls[1].path != "/v1/runs/cancel" || e.calls[1].body["runId"] != "run_a" {
		t.Fatalf("cancel = %+v", e.calls[1])
	}
	if e.calls[2].query.Get("before") != "cur" {
		t.Fatalf("second page = %v", e.calls[2].query)
	}
	if e.calls[4].path != "/v1/runs/run_f/delete" {
		t.Fatalf("delete = %+v", e.calls[4])
	}
}

func TestListDead(t *testing.T) {
	now := time.Now().UnixMilli()
	data, _ := json.Marshal(JobData{Job: job, Key: "d1"})
	c, e := newClient(t,
		page("", runItem("run_f", "failed", now-9000, now-1000, map[string]string{}), runItem("run_g", "failed", now-9000, now-2000, map[string]string{})),
		map[string]any{"id": "run_f", "status": "failed", "createdAt": now - 9000, "endedAt": now - 1000, "tags": map[string]string{},
			"attempts": 4, "error": map[string]any{"name": "Error", "message": "store down"},
			"event": map[string]any{"id": "e", "name": "agentenkit/job", "data": json.RawMessage(data), "ts": 1}},
	)
	dead, err := New(c, Options{}).ListDead(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].ID != "run_f" || dead[0].Attempts != 4 || dead[0].LastError != "store down" ||
		dead[0].Job.RunID != "r1" || dead[0].DeadAt.UnixMilli() != now-1000 {
		t.Fatalf("dead = %+v", dead)
	}
	if e.calls[0].query.Get("status") != "failed" || len(e.calls) != 2 {
		t.Fatalf("calls = %+v", e.calls)
	}
}

func TestRedrive(t *testing.T) {
	detail := func(status string) any {
		return map[string]any{"id": "run_f", "functionId": "agentenkit-run", "status": status, "createdAt": 1, "endedAt": 2,
			"tags": map[string]string{}, "attempts": 1, "error": nil, "event": map[string]any{"id": "e", "name": "n", "data": map[string]any{}, "ts": 1}}
	}
	c, e := newClient(t,
		detail("failed"), map[string]any{"retried": true},
		detail("failed"), map[string]any{"retried": false, "reason": "paused"},
		detail("cancelled"),
	)
	q := New(c, Options{})
	if ok, err := q.Redrive(context.Background(), "run_f"); !ok || err != nil {
		t.Fatalf("ok = %v, err = %v", ok, err)
	}
	if ok, err := q.Redrive(context.Background(), "run_f"); ok || err != nil {
		t.Fatalf("paused: ok = %v, err = %v", ok, err)
	}
	// A job Purge cancelled is not dead: it is not brought back.
	if ok, err := q.Redrive(context.Background(), "run_f"); ok || err != nil {
		t.Fatalf("cancelled: ok = %v, err = %v", ok, err)
	}
	if len(e.calls) != 5 || e.calls[1].path != "/v1/runs/run_f/retry" || e.calls[4].path != "/v1/runs/run_f" {
		t.Fatalf("calls = %+v", e.calls)
	}
}

func TestPurgeFreshDispatchesOnly(t *testing.T) {
	now := time.Now().UnixMilli()
	c, e := newClient(t,
		page("", runItem("run_d", "queued", now, 0, map[string]string{"threadId": "t1", "kind": ""}),
			runItem("run_r", "queued", now, 0, map[string]string{"threadId": "t1", "kind": "retry"})),
		map[string]any{"cancelled": []string{"run_d"}, "notActive": []string{}},
	)
	dispatch := ports.JobDispatch
	n, err := New(c, Options{}).Purge(context.Background(), ports.PurgeFilter{ThreadID: "t1", Kind: &dispatch})
	if err != nil || n != 1 || e.calls[1].body["runId"] != "run_d" {
		t.Fatalf("n = %d, err = %v, calls = %+v", n, err, e.calls)
	}
}

func TestPurgeDead(t *testing.T) {
	now := time.Now().UnixMilli()
	c, e := newClient(t,
		page("", runItem("run_old", "failed", now-7_200_000, now-7_200_000, nil), runItem("run_new", "failed", now-1000, now-1000, nil),
			runItem("run_kept", "failed", now-7_200_000, now-7_200_000, nil)),
		map[string]any{"deleted": true},
		map[string]any{"deleted": false, "reason": "kept"},
	)
	n, err := New(c, Options{}).PurgeDead(context.Background(), time.Hour)
	if err != nil || n != 1 {
		t.Fatalf("n = %d, err = %v", n, err)
	}
	if len(e.calls) != 3 || e.calls[1].path != "/v1/runs/run_old/delete" || e.calls[2].path != "/v1/runs/run_kept/delete" {
		t.Fatalf("calls = %+v", e.calls)
	}
}

func TestSegmentMustBeShorterThanStep(t *testing.T) {
	c, _ := newClient(t)
	if _, err := Functions(c, &fakeWorker{}, FunctionOpts{SegmentTimeout: 60 * time.Minute}); err == nil {
		t.Fatal("a 60 minute segment under a 14 minute step was taken")
	}
	if _, err := Functions(c, &fakeWorker{}, FunctionOpts{ID: "ok", SegmentTimeout: 10 * time.Minute}); err != nil {
		t.Fatal(err)
	}
}

func TestGlobalConcurrency(t *testing.T) {
	c, _ := newClient(t)
	fns, err := Functions(c, &fakeWorker{}, FunctionOpts{Concurrency: 2, GlobalConcurrency: 10})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(fns[0].Config())
	if !strings.Contains(string(got), `"concurrency":[{"limit":2,"key":"event.data.job.partitionKey"},{"limit":10}]`) {
		t.Fatalf("config: %s", got)
	}
}

func TestRetryBackoff(t *testing.T) {
	opts := FunctionOpts{RetryBackoff: time.Second, RetryBackoffMax: 5 * time.Second}
	for attempt, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second} {
		if got := opts.backoff(attempt); got != want {
			t.Errorf("backoff(%d) = %s, want %s", attempt, got, want)
		}
	}

	c, _ := newClient(t)
	w := &fakeWorker{err: errors.New("store down")}
	if _, err := Functions(c, w, FunctionOpts{RetryBackoff: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	before := time.Now().UnixMilli()
	ops := invokeAt(t, c, "agentenkit-run", jobEvent(JobData{Job: job, Key: "d1"}), 2)
	at, _ := ops[0]["retryAt"].(float64)
	if len(ops) != 1 || int64(at) < before+8000 || int64(at) > time.Now().UnixMilli()+8000 {
		t.Fatalf("ops = %v, want a retry about 8s out", ops)
	}

	// Unset: Flow's own backoff.
	c, _ = newClient(t)
	if _, err := Functions(c, w, FunctionOpts{}); err != nil {
		t.Fatal(err)
	}
	if ops := invokeAt(t, c, "agentenkit-run", jobEvent(JobData{Job: job, Key: "d1"}), 2); ops[0]["retryAt"] != nil {
		t.Fatalf("ops = %v", ops)
	}
}
