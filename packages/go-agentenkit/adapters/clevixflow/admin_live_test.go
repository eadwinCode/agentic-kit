package clevixflow

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// liveWorker fails while fail is set, and counts the jobs it ran.
type liveWorker struct {
	fail atomic.Bool
	ran  atomic.Int32
	dead atomic.Int32
}

func (w *liveWorker) Handler() func(context.Context, ports.RunJob) error {
	return func(context.Context, ports.RunJob) error {
		if w.fail.Load() {
			return errors.New("live: failing on purpose")
		}
		w.ran.Add(1)
		return nil
	}
}

func (w *liveWorker) HandleDeadJob(context.Context, ports.RunJob, int, error) { w.dead.Add(1) }

// TestLiveQueueAdmin runs the admin calls on a real engine, with the function
// served from this test. Set TEST_FLOW_SERVE_URL to the address the engine
// reaches this process at, as "http://host:port" (for an engine in Docker:
// http://host.docker.internal:<port>); the test listens on that port.
func TestLiveQueueAdmin(t *testing.T) {
	serveURL := os.Getenv("TEST_FLOW_SERVE_URL")
	if serveURL == "" {
		t.Skip("TEST_FLOW_SERVE_URL not set: skipping")
	}
	c := liveClient(t)
	ctx := context.Background()
	w := &liveWorker{}
	zero := 0
	_, err := Functions(c, w, FunctionOpts{Namespace: "admin-live", Retries: &zero})
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(serveURL[len("http://"):])
	ln, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: c.Serve()}
	go srv.Serve(ln)
	defer srv.Close()
	if status, body, err := c.Sync(ctx, serveURL+"/api/flow"); err != nil || status >= 300 {
		t.Fatalf("sync: %d %s %v", status, body, err)
	}

	q := New(c, Options{Namespace: "admin-live"})
	wait := func(what string, cond func() bool) {
		t.Helper()
		for i := 0; i < 60; i++ {
			if cond() {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	newJob := func(thread string) ports.RunJob {
		id := strconv.FormatInt(time.Now().UnixNano(), 10)
		return ports.RunJob{ThreadID: thread, RunID: "r" + id, DispatchID: "d" + id, Model: "m"}
	}

	// Pause holds a job; Resume runs it.
	if err := q.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, newJob("t-hold"), nil); err != nil {
		t.Fatalf("enqueue while paused: %v", err)
	}
	wait("a held run", func() bool { s, _ := q.Stats(ctx); return s.Paused })
	if w.ran.Load() != 0 {
		t.Fatal("a job ran while paused")
	}

	// Purge cancels a held job by thread.
	if err := q.Enqueue(ctx, newJob("t-purge"), nil); err != nil {
		t.Fatal(err)
	}
	var purged int64
	wait("purge", func() bool {
		n, err := q.Purge(ctx, ports.PurgeFilter{ThreadID: "t-purge"})
		if err != nil {
			t.Fatal(err)
		}
		purged += n
		return purged >= 1
	})
	if err := q.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	wait("the held job to run", func() bool { return w.ran.Load() == 1 })

	// A failing job dies, is listed, redriven, and purged.
	w.fail.Store(true)
	if err := q.Enqueue(ctx, newJob("t-dead"), nil); err != nil {
		t.Fatal(err)
	}
	wait("the dead job handler", func() bool { return w.dead.Load() >= 1 })
	var dead []ports.DeadJob
	wait("ListDead", func() bool {
		dead, _ = q.ListDead(ctx, 1)
		return len(dead) == 1 && dead[0].Job.ThreadID == "t-dead"
	})
	if dead[0].LastError == "" {
		t.Errorf("dead job has no error: %+v", dead[0])
	}
	w.fail.Store(false)
	ok, err := q.Redrive(ctx, dead[0].ID)
	if err != nil || !ok {
		t.Fatalf("redrive = %v, %v", ok, err)
	}
	wait("the redriven job to run", func() bool { return w.ran.Load() == 2 })

	w.fail.Store(true)
	if err := q.Enqueue(ctx, newJob("t-dead2"), nil); err != nil {
		t.Fatal(err)
	}
	wait("a second dead job", func() bool { d, _ := q.ListDead(ctx, 1); return len(d) == 1 && d[0].Job.ThreadID == "t-dead2" })
	n, err := q.PurgeDead(ctx, 0)
	if err != nil || n < 1 {
		t.Fatalf("purge dead = %d, %v", n, err)
	}
}
