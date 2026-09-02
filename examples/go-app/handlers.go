package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// routes is the HTTP contract the React hook expects (its default routes),
// plus the admin reads and two demo endpoints. Each handler is a few lines
// over the runtime: parse, call, encode.
func (a *app) routes(static string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/agent/run", a.run)
	mux.HandleFunc("POST /api/agent/control", a.stop)
	mux.HandleFunc("POST /api/agent/respond", a.respond)
	mux.HandleFunc("GET /api/agent/stream", a.stream)
	mux.HandleFunc("GET /api/agent/history", a.history)
	mux.HandleFunc("GET /api/agent/usage", a.usage)
	// The whole durable log: how the SPA restores custom events from earlier
	// runs, which the snapshot (active run only) does not carry.
	mux.HandleFunc("GET /api/agent/events", a.events)
	mux.HandleFunc("GET /api/threads", a.listThreads)
	mux.HandleFunc("DELETE /api/threads", a.deleteThread)
	// A durable queue (QStash, SQS, …) would POST dispatch tickets here.
	mux.HandleFunc("POST /api/queue/agent-run", a.queueConsumer)

	mux.HandleFunc("GET /api/admin/overview", a.adminOverview)
	mux.HandleFunc("GET /api/admin/runs", a.adminRuns)
	mux.HandleFunc("GET /api/admin/runs/{runId}", a.adminRun)
	mux.HandleFunc("GET /api/admin/threads", a.adminThreads)
	mux.HandleFunc("GET /api/admin/threads/{threadId}", a.adminThread)

	mux.HandleFunc("GET /api/previews/{file}", a.preview)
	// Custom events from OUTSIDE a run: a billing webhook would do this.
	mux.HandleFunc("POST /api/demo/credit-limit", a.demoCreditLimit)
	mux.HandleFunc("DELETE /api/demo/credit-limit", a.demoCreditClear)
	mux.HandleFunc("GET /api/demo/credit", a.demoCredit)

	if static != "" {
		mux.Handle("/", spa(static))
	}
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
}

func decode(r *http.Request, v any) error {
	return json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20)).Decode(v)
}

// run: heal orphans → billing pre-check → persist the user message → RUNNING
// → enqueue. The inline queue then executes on its own goroutine.
func (a *app) run(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ThreadID      string                   `json:"threadId"`
		Prompt        string                   `json:"prompt"`
		Model         string                   `json:"model"`
		EditMessageID string                   `json:"editMessageId"`
		TokenBudget   int                      `json:"tokenBudget"`
		State         agentenkit.AgentRunState `json:"state"`
	}
	if err := decode(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"accepted": false, "error": err.Error()})
		return
	}
	if body.Model == "" {
		body.Model = a.model
	}
	// A run can never spend more than the thread has left: the platform
	// checks the budget between steps and stops with TOKEN_BUDGET_EXHAUSTED.
	budget := body.TokenBudget
	if budget == 0 && body.ThreadID != "" {
		budget = credit.balance(body.ThreadID)
	}
	res, err := a.chat.Run(r.Context(), agentenkit.RunInput{
		ThreadID: body.ThreadID, Prompt: body.Prompt, Model: body.Model,
		EditMessageID: body.EditMessageID, TokenBudget: budget,
		// The run state (§2.10): reaches every storage call, tool and nested run.
		State: body.State,
	})
	if err != nil {
		fail(w, err)
		return
	}
	if !res.Accepted {
		writeJSON(w, http.StatusConflict, res)
		return
	}
	writeJSON(w, http.StatusAccepted, res)
}

func (a *app) stop(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ThreadID string `json:"threadId"`
	}
	if err := decode(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"accepted": false, "error": err.Error()})
		return
	}
	res, err := a.chat.Stop(r.Context(), body.ThreadID, nil)
	if err != nil {
		fail(w, err)
		return
	}
	status := http.StatusOK
	if !res.Accepted {
		status = http.StatusConflict
	}
	writeJSON(w, status, res)
}

func (a *app) respond(w http.ResponseWriter, r *http.Request) {
	var body agentenkit.RespondInput
	if err := decode(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"delivered": false, "error": err.Error()})
		return
	}
	res, err := a.rt.HITL.Respond(r.Context(), body)
	if err != nil {
		fail(w, err)
		return
	}
	status := http.StatusOK
	if !res.Delivered {
		status = http.StatusConflict
	}
	writeJSON(w, status, res)
}

// stream is the SSE distributor. Replay-then-tail lives in the runtime; this
// handler is a cursor and a writer. EventSource sends Last-Event-ID on its own
// reconnects, so a client never replays what it has seen.
func (a *app) stream(w http.ResponseWriter, r *http.Request) {
	threadID := r.URL.Query().Get("threadId")
	if threadID == "" {
		http.Error(w, "threadId is required", http.StatusBadRequest)
		return
	}
	raw := r.Header.Get("Last-Event-ID")
	if raw == "" {
		raw = r.URL.Query().Get("since")
	}
	since, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		since = -1
	}
	// The §2.5 fallback: one call per connection rather than a poll per viewer.
	_, _ = a.rt.HITL.ReclaimIfOrphaned(r.Context(), threadID, nil)

	stream, err := a.rt.Events.SSE(r.Context(), threadID, agentenkit.SSEStateOptions{
		SSEOptions: agentenkit.SSEOptions{FollowOptions: agentenkit.FollowOptions{Since: since}, RetryMs: 2000},
	})
	if err != nil {
		fail(w, err)
		return
	}
	stream.ServeHTTP(w, r) // returns when the client hangs up, and unsubscribes
}

func (a *app) history(w http.ResponseWriter, r *http.Request) {
	threadID := r.URL.Query().Get("threadId")
	if threadID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "threadId is required"})
		return
	}
	snap, err := a.rt.GetThreadSnapshot(r.Context(), threadID, nil)
	if err != nil {
		fail(w, err)
		return
	}
	if snap == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "Thread not found"})
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (a *app) usage(w http.ResponseWriter, r *http.Request) {
	threadID := r.URL.Query().Get("threadId")
	u, err := a.rt.GetThreadUsage(r.Context(), threadID, nil)
	if err != nil {
		fail(w, err)
		return
	}
	if u == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "Thread not found"})
		return
	}
	writeJSON(w, http.StatusOK, u)
}

func (a *app) events(w http.ResponseWriter, r *http.Request) {
	threadID := r.URL.Query().Get("threadId")
	since, err := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	if err != nil {
		since = -1
	}
	events, err := a.rt.Events.Since(r.Context(), threadID, since, nil)
	if err != nil {
		fail(w, err)
		return
	}
	if events == nil {
		events = []agentenkit.AgentEvent{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

type threadListItem struct {
	ID        string                    `json:"id"`
	Title     string                    `json:"title"`
	State     agentenkit.ExecutionState `json:"state"`
	Model     string                    `json:"model"`
	UpdatedAt time.Time                 `json:"updatedAt"`
}

// listThreads: most recent first. The title is the first user turn.
func (a *app) listThreads(w http.ResponseWriter, r *http.Request) {
	threads, err := a.rt.ListThreads(r.Context(), nil)
	if err != nil {
		fail(w, err)
		return
	}
	storage := a.rt.Ports(nil).Storage
	out := make([]threadListItem, 0, len(threads))
	for _, t := range threads {
		item := threadListItem{ID: t.ID, State: t.State, Model: t.Model, UpdatedAt: t.UpdatedAt}
		if msgs, err := storage.Messages.List(r.Context(), t.ID, agentenkit.MainAgent); err == nil {
			for _, m := range msgs {
				if m.Role == agentenkit.RoleUser {
					if parts := agentenkit.ParseContent(m.Content); len(parts) > 0 {
						item.Title = truncate(parts[0].Text, 40)
					}
					break
				}
			}
		}
		out = append(out, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"threads": out})
}

func (a *app) deleteThread(w http.ResponseWriter, r *http.Request) {
	threadID := r.URL.Query().Get("threadId")
	res, err := a.rt.DeleteThread(r.Context(), threadID, nil)
	if err != nil {
		fail(w, err)
		return
	}
	status := http.StatusOK
	switch {
	case res.Accepted:
	case res.Error == "Thread not found":
		status = http.StatusNotFound
	default:
		status = http.StatusConflict
	}
	writeJSON(w, status, res)
}

// queueConsumer accepts a dispatch ticket from an external queue and runs it
// in the background. Delivery is at-least-once; the run lock makes a
// duplicate a no-op. Verify the queue's signature here in production.
func (a *app) queueConsumer(w http.ResponseWriter, r *http.Request) {
	var job agentenkit.RunJob
	if err := decode(r, &job); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"accepted": false, "error": err.Error()})
		return
	}
	go func() { _, _ = a.rt.Worker.HandleJob(r.Context(), job) }()
	writeJSON(w, http.StatusOK, map[string]any{"accepted": true})
}

// ---- admin reads (§2.9): all from the platform's own store ----

func sinceParam(r *http.Request) *time.Time {
	hours, err := strconv.ParseFloat(r.URL.Query().Get("hours"), 64)
	if err != nil || hours <= 0 {
		hours = 24
	}
	t := time.Now().Add(-time.Duration(hours * float64(time.Hour)))
	return &t
}

func statesParam(r *http.Request) []agentenkit.ExecutionState {
	var out []agentenkit.ExecutionState
	for _, s := range r.URL.Query()["state"] {
		out = append(out, agentenkit.ExecutionState(s))
	}
	return out
}

func limitParam(r *http.Request) int {
	n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if n <= 0 {
		n = 100
	}
	return n
}

func (a *app) adminOverview(w http.ResponseWriter, r *http.Request) {
	ov, err := a.rt.Admin.Overview(r.Context(), sinceParam(r))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ov)
}

func (a *app) adminRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := a.rt.Admin.ListRuns(r.Context(), agentenkit.RunFilter{
		State: statesParam(r), Agent: r.URL.Query().Get("agent"), Since: sinceParam(r), Limit: limitParam(r),
	})
	if err != nil {
		fail(w, err)
		return
	}
	if runs == nil {
		runs = []agentenkit.RunRecord{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (a *app) adminRun(w http.ResponseWriter, r *http.Request) {
	detail, err := a.rt.Admin.GetRun(r.Context(), r.PathValue("runId"))
	if err != nil {
		fail(w, err)
		return
	}
	if detail == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "Run not found"})
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (a *app) adminThreads(w http.ResponseWriter, r *http.Request) {
	threads, err := a.rt.Admin.ListThreads(r.Context(), agentenkit.AdminThreadFilter{
		State: statesParam(r), Since: sinceParam(r), Limit: limitParam(r),
	})
	if err != nil {
		fail(w, err)
		return
	}
	if threads == nil {
		threads = []agentenkit.ThreadSummary{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"threads": threads})
}

func (a *app) adminThread(w http.ResponseWriter, r *http.Request) {
	detail, err := a.rt.Admin.GetThread(r.Context(), r.PathValue("threadId"))
	if err != nil {
		fail(w, err)
		return
	}
	if detail == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "Thread not found"})
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

// ---- demo endpoints ----

func (a *app) preview(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(r.PathValue("file"), ".svg")
	svg, ok := a.previews.Load(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_, _ = w.Write([]byte(svg.(string)))
}

// demoCreditLimit is what a billing webhook would do when an account runs
// dry: set the thread's allowance to zero. Nothing else happens here. The
// next message the user sends meets BillingPreCheck, which refuses the run
// and publishes CREDIT_LIMIT on the thread; that is what the chat shows. A
// run already in flight is stopped, since it can no longer be paid for.
func (a *app) demoCreditLimit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ThreadID string `json:"threadId"`
	}
	if err := decode(r, &body); err != nil || body.ThreadID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "threadId is required"})
		return
	}
	credit.set(body.ThreadID, 0)
	stopped := false
	if th, err := a.rt.Ports(nil).Storage.Threads.Get(r.Context(), body.ThreadID); err == nil && th != nil &&
		(th.State == agentenkit.StateRunning || th.State == agentenkit.StateWaitingForInput) {
		res, err := a.chat.Stop(r.Context(), body.ThreadID, nil)
		if err != nil {
			fail(w, err)
			return
		}
		stopped = res.Accepted
	}
	writeJSON(w, http.StatusOK, map[string]any{"remaining": 0, "stoppedRun": stopped})
}

// demoCreditClear is a top-up, or a new billing period: the allowance is
// back, and the thread learns it through CREDIT_RESTORED.
func (a *app) demoCreditClear(w http.ResponseWriter, r *http.Request) {
	threadID := r.URL.Query().Get("threadId")
	if threadID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "threadId is required"})
		return
	}
	credit.set(threadID, creditAllowance)
	event, err := a.rt.Events.PublishEvent(r.Context(), threadID, "CREDIT_RESTORED",
		map[string]any{"kind": "monthly", "remaining": creditAllowance}, agentenkit.PublishStateOptions{})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"remaining": creditAllowance, "event": event})
}

// demoCredit reports the thread's remaining allowance.
func (a *app) demoCredit(w http.ResponseWriter, r *http.Request) {
	threadID := r.URL.Query().Get("threadId")
	writeJSON(w, http.StatusOK, map[string]any{"remaining": credit.balance(threadID), "allowance": creditAllowance})
}

// spa serves the built React app, falling back to index.html for client-side
// routes. The dev server (vite) proxies /api here instead.
func spa(dir string) http.Handler {
	fs := http.FileServer(http.Dir(dir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Join(dir, filepath.Clean(r.URL.Path))
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			fs.ServeHTTP(w, r)
			return
		}
		index := filepath.Join(dir, "index.html")
		if _, err := os.Stat(index); errors.Is(err, os.ErrNotExist) {
			http.Error(w, "SPA not built: run `bun run build` in examples/go-app/web", http.StatusNotFound)
			return
		}
		http.ServeFile(w, r, index)
	})
}
