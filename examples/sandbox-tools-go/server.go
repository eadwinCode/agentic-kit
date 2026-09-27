package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// The HTTP contract the React hook expects (its default routes), plus
// /api/info for the shared page, and the page itself. Each handler is a few
// lines over the runtime: parse, call, encode. web-tools-go has the same
// file.

type server struct {
	rt   *agentenkit.AgentCore
	chat *agentenkit.AgentHandle
	info any
}

func (s *server) routes(ui string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/info", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, s.info) })
	mux.HandleFunc("POST /api/agent/run", s.run)
	mux.HandleFunc("POST /api/agent/control", s.stop)
	mux.HandleFunc("POST /api/agent/respond", s.respond)
	mux.HandleFunc("GET /api/agent/stream", s.stream)
	mux.HandleFunc("GET /api/agent/history", s.history)
	mux.HandleFunc("GET /api/agent/usage", s.usage)
	mux.HandleFunc("GET /api/threads", s.listThreads)
	mux.HandleFunc("DELETE /api/threads", s.deleteThread)
	mux.Handle("/", page(ui))
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

func statusIf(ok bool) int {
	if ok {
		return http.StatusOK
	}
	return http.StatusConflict
}

func (s *server) run(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ThreadID        string `json:"threadId"`
		Prompt          string `json:"prompt"`
		EditMessageID   string `json:"editMessageId"`
		ClientMessageID string `json:"clientMessageId"`
	}
	if err := decode(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"accepted": false, "error": err.Error()})
		return
	}
	res, err := s.chat.Run(r.Context(), agentenkit.RunInput{
		ThreadID: body.ThreadID, Prompt: body.Prompt,
		EditMessageID: body.EditMessageID, ClientMessageID: body.ClientMessageID,
	})
	if err != nil {
		fail(w, err)
		return
	}
	status := http.StatusAccepted
	if !res.Accepted {
		status = http.StatusConflict
	}
	writeJSON(w, status, res)
}

func (s *server) stop(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ThreadID string `json:"threadId"`
	}
	if err := decode(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"accepted": false, "error": err.Error()})
		return
	}
	res, err := s.chat.Stop(r.Context(), body.ThreadID, nil)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, statusIf(res.Accepted), res)
}

func (s *server) respond(w http.ResponseWriter, r *http.Request) {
	var body agentenkit.RespondInput
	if err := decode(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"delivered": false, "error": err.Error()})
		return
	}
	res, err := s.rt.HITL.Respond(r.Context(), body)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, statusIf(res.Delivered), res)
}

// stream is the SSE feed: the thread record from the cursor, then the run
// stream. EventSource sends Last-Event-ID on its own reconnects; the hook
// puts the cursor in the query when it opens the stream.
func (s *server) stream(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	threadID := q.Get("threadId")
	cursor := r.Header.Get("Last-Event-ID")
	if cursor == "" {
		cursor = q.Get("cursor")
	}
	_, _ = s.rt.HITL.ReclaimIfOrphaned(r.Context(), threadID, nil)
	stream, err := s.rt.Events.SSE(r.Context(), threadID, agentenkit.SSEStateOptions{
		FollowStateOptions: agentenkit.FollowStateOptions{Cursor: cursor, LastMessageID: q.Get("lastMessageId")},
		RetryMs:            2000,
	})
	if err != nil {
		fail(w, err)
		return
	}
	stream.ServeHTTP(w, r)
}

func (s *server) history(w http.ResponseWriter, r *http.Request) {
	snap, err := s.rt.GetThreadSnapshot(r.Context(), r.URL.Query().Get("threadId"), nil)
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

func (s *server) usage(w http.ResponseWriter, r *http.Request) {
	u, err := s.rt.GetThreadUsage(r.Context(), r.URL.Query().Get("threadId"), nil)
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

type threadItem struct {
	ID        string                    `json:"id"`
	Title     string                    `json:"title"`
	State     agentenkit.ExecutionState `json:"state"`
	Model     string                    `json:"model"`
	UpdatedAt time.Time                 `json:"updatedAt"`
}

// listThreads: newest first, titled by each thread's first message.
func (s *server) listThreads(w http.ResponseWriter, r *http.Request) {
	threads, err := s.rt.ListThreads(r.Context(), nil)
	if err != nil {
		fail(w, err)
		return
	}
	storage := s.rt.Ports(nil).Storage
	out := make([]threadItem, 0, len(threads))
	for _, t := range threads {
		item := threadItem{ID: t.ID, State: t.State, Model: t.Model, UpdatedAt: t.UpdatedAt}
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

func (s *server) deleteThread(w http.ResponseWriter, r *http.Request) {
	res, err := s.rt.DeleteThread(r.Context(), r.URL.Query().Get("threadId"), nil)
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

// page serves the shared page's build: a file when there is one, else
// index.html.
func page(dir string) http.Handler {
	files := http.FileServer(http.Dir(dir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if info, err := os.Stat(filepath.Join(dir, filepath.Clean(r.URL.Path))); err == nil && !info.IsDir() {
			files.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(dir, "index.html"))
	})
}

func truncate(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

// loadDotEnv reads KEY=VALUE lines from a .env file into the environment,
// for keys not already set, so a shell export still wins. A missing file
// does nothing.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) || err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimPrefix(strings.TrimSpace(sc.Text()), "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok || strings.HasPrefix(line, "#") {
			continue
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		if _, set := os.LookupEnv(key); !set && key != "" {
			os.Setenv(key, value)
		}
	}
}
