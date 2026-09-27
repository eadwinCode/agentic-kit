//go:build !windows

package agentenkit_test

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/e2b"
)

// A stand-in for E2B's API and the daemon in each sandbox (envd), speaking
// the same requests: enough to run the sandbox suite against the e2b
// adapter with no account. Each sandbox is a temp folder; /home/user is
// that folder. The TS package has the same fake (test/e2b-fake.ts).

const fakeE2BToken = "fake-token"

type fakeBox struct {
	dir       string
	startedAt time.Time
	endAt     time.Time
	metadata  map[string]string
	envs      map[string]string
	procs     map[int]*exec.Cmd
}

type fakeE2B struct {
	url        string
	mu         sync.Mutex
	boxes      map[string]*fakeBox
	n          int
	lastCreate map[string]any
}

func (f *fakeE2B) lastCreateBody() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastCreate
}

func startFakeE2B(t *testing.T) *fakeE2B {
	t.Helper()
	root := t.TempDir()
	f := &fakeE2B{boxes: map[string]*fakeBox{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.serve(root, w, r) }))
	t.Cleanup(func() {
		f.mu.Lock()
		for _, b := range f.boxes {
			for _, c := range b.procs {
				_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
			}
		}
		f.mu.Unlock()
		srv.CloseClientConnections()
		srv.Close()
	})
	f.url = srv.URL
	return f
}

var (
	connectPath = regexp.MustCompile(`^/v2/sandboxes/([^/]+)/connect$`)
	timeoutPath = regexp.MustCompile(`^/sandboxes/([^/]+)/timeout$`)
	boxPath     = regexp.MustCompile(`^/sandboxes/([^/]+)$`)
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeE2B) box(id string) *fakeBox {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.boxes[id]
}

func (b *fakeBox) mapPath(p string) string {
	if strings.HasPrefix(p, "/home/user") {
		return filepath.Join(b.dir, strings.TrimPrefix(p, "/home/user"))
	}
	return filepath.Join(b.dir, "__root", p)
}

func (f *fakeE2B) serve(root string, w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	created := func(id string) map[string]any {
		return map[string]any{"sandboxID": id, "domain": nil, "envdVersion": "0.4.0", "envdAccessToken": fakeE2BToken}
	}

	// --- the API ---
	switch {
	case r.Method == http.MethodPost && path == "/v2/sandboxes":
		if r.Header.Get("X-API-Key") != "test-key" {
			writeJSON(w, 401, map[string]any{"message": "bad key"})
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.lastCreate = body
		f.n++
		id := fmt.Sprintf("fake%d", f.n)
		dir := filepath.Join(root, id)
		_ = os.MkdirAll(dir, 0o755)
		md := map[string]string{}
		if m, ok := body["metadata"].(map[string]any); ok {
			for k, v := range m {
				md[k] = fmt.Sprint(v)
			}
		}
		envs := map[string]string{}
		if m, ok := body["envVars"].(map[string]any); ok {
			for k, v := range m {
				envs[k] = fmt.Sprint(v)
			}
		}
		secs, _ := body["timeout"].(float64)
		f.boxes[id] = &fakeBox{dir: dir, startedAt: time.Now(), endAt: time.Now().Add(time.Duration(secs) * time.Second), metadata: md, envs: envs, procs: map[int]*exec.Cmd{}}
		f.mu.Unlock()
		writeJSON(w, 201, created(id))
		return
	case r.Method == http.MethodPost && connectPath.MatchString(path):
		id := connectPath.FindStringSubmatch(path)[1]
		if f.box(id) == nil {
			writeJSON(w, 404, map[string]any{"message": "not found"})
			return
		}
		writeJSON(w, 200, created(id))
		return
	case r.Method == http.MethodPost && timeoutPath.MatchString(path):
		b := f.box(timeoutPath.FindStringSubmatch(path)[1])
		if b == nil {
			writeJSON(w, 404, map[string]any{"message": "not found"})
			return
		}
		var body struct{ Timeout int }
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		b.endAt = time.Now().Add(time.Duration(body.Timeout) * time.Second)
		f.mu.Unlock()
		w.WriteHeader(204)
		return
	case boxPath.MatchString(path):
		id := boxPath.FindStringSubmatch(path)[1]
		b := f.box(id)
		if b == nil {
			writeJSON(w, 404, map[string]any{"message": "not found"})
			return
		}
		if r.Method == http.MethodDelete {
			f.mu.Lock()
			delete(f.boxes, id)
			for _, c := range b.procs {
				_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
			}
			f.mu.Unlock()
			_ = os.RemoveAll(b.dir)
			w.WriteHeader(204)
			return
		}
		f.mu.Lock()
		info := map[string]any{"sandboxID": id, "templateID": "base", "startedAt": b.startedAt.Format(time.RFC3339Nano),
			"endAt": b.endAt.Format(time.RFC3339Nano), "metadata": b.metadata, "state": "running"}
		f.mu.Unlock()
		writeJSON(w, 200, info)
		return
	}

	// --- envd, reached through SandboxURL with the sandbox in a header ---
	b := f.box(r.Header.Get("E2b-Sandbox-Id"))
	if b == nil {
		http.Error(w, "sandbox not found", 502)
		return
	}
	if r.Header.Get("X-Access-Token") != fakeE2BToken {
		writeJSON(w, 401, map[string]any{"message": "bad token"})
		return
	}
	rpcError := func(code string, status int) { writeJSON(w, status, map[string]any{"code": code, "message": code}) }

	switch {
	case path == "/files":
		file := b.mapPath(r.URL.Query().Get("path"))
		if r.Method == http.MethodGet {
			st, err := os.Stat(file)
			if err != nil || !st.Mode().IsRegular() {
				writeJSON(w, 404, map[string]any{"code": 404, "message": "file not found"})
				return
			}
			data, _ := os.ReadFile(file)
			_, _ = w.Write(data)
			return
		}
		part, _, err := r.FormFile("file")
		if err != nil {
			writeJSON(w, 400, map[string]any{"message": err.Error()})
			return
		}
		data, _ := io.ReadAll(part)
		_ = os.MkdirAll(filepath.Dir(file), 0o755)
		_ = os.WriteFile(file, data, 0o644)
		writeJSON(w, 200, []any{map[string]any{"name": filepath.Base(file), "type": "file"}})
	case strings.HasPrefix(path, "/filesystem.Filesystem/"):
		var body struct{ Path string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		target := b.mapPath(body.Path)
		st, statErr := os.Stat(target)
		switch strings.TrimPrefix(path, "/filesystem.Filesystem/") {
		case "ListDir":
			if statErr != nil || !st.IsDir() {
				rpcError("not_found", 404)
				return
			}
			entries := []any{}
			dirents, _ := os.ReadDir(target)
			for _, d := range dirents {
				kind, size := "FILE_TYPE_FILE", int64(0)
				if d.IsDir() {
					kind = "FILE_TYPE_DIRECTORY"
				} else if info, err := d.Info(); err == nil {
					size = info.Size()
				}
				entries = append(entries, map[string]any{"name": d.Name(), "type": kind, "size": fmt.Sprint(size)})
			}
			writeJSON(w, 200, map[string]any{"entries": entries})
		case "MakeDir":
			if statErr == nil {
				rpcError("already_exists", 409)
				return
			}
			_ = os.MkdirAll(target, 0o755)
			writeJSON(w, 200, map[string]any{})
		case "Stat":
			if statErr != nil {
				rpcError("not_found", 404)
				return
			}
			writeJSON(w, 200, map[string]any{"entry": map[string]any{"name": body.Path}})
		case "Remove":
			if statErr != nil {
				rpcError("not_found", 404)
				return
			}
			_ = os.RemoveAll(target)
			writeJSON(w, 200, map[string]any{})
		}
	case path == "/process.Process/SendSignal":
		var body struct {
			Process struct{ Pid int }
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		c := b.procs[body.Process.Pid]
		f.mu.Unlock()
		if c != nil {
			_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
		}
		writeJSON(w, 200, map[string]any{})
	case path == "/process.Process/Start":
		if r.Header.Get("Content-Type") != "application/connect+json" {
			writeJSON(w, 415, map[string]any{"message": "want connect+json"})
			return
		}
		raw, _ := io.ReadAll(r.Body)
		n := binary.BigEndian.Uint32(raw[1:5])
		var start struct {
			Process struct {
				Cmd  string
				Args []string
				Cwd  string
				Envs map[string]string
			}
		}
		_ = json.Unmarshal(raw[5:5+n], &start)
		p := start.Process
		cmd := exec.Command(p.Cmd, p.Args...)
		cmd.Dir = b.mapPath(p.Cwd)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + b.dir}
		for _, env := range []map[string]string{b.envs, p.Envs} {
			for k, v := range env {
				cmd.Env = append(cmd.Env, k+"="+v)
			}
		}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()
		if err := cmd.Start(); err != nil {
			writeJSON(w, 500, map[string]any{"message": err.Error()})
			return
		}
		f.mu.Lock()
		b.procs[cmd.Process.Pid] = cmd
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/connect+json")
		flusher := w.(http.Flusher)
		var wmu sync.Mutex
		send := func(message any, flags byte) {
			wmu.Lock()
			defer wmu.Unlock()
			_, _ = w.Write(e2b.Envelope(message, flags))
			flusher.Flush()
		}
		send(map[string]any{"event": map[string]any{"start": map[string]any{"pid": cmd.Process.Pid}}}, 0)
		var wg sync.WaitGroup
		pump := func(r io.Reader, key string) {
			defer wg.Done()
			buf := make([]byte, 32*1024)
			for {
				n, err := r.Read(buf)
				if n > 0 {
					send(map[string]any{"event": map[string]any{"data": map[string]any{key: base64.StdEncoding.EncodeToString(buf[:n])}}}, 0)
				}
				if err != nil {
					return
				}
			}
		}
		wg.Add(2)
		go pump(stdout, "stdout")
		go pump(stderr, "stderr")
		wg.Wait()
		err := cmd.Wait()
		f.mu.Lock()
		delete(b.procs, cmd.Process.Pid)
		f.mu.Unlock()
		// Protobuf JSON leaves a zero out, as envd does.
		end := map[string]any{"exited": true}
		if err != nil {
			code := cmd.ProcessState.ExitCode()
			end = map[string]any{"exitCode": code, "exited": code >= 0}
		}
		send(map[string]any{"event": map[string]any{"end": end}}, 0)
		send(map[string]any{}, 2)
	default:
		writeJSON(w, 404, map[string]any{"message": "no route " + path})
	}
}
