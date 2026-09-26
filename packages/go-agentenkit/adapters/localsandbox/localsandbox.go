// Package localsandbox is a sandbox that is only a folder on this machine:
// for development, where you want the sandbox tools without Docker or an
// account. Commands run as you, with your files in reach. There is NO
// isolation, and the network setting is not applied, so New warns when
// GO_ENV is production.
//
// Commands see only PATH, HOME (the sandbox folder) and LANG from your
// environment, so your API keys do not leak into what the model runs. The
// TS runtime has the same adapter (LocalSandbox).
package localsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/internal/sandboxsh"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Options shapes the provider.
type Options struct {
	// RootDir is where the sandboxes' folders go. Default:
	// agentenkit-sandboxes in the system temp folder.
	RootDir string
	// Env holds environment variables every command sees, on top of PATH,
	// HOME and LANG.
	Env map[string]string
	// Log gets the production warning. Nil uses slog.Default().
	Log *slog.Logger
}

// Provider makes local sandboxes.
type Provider struct {
	root string
	env  map[string]string
}

var idPattern = regexp.MustCompile(`^local-[a-z0-9]+$`)

// New returns the provider.
func New(opts Options) *Provider {
	root := opts.RootDir
	if root == "" {
		root = filepath.Join(os.TempDir(), "agentenkit-sandboxes")
	}
	if os.Getenv("GO_ENV") == "production" {
		log := opts.Log
		if log == nil {
			log = slog.Default()
		}
		log.Warn("LocalSandbox runs commands on this machine with no isolation: use the docker or e2b adapter in production")
	}
	return &Provider{root: root, env: opts.Env}
}

// Name implements ports.SandboxProvider.
func (p *Provider) Name() string { return "local" }

type meta struct {
	CreatedAt int64             `json:"createdAt"`
	ExpiresAt int64             `json:"expiresAt"`
	Metadata  map[string]string `json:"metadata"`
	Envs      map[string]string `json:"envs"`
}

// Create implements ports.SandboxProvider.
func (p *Provider) Create(_ context.Context, opts ports.CreateSandboxOptions) (ports.Sandbox, error) {
	p.sweep()
	id := "local-" + sandboxsh.RandomID(12)
	dir := filepath.Join(p.root, id)
	if err := os.MkdirAll(filepath.Join(dir, "work"), 0o755); err != nil {
		return nil, err
	}
	now := time.Now()
	md := map[string]string{"threadId": opts.Metadata.ThreadID}
	if opts.Metadata.RunID != "" {
		md["runId"] = opts.Metadata.RunID
	}
	envs := opts.Envs
	if envs == nil {
		envs = map[string]string{}
	}
	m := meta{CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(opts.Timeout).UnixMilli(), Metadata: md, Envs: envs}
	if err := writeMeta(dir, m); err != nil {
		return nil, err
	}
	return p.box(id, dir), nil
}

// Connect implements ports.SandboxProvider.
func (p *Provider) Connect(_ context.Context, sandboxID string) (ports.Sandbox, error) {
	if !idPattern.MatchString(sandboxID) {
		return nil, ports.SandboxGone(sandboxID, "not a local sandbox id")
	}
	dir := filepath.Join(p.root, sandboxID)
	m, ok := readMeta(dir)
	if !ok {
		return nil, ports.SandboxGone(sandboxID, "")
	}
	if m.ExpiresAt <= time.Now().UnixMilli() {
		_ = os.RemoveAll(dir)
		return nil, ports.SandboxGone(sandboxID, "it timed out")
	}
	return p.box(sandboxID, dir), nil
}

func (p *Provider) box(id, dir string) *box {
	b := &box{id: id, dir: dir, workdir: filepath.Join(dir, "work"), env: p.env}
	b.files = &files{b: b}
	return b
}

// sweep removes sandboxes past their time. There is no process watching
// them, so each Create tidies up after the others.
func (p *Provider) sweep() {
	entries, err := os.ReadDir(p.root)
	if err != nil {
		return
	}
	now := time.Now().UnixMilli()
	for _, e := range entries {
		if !idPattern.MatchString(e.Name()) {
			continue
		}
		dir := filepath.Join(p.root, e.Name())
		if m, ok := readMeta(dir); ok && m.ExpiresAt <= now {
			_ = os.RemoveAll(dir)
		}
	}
}

func readMeta(dir string) (meta, bool) {
	var m meta
	raw, err := os.ReadFile(filepath.Join(dir, "sandbox.json"))
	if err != nil || json.Unmarshal(raw, &m) != nil {
		return m, false
	}
	return m, true
}

func writeMeta(dir string, m meta) error {
	raw, _ := json.Marshal(m)
	return os.WriteFile(filepath.Join(dir, "sandbox.json"), raw, 0o644)
}

type box struct {
	id, dir, workdir string
	env              map[string]string
	files            *files
}

func (b *box) ID() string                          { return b.id }
func (b *box) Provider() string                    { return "local" }
func (b *box) Filesystem() ports.SandboxFileSystem { return b.files }

func (b *box) meta() (meta, error) {
	m, ok := readMeta(b.dir)
	if !ok {
		return m, ports.SandboxGone(b.id, "")
	}
	return m, nil
}

func (b *box) path(p string) string { return sandboxsh.ResolveIn(b.workdir, p) }

func (b *box) RunCommand(ctx context.Context, command string, opts ports.RunCommandOptions) (ports.CommandResult, error) {
	m, err := b.meta()
	if err != nil {
		return ports.CommandResult{}, err
	}
	cwd := opts.Cwd
	if cwd == "" {
		cwd = "."
	}
	env := map[string]string{"PATH": os.Getenv("PATH"), "HOME": b.workdir, "LANG": os.Getenv("LANG")}
	if env["PATH"] == "" {
		env["PATH"] = "/usr/local/bin:/usr/bin:/bin"
	}
	if env["LANG"] == "" {
		env["LANG"] = "C.UTF-8"
	}
	for _, extra := range []map[string]string{b.env, m.Envs, opts.Env} {
		for k, v := range extra {
			env[k] = v
		}
	}
	if opts.Background {
		r, err := b.exec(ctx, sandboxsh.BackgroundCommand(command), b.path(cwd), env, ports.DefaultCommandTimeout, nil, nil)
		r.Stdout, r.Stderr = "", ""
		return r, err
	}
	limit := opts.Timeout
	if limit <= 0 {
		limit = ports.DefaultCommandTimeout
	}
	return b.exec(ctx, command, b.path(cwd), env, limit, opts.OnStdout, opts.OnStderr)
}

func (b *box) exec(ctx context.Context, command, cwd string, env map[string]string, limit time.Duration, onOut, onErr func(string)) (ports.CommandResult, error) {
	started := time.Now()
	argv := sandboxsh.RunArgv(command, 0, false)
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = cwd
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, errOut := sandboxsh.NewOutput(onOut), sandboxsh.NewOutput(onErr)
	cmd.Stdout, cmd.Stderr = out, errOut
	ownGroup(cmd)
	if err := cmd.Start(); err != nil {
		return ports.CommandResult{}, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(limit)
	defer timer.Stop()
	timedOut := false
	var waitErr error
	select {
	case waitErr = <-done:
	case <-timer.C:
		timedOut = true
		killGroup(cmd)
		waitErr = <-done
	case <-ctx.Done():
		killGroup(cmd)
		<-done
		return ports.CommandResult{}, ctx.Err()
	}
	code := 0
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		code = exitErr.ExitCode()
		if code < 0 {
			code = 128 + signalOf(exitErr)
		}
	} else if waitErr != nil {
		return ports.CommandResult{}, waitErr
	}
	if timedOut {
		code = ports.TimedOutExitCode
	}
	return ports.CommandResult{
		Stdout:     out.String(),
		Stderr:     errOut.String(),
		ExitCode:   code,
		DurationMs: time.Since(started).Milliseconds(),
		TimedOut:   timedOut,
		Truncated:  out.Truncated || errOut.Truncated,
	}, nil
}

func (b *box) GetURL(_ context.Context, port int, protocol string) (string, error) {
	if _, err := b.meta(); err != nil {
		return "", err
	}
	if protocol == "" {
		protocol = "http"
	}
	return fmt.Sprintf("%s://localhost:%d", protocol, port), nil
}

func (b *box) GetInfo(context.Context) (ports.SandboxInfo, error) {
	m, err := b.meta()
	if err != nil {
		return ports.SandboxInfo{}, err
	}
	return ports.SandboxInfo{
		ID: b.id, Provider: "local", Status: "running",
		CreatedAt: time.UnixMilli(m.CreatedAt), ExpiresAt: time.UnixMilli(m.ExpiresAt),
		Workdir: b.workdir, Metadata: m.Metadata,
	}, nil
}

func (b *box) SetTimeout(_ context.Context, d time.Duration) error {
	m, err := b.meta()
	if err != nil {
		return err
	}
	m.ExpiresAt = time.Now().Add(d).UnixMilli()
	return writeMeta(b.dir, m)
}

func (b *box) Destroy(context.Context) error { return os.RemoveAll(b.dir) }

type files struct{ b *box }

func missing(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, os.ErrNotExist) || isNotDir(err)
}

func (f *files) ReadFile(ctx context.Context, p string) (string, error) {
	b, err := f.ReadFileBytes(ctx, p)
	return string(b), err
}

func (f *files) ReadFileBytes(_ context.Context, p string) ([]byte, error) {
	full := f.b.path(p)
	st, err := os.Stat(full)
	if err != nil {
		if missing(err) {
			return nil, ports.SandboxFileNotFound(p)
		}
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, ports.SandboxFileNotFound(p)
	}
	return os.ReadFile(full)
}

func (f *files) WriteFile(_ context.Context, p string, content []byte) error {
	full := f.b.path(p)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	return os.WriteFile(full, content, 0o644)
}

func (f *files) ReadDir(_ context.Context, p string) ([]ports.FileEntry, error) {
	full := f.b.path(p)
	dirents, err := os.ReadDir(full)
	if err != nil {
		if missing(err) {
			return nil, ports.SandboxFileNotFound(p)
		}
		return nil, err
	}
	entries := []ports.FileEntry{}
	for _, d := range dirents {
		if d.IsDir() {
			entries = append(entries, ports.FileEntry{Name: d.Name(), Type: "directory"})
			continue
		}
		var size int64
		if st, err := os.Stat(filepath.Join(full, d.Name())); err == nil {
			size = st.Size()
		}
		entries = append(entries, ports.FileEntry{Name: d.Name(), Type: "file", Size: &size})
	}
	return sandboxsh.SortEntries(entries), nil
}

func (f *files) Mkdir(_ context.Context, p string) error { return os.MkdirAll(f.b.path(p), 0o755) }

func (f *files) Exists(_ context.Context, p string) (bool, error) {
	_, err := os.Lstat(f.b.path(p))
	if err == nil {
		return true, nil
	}
	if missing(err) {
		return false, nil
	}
	return false, err
}

func (f *files) Remove(_ context.Context, p string) error { return os.RemoveAll(f.b.path(p)) }
