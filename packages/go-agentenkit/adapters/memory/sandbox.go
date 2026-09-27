package memory

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// SandboxRun answers one command for a memory Sandbox.
type SandboxRun func(command string, opts ports.RunCommandOptions, sandbox ports.Sandbox) (ports.CommandResult, error)

// SandboxTimeout is one SetTimeout call a memory Sandbox saw.
type SandboxTimeout struct {
	SandboxID string
	Timeout   time.Duration
}

// SandboxCommand is one command a memory Sandbox was asked to run.
type SandboxCommand struct {
	SandboxID string
	Command   string
	Options   ports.RunCommandOptions
}

// Sandbox makes sandboxes that live in memory, for tests: files are kept,
// commands go to the run function you give (nil fails every command with
// 127). It records what was made, pushed back and destroyed, and End makes
// one go away as a timed-out sandbox would.
type Sandbox struct {
	name string
	run  SandboxRun

	mu        sync.Mutex
	boxes     map[string]*memBox
	n         int
	created   []ports.CreateSandboxOptions
	destroyed []string
	timeouts  []SandboxTimeout
	commands  []SandboxCommand
}

// NewSandbox returns a memory sandbox provider. Name defaults to "memory".
func NewSandbox(run SandboxRun, name string) *Sandbox {
	if name == "" {
		name = "memory"
	}
	return &Sandbox{name: name, run: run, boxes: map[string]*memBox{}}
}

// Name implements ports.SandboxProvider.
func (s *Sandbox) Name() string { return s.name }

// Create implements ports.SandboxProvider.
func (s *Sandbox) Create(_ context.Context, opts ports.CreateSandboxOptions) (ports.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created = append(s.created, opts)
	s.n++
	b := &memBox{owner: s, id: fmt.Sprintf("mem-%d", s.n), files: map[string][]byte{}, dirs: map[string]bool{"/work": true}, createdAt: time.Now()}
	s.boxes[b.id] = b
	return b, nil
}

// Connect implements ports.SandboxProvider.
func (s *Sandbox) Connect(_ context.Context, id string) (ports.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.boxes[id]
	if !ok {
		return nil, ports.SandboxGone(id, "")
	}
	return b, nil
}

// End makes the sandbox go away on its own, as one past its time does.
func (s *Sandbox) End(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.boxes, id)
}

// Live is the ids of the sandboxes still there.
func (s *Sandbox) Live() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.boxes))
	for id := range s.boxes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Created is every Create call's options, in order.
func (s *Sandbox) Created() []ports.CreateSandboxOptions {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.CreateSandboxOptions(nil), s.created...)
}

// Destroyed is the ids destroyed, in order.
func (s *Sandbox) Destroyed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.destroyed...)
}

// Timeouts is every SetTimeout call, in order.
func (s *Sandbox) Timeouts() []SandboxTimeout {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SandboxTimeout(nil), s.timeouts...)
}

// Commands is every command run, in order.
func (s *Sandbox) Commands() []SandboxCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SandboxCommand(nil), s.commands...)
}

func (s *Sandbox) alive(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.boxes[id]; !ok {
		return ports.SandboxGone(id, "")
	}
	return nil
}

type memBox struct {
	owner     *Sandbox
	id        string
	mu        sync.Mutex
	files     map[string][]byte
	dirs      map[string]bool
	createdAt time.Time
	expiresAt time.Time
}

func (b *memBox) ID() string                          { return b.id }
func (b *memBox) Provider() string                    { return b.owner.name }
func (b *memBox) Filesystem() ports.SandboxFileSystem { return memFiles{b} }

func (b *memBox) RunCommand(_ context.Context, command string, opts ports.RunCommandOptions) (ports.CommandResult, error) {
	if err := b.owner.alive(b.id); err != nil {
		return ports.CommandResult{}, err
	}
	b.owner.mu.Lock()
	b.owner.commands = append(b.owner.commands, SandboxCommand{SandboxID: b.id, Command: command, Options: opts})
	b.owner.mu.Unlock()
	if b.owner.run == nil {
		return ports.CommandResult{Stderr: "memory sandbox: " + command + ": not found", ExitCode: 127}, nil
	}
	return b.owner.run(command, opts, b)
}

func (b *memBox) GetURL(_ context.Context, port int, protocol string) (string, error) {
	if err := b.owner.alive(b.id); err != nil {
		return "", err
	}
	if protocol == "" {
		protocol = "http"
	}
	return fmt.Sprintf("%s://%s.memory:%d", protocol, b.id, port), nil
}

func (b *memBox) GetInfo(context.Context) (ports.SandboxInfo, error) {
	if err := b.owner.alive(b.id); err != nil {
		return ports.SandboxInfo{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return ports.SandboxInfo{ID: b.id, Provider: b.owner.name, Status: "running", CreatedAt: b.createdAt, ExpiresAt: b.expiresAt, Workdir: "/work"}, nil
}

func (b *memBox) SetTimeout(_ context.Context, d time.Duration) error {
	if err := b.owner.alive(b.id); err != nil {
		return err
	}
	b.owner.mu.Lock()
	b.owner.timeouts = append(b.owner.timeouts, SandboxTimeout{SandboxID: b.id, Timeout: d})
	b.owner.mu.Unlock()
	b.mu.Lock()
	b.expiresAt = time.Now().Add(d)
	b.mu.Unlock()
	return nil
}

func (b *memBox) Destroy(context.Context) error {
	b.owner.mu.Lock()
	defer b.owner.mu.Unlock()
	if _, ok := b.owner.boxes[b.id]; ok {
		delete(b.owner.boxes, b.id)
		b.owner.destroyed = append(b.owner.destroyed, b.id)
	}
	return nil
}

type memFiles struct{ b *memBox }

func (f memFiles) path(p string) (string, error) {
	if err := f.b.owner.alive(f.b.id); err != nil {
		return "", err
	}
	full := p
	if !strings.HasPrefix(p, "/") {
		full = path.Join("/work", p)
	}
	return path.Clean(full), nil
}

func (f memFiles) addParents(p string) {
	for d := path.Dir(p); d != "/"; d = path.Dir(d) {
		f.b.dirs[d] = true
	}
}

func (f memFiles) ReadFile(ctx context.Context, p string) (string, error) {
	b, err := f.ReadFileBytes(ctx, p)
	return string(b), err
}

func (f memFiles) ReadFileBytes(_ context.Context, p string) ([]byte, error) {
	full, err := f.path(p)
	if err != nil {
		return nil, err
	}
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	b, ok := f.b.files[full]
	if !ok {
		return nil, ports.SandboxFileNotFound(p)
	}
	return append([]byte(nil), b...), nil
}

func (f memFiles) WriteFile(_ context.Context, p string, content []byte) error {
	full, err := f.path(p)
	if err != nil {
		return err
	}
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	f.addParents(full)
	f.b.files[full] = append([]byte{}, content...)
	return nil
}

func (f memFiles) ReadDir(_ context.Context, p string) ([]ports.FileEntry, error) {
	full, err := f.path(p)
	if err != nil {
		return nil, err
	}
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	if !f.b.dirs[full] {
		return nil, ports.SandboxFileNotFound(p)
	}
	entries := []ports.FileEntry{}
	for d := range f.b.dirs {
		if d != full && path.Dir(d) == full {
			entries = append(entries, ports.FileEntry{Name: path.Base(d), Type: "directory"})
		}
	}
	for name, b := range f.b.files {
		if path.Dir(name) == full {
			size := int64(len(b))
			entries = append(entries, ports.FileEntry{Name: path.Base(name), Type: "file", Size: &size})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

func (f memFiles) Mkdir(_ context.Context, p string) error {
	full, err := f.path(p)
	if err != nil {
		return err
	}
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	f.addParents(full)
	f.b.dirs[full] = true
	return nil
}

func (f memFiles) Exists(_ context.Context, p string) (bool, error) {
	full, err := f.path(p)
	if err != nil {
		return false, err
	}
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	_, file := f.b.files[full]
	return file || f.b.dirs[full], nil
}

func (f memFiles) Remove(_ context.Context, p string) error {
	full, err := f.path(p)
	if err != nil {
		return err
	}
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	under := func(x string) bool { return x == full || strings.HasPrefix(x, full+"/") }
	for name := range f.b.files {
		if under(name) {
			delete(f.b.files, name)
		}
	}
	for d := range f.b.dirs {
		if under(d) {
			delete(f.b.dirs, d)
		}
	}
	return nil
}
