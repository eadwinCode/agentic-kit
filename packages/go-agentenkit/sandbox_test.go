//go:build !windows

package agentenkit_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/docker"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/e2b"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/localsandbox"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// The sandbox adapters, each through the shared suite: the promise every
// sandbox adapter keeps. Docker runs when TEST_DOCKER_SANDBOX=1 (it needs a
// docker daemon); live E2B when E2B_API_KEY is set. The TS package runs the
// same cases under the same names (test/sandbox-suite.ts,
// test/sandbox.test.ts).

type sandboxSuiteOptions struct {
	// noSetTimeout is for an adapter that cannot push a sandbox's time back.
	noSetTimeout bool
}

func size(n int64) *int64 { return &n }

func runSandboxSuite(t *testing.T, name string, newProvider func(t *testing.T) ports.SandboxProvider, opts sandboxSuiteOptions) {
	t.Run("sandbox ("+name+")", func(t *testing.T) {
		ctx := context.Background()
		provider := newProvider(t)
		var made []ports.Sandbox
		var mu sync.Mutex
		fresh := func(t *testing.T) ports.Sandbox {
			t.Helper()
			s, err := provider.Create(ctx, ports.CreateSandboxOptions{Timeout: time.Hour, Metadata: ports.SandboxMetadata{ThreadID: "t1", RunID: "r1"}})
			if err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			made = append(made, s)
			mu.Unlock()
			return s
		}
		t.Cleanup(func() {
			for _, s := range made {
				_ = s.Destroy(ctx)
			}
		})
		var shared ports.Sandbox
		box := func(t *testing.T) ports.Sandbox {
			if shared == nil {
				shared = fresh(t)
			}
			return shared
		}
		runOK := func(t *testing.T, s ports.Sandbox, cmd string, o ports.RunCommandOptions) ports.CommandResult {
			t.Helper()
			r, err := s.RunCommand(ctx, cmd, o)
			if err != nil {
				t.Fatal(err)
			}
			return r
		}
		must := func(t *testing.T, err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}

		t.Run("runs a command and returns its output", func(t *testing.T) {
			r := runOK(t, box(t), "echo hello; echo oops >&2", ports.RunCommandOptions{})
			mustEqual(t, r.Stdout, "hello\n", "stdout")
			mustEqual(t, r.Stderr, "oops\n", "stderr")
			mustEqual(t, r.ExitCode, 0, "exit code")
			mustEqual(t, r.TimedOut, false, "timed out")
		})

		t.Run("returns the exit code", func(t *testing.T) {
			mustEqual(t, runOK(t, box(t), "exit 3", ports.RunCommandOptions{}).ExitCode, 3, "exit code")
		})

		t.Run("stops a command at its time limit", func(t *testing.T) {
			started := time.Now()
			r := runOK(t, box(t), "echo start; sleep 30", ports.RunCommandOptions{Timeout: time.Second})
			mustEqual(t, r.TimedOut, true, "timed out")
			mustEqual(t, r.ExitCode, 124, "exit code")
			mustEqual(t, r.Stdout, "start\n", "stdout")
			if time.Since(started) > 10*time.Second {
				t.Fatalf("took %v", time.Since(started))
			}
		})

		t.Run("runs in the work folder by default, or in cwd", func(t *testing.T) {
			s := box(t)
			must(t, s.Filesystem().WriteFile(ctx, "top.txt", []byte("top")))
			must(t, s.Filesystem().WriteFile(ctx, "sub/inner.txt", []byte("inner")))
			mustEqual(t, runOK(t, s, "cat top.txt", ports.RunCommandOptions{}).Stdout, "top", "top")
			mustEqual(t, runOK(t, s, "cat inner.txt", ports.RunCommandOptions{Cwd: "sub"}).Stdout, "inner", "inner")
		})

		t.Run("passes env to the command", func(t *testing.T) {
			r := runOK(t, box(t), `echo "$GREETING"`, ports.RunCommandOptions{Env: map[string]string{"GREETING": "hi there"}})
			mustEqual(t, r.Stdout, "hi there\n", "stdout")
		})

		t.Run("streams output as it comes", func(t *testing.T) {
			var mu sync.Mutex
			var chunks []string
			var first time.Time
			r := runOK(t, box(t), "echo one; sleep 1; echo two", ports.RunCommandOptions{OnStdout: func(s string) {
				mu.Lock()
				defer mu.Unlock()
				if len(chunks) == 0 {
					first = time.Now()
				}
				chunks = append(chunks, s)
			}})
			ended := time.Now()
			mustEqual(t, strings.Join(chunks, ""), "one\ntwo\n", "chunks")
			mustEqual(t, r.Stdout, "one\ntwo\n", "stdout")
			// "one" arrived well before the command ended.
			if ended.Sub(first) < 500*time.Millisecond {
				t.Fatalf("first chunk came %v before the end", ended.Sub(first))
			}
		})

		t.Run("cuts output past the cap and says so", func(t *testing.T) {
			r := runOK(t, box(t), fmt.Sprintf(`head -c %d /dev/zero | tr '\0' a`, ports.MaxCommandOutputBytes+1000), ports.RunCommandOptions{})
			mustEqual(t, r.Truncated, true, "truncated")
			mustEqual(t, len(r.Stdout), ports.MaxCommandOutputBytes, "length")
			mustEqual(t, r.ExitCode, 0, "exit code")
		})

		t.Run("writes and reads text files", func(t *testing.T) {
			s := box(t)
			fs := s.Filesystem()
			must(t, fs.WriteFile(ctx, "notes/a.txt", []byte("héllo ✓\nline two")))
			got, err := fs.ReadFile(ctx, "notes/a.txt")
			must(t, err)
			mustEqual(t, got, "héllo ✓\nline two", "read")
			// Commands see the same files.
			mustEqual(t, runOK(t, s, "cat notes/a.txt", ports.RunCommandOptions{}).Stdout, "héllo ✓\nline two", "cat")
			must(t, fs.WriteFile(ctx, "notes/a.txt", []byte("replaced")))
			got, _ = fs.ReadFile(ctx, "notes/a.txt")
			mustEqual(t, got, "replaced", "replaced")
		})

		t.Run("writes and reads binary files", func(t *testing.T) {
			s := box(t)
			data := make([]byte, 70_000)
			for i := range data {
				data[i] = byte((i * 7) % 256)
			}
			must(t, s.Filesystem().WriteFile(ctx, "bin/data.bin", data))
			got, err := s.Filesystem().ReadFileBytes(ctx, "bin/data.bin")
			must(t, err)
			if !bytes.Equal(got, data) {
				t.Fatalf("bytes differ: got %d bytes", len(got))
			}
			mustEqual(t, strings.TrimSpace(runOK(t, s, "wc -c < bin/data.bin", ports.RunCommandOptions{}).Stdout), "70000", "wc")
		})

		t.Run("lists, makes and removes folders", func(t *testing.T) {
			fs := box(t).Filesystem()
			must(t, fs.Mkdir(ctx, "d/e"))
			must(t, fs.Mkdir(ctx, "d/e")) // fine when it is there
			must(t, fs.WriteFile(ctx, "d/f.txt", []byte("x")))
			entries, err := fs.ReadDir(ctx, "d")
			must(t, err)
			want := []ports.FileEntry{{Name: "e", Type: "directory"}, {Name: "f.txt", Type: "file", Size: size(1)}}
			if !reflect.DeepEqual(entries, want) {
				t.Fatalf("entries: %+v", entries)
			}
			there, _ := fs.Exists(ctx, "d/e")
			mustEqual(t, there, true, "exists")
			must(t, fs.Remove(ctx, "d"))
			there, _ = fs.Exists(ctx, "d")
			mustEqual(t, there, false, "exists after remove")
			must(t, fs.Remove(ctx, "d")) // fine when it is not there
		})

		t.Run("a missing file is not found", func(t *testing.T) {
			fs := box(t).Filesystem()
			_, err := fs.ReadFile(ctx, "nope.txt")
			mustEqual(t, errors.Is(err, ports.ErrSandboxFileNotFound), true, "read: "+fmt.Sprint(err))
			_, err = fs.ReadFileBytes(ctx, "nope.txt")
			mustEqual(t, errors.Is(err, ports.ErrSandboxFileNotFound), true, "read bytes")
			_, err = fs.ReadDir(ctx, "nope")
			mustEqual(t, errors.Is(err, ports.ErrSandboxFileNotFound), true, "readdir")
			there, err := fs.Exists(ctx, "nope.txt")
			must(t, err)
			mustEqual(t, there, false, "exists")
		})

		t.Run("runs a command in the background", func(t *testing.T) {
			s := box(t)
			started := time.Now()
			runOK(t, s, "sleep 1; echo done > bg.txt", ports.RunCommandOptions{Background: true})
			if time.Since(started) > time.Second {
				t.Fatalf("background took %v", time.Since(started))
			}
			there := false
			for i := 0; i < 50 && !there; i++ {
				there, _ = s.Filesystem().Exists(ctx, "bg.txt")
				if !there {
					time.Sleep(200 * time.Millisecond)
				}
			}
			mustEqual(t, there, true, "bg.txt")
		})

		t.Run("getInfo describes the sandbox", func(t *testing.T) {
			s := box(t)
			info, err := s.GetInfo(ctx)
			must(t, err)
			mustEqual(t, info.ID, s.ID(), "id")
			mustEqual(t, info.Provider, s.Provider(), "provider")
			mustEqual(t, info.Status, "running", "status")
			mustEqual(t, strings.HasPrefix(info.Workdir, "/"), true, "workdir")
			mustEqual(t, info.Metadata["threadId"], "t1", "thread")
		})

		if !opts.noSetTimeout {
			t.Run("setTimeout pushes back the end", func(t *testing.T) {
				s := box(t)
				must(t, s.SetTimeout(ctx, 10*time.Minute))
				info, err := s.GetInfo(ctx)
				must(t, err)
				left := time.Until(info.ExpiresAt)
				if left < 8*time.Minute || left > 12*time.Minute {
					t.Fatalf("ends in %v", left)
				}
			})
		}

		t.Run("connects to a sandbox made earlier", func(t *testing.T) {
			s := fresh(t)
			must(t, s.Filesystem().WriteFile(ctx, "kept.txt", []byte("still here")))
			again, err := provider.Connect(ctx, s.ID())
			must(t, err)
			mustEqual(t, again.ID(), s.ID(), "id")
			got, err := again.Filesystem().ReadFile(ctx, "kept.txt")
			must(t, err)
			mustEqual(t, got, "still here", "kept")
		})

		t.Run("a destroyed sandbox is gone", func(t *testing.T) {
			s := fresh(t)
			must(t, s.Destroy(ctx))
			_, err := provider.Connect(ctx, s.ID())
			mustEqual(t, errors.Is(err, ports.ErrSandboxGone), true, "gone: "+fmt.Sprint(err))
			must(t, s.Destroy(ctx)) // fine when it is already gone
		})
	})
}

func TestSandbox(t *testing.T) {
	runSandboxSuite(t, "local", func(t *testing.T) ports.SandboxProvider {
		return localsandbox.New(localsandbox.Options{RootDir: t.TempDir()})
	}, sandboxSuiteOptions{})

	runSandboxSuite(t, "e2b (fake)", func(t *testing.T) ports.SandboxProvider {
		fake := startFakeE2B(t)
		p, err := e2b.New("test-key", e2b.Options{APIURL: fake.url, SandboxURL: fake.url})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}, sandboxSuiteOptions{})

	if os.Getenv("TEST_DOCKER_SANDBOX") == "1" {
		// Pulling the image the first time can take a while.
		_ = exec.Command("docker", "pull", "-q", "python:3.12-slim").Run()
		runSandboxSuite(t, "docker", func(*testing.T) ports.SandboxProvider { return docker.New(docker.Options{}) }, sandboxSuiteOptions{})
	}

	if key := os.Getenv("E2B_API_KEY"); key != "" {
		runSandboxSuite(t, "e2b", func(t *testing.T) ports.SandboxProvider {
			p, err := e2b.New(key, e2b.Options{})
			if err != nil {
				t.Fatal(err)
			}
			return p
		}, sandboxSuiteOptions{})
	}
}

func TestSandboxAdapters_E2BSandboxSendsTheTemplateTimeMetadataAndNetworkItWasGiven(t *testing.T) {
	ctx := context.Background()
	fake := startFakeE2B(t)
	p, _ := e2b.New("test-key", e2b.Options{APIURL: fake.url, SandboxURL: fake.url, Template: "code-interpreter"})
	s, err := p.Create(ctx, ports.CreateSandboxOptions{
		Timeout:  90500 * time.Millisecond,
		Metadata: ports.SandboxMetadata{ThreadID: "t9", RunID: "r9"},
		Network:  &ports.SandboxNetwork{Mode: "allow", Allow: []string{"pypi.org"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustJSON(t, fake.lastCreateBody(), map[string]any{
		"templateID":            "code-interpreter",
		"timeout":               91,
		"metadata":              map[string]any{"agentenkit.threadId": "t9", "agentenkit.runId": "r9"},
		"allow_internet_access": true,
		"network":               map[string]any{"allowOut": []any{"pypi.org"}, "denyOut": []any{"0.0.0.0/0"}},
	}, "create body")
	_ = s.Destroy(ctx)
	if _, err := p.Create(ctx, ports.CreateSandboxOptions{Timeout: time.Second, Metadata: ports.SandboxMetadata{ThreadID: "t9"}}); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, fake.lastCreateBody()["allow_internet_access"], false, "no network by default")
}

func TestSandboxAdapters_E2BSandboxDoesNotRunACommandWhoseSignalIsAlreadyStopped(t *testing.T) {
	ctx := context.Background()
	fake := startFakeE2B(t)
	p, _ := e2b.New("test-key", e2b.Options{APIURL: fake.url, SandboxURL: fake.url})
	s, err := p.Create(ctx, ports.CreateSandboxOptions{Timeout: time.Minute, Metadata: ports.SandboxMetadata{ThreadID: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	stopped, cancel := context.WithCancel(ctx)
	cancel()
	_, err = s.RunCommand(stopped, "echo x > ran.txt", ports.RunCommandOptions{})
	mustEqual(t, errors.Is(err, context.Canceled), true, "error: "+fmt.Sprint(err))
	there, _ := s.Filesystem().Exists(ctx, "ran.txt")
	mustEqual(t, there, false, "ran.txt")
	_ = s.Destroy(ctx)
}

func TestSandboxAdapters_E2BSandboxNeedsAnAPIKey(t *testing.T) {
	_, err := e2b.New("", e2b.Options{})
	if err == nil || err.Error() != "E2BSandbox: apiKey is required" {
		t.Fatalf("got %v", err)
	}
}

func TestSandboxAdapters_DockerSandboxHasNoNetworkAllowList(t *testing.T) {
	p := docker.New(docker.Options{Docker: "false"})
	_, err := p.Create(context.Background(), ports.CreateSandboxOptions{
		Timeout: time.Second, Metadata: ports.SandboxMetadata{ThreadID: "t"},
		Network: &ports.SandboxNetwork{Mode: "allow", Allow: []string{"pypi.org"}},
	})
	mustEqual(t, errors.Is(err, ports.ErrSandboxUnsupported), true, "unsupported: "+fmt.Sprint(err))
}

func TestSandboxAdapters_LocalSandboxCommandsDoNotSeeTheAppEnvironment(t *testing.T) {
	t.Setenv("AGENTENKIT_SECRET_FOR_TEST", "do-not-leak")
	ctx := context.Background()
	s, err := localsandbox.New(localsandbox.Options{RootDir: t.TempDir()}).Create(ctx, ports.CreateSandboxOptions{Timeout: time.Minute, Metadata: ports.SandboxMetadata{ThreadID: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.RunCommand(ctx, `echo "[$AGENTENKIT_SECRET_FOR_TEST]"`, ports.RunCommandOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, r.Stdout, "[]\n", "stdout")
}

func TestSandboxAdapters_LocalSandboxEndsASandboxPastItsTime(t *testing.T) {
	ctx := context.Background()
	p := localsandbox.New(localsandbox.Options{RootDir: t.TempDir()})
	s, err := p.Create(ctx, ports.CreateSandboxOptions{Timeout: 50 * time.Millisecond, Metadata: ports.SandboxMetadata{ThreadID: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	_, err = p.Connect(ctx, s.ID())
	if err == nil || !strings.Contains(err.Error(), "is gone") {
		t.Fatalf("got %v", err)
	}
}
