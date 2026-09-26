package ports

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// The sandbox port: a place apart from the app where an agent's commands
// run and its files live. A container, a hosted machine, or a folder on
// this machine in development.
//
// The shape and names follow ComputeSDK's Sandbox, with three additions:
// ReadFileBytes (charts and images need it), SetTimeout (how an idle
// sandbox ends by itself) and SandboxInfo.Workdir. The TS runtime has the
// same port (src/ports/sandbox.ts).

// DefaultCommandTimeout is how long a command may run when the caller does
// not say.
const DefaultCommandTimeout = 120 * time.Second

// MaxCommandOutputBytes is the most output kept from one command, per
// stream. Past it the rest is dropped and the result says Truncated, so a
// command that prints without end cannot fill the app's memory.
const MaxCommandOutputBytes = 4 * 1024 * 1024

// TimedOutExitCode is the exit code of a command stopped at its time limit,
// as timeout(1) gives it.
const TimedOutExitCode = 124

// SandboxNetwork is what a sandbox may reach: "none" (the default), "all",
// or only the hosts in Allow.
type SandboxNetwork struct {
	// Mode is "none", "all" or "allow". Empty is "none".
	Mode  string
	Allow []string
}

// SandboxMetadata says what a sandbox is for. Adapters keep ThreadID and
// RunID as labels or metadata where the provider has them; a multi-tenant
// adapter can read State to pick a template or key per tenant (§2.10).
type SandboxMetadata struct {
	ThreadID string
	RunID    string
	State    AgentRunState
}

// SandboxResources asks for a size; zero leaves the adapter's default.
type SandboxResources struct {
	CPU       float64
	MemoryMiB int
	DiskMiB   int
}

// CreateSandboxOptions shapes one new sandbox.
type CreateSandboxOptions struct {
	// Timeout is how long the sandbox lives unless SetTimeout pushes it
	// back. It then shuts itself down, even when the app that made it is
	// gone.
	Timeout  time.Duration
	Metadata SandboxMetadata
	// Envs are environment variables every command sees.
	Envs map[string]string
	// Template (E2B) or Image (Docker); adapters have their own default.
	Template  string
	Image     string
	Network   *SandboxNetwork
	Resources SandboxResources
	// Extra holds provider-only settings, passed through as they are.
	Extra map[string]any
}

// SandboxProvider makes sandboxes and finds them again. One per app, set as
// RuntimeOptions.Tools.Sandbox.
type SandboxProvider interface {
	// Name is the adapter's name: "docker", "e2b", "local".
	Name() string
	Create(ctx context.Context, opts CreateSandboxOptions) (Sandbox, error)
	// Connect finds a sandbox made earlier, by this process or another. It
	// returns an error matching ErrSandboxGone when it no longer exists.
	Connect(ctx context.Context, sandboxID string) (Sandbox, error)
}

// RunCommandOptions shapes one command.
type RunCommandOptions struct {
	// Cwd is where the command runs. A relative path is taken from the
	// sandbox's work folder, which is also the default.
	Cwd string
	Env map[string]string
	// Timeout stops the command after this long: the result then has
	// TimedOut and exit code 124, with the output it printed so far. Zero
	// is DefaultCommandTimeout. Cancelling ctx also stops it, and returns
	// ctx's error.
	Timeout time.Duration
	// Background starts the command and returns at once, leaving it
	// running (a dev server, say). Its output is not kept.
	Background bool
	// OnStdout and OnStderr get output as it comes, decoded as UTF-8.
	OnStdout func(chunk string)
	OnStderr func(chunk string)
}

// CommandResult is what a command did.
type CommandResult struct {
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	ExitCode   int    `json:"exitCode"`
	DurationMs int64  `json:"durationMs"`
	// TimedOut is true when the command was stopped at its time limit.
	TimedOut bool `json:"timedOut,omitempty"`
	// Truncated is true when output past MaxCommandOutputBytes was dropped.
	Truncated bool `json:"truncated,omitempty"`
}

// FileEntry is one entry in a folder. Type is "file" or "directory"; Size
// is set for files.
type FileEntry struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Size *int64 `json:"size,omitempty"`
}

// SandboxFileSystem is the sandbox's files. A relative path is taken from
// the work folder.
type SandboxFileSystem interface {
	// ReadFile returns an error matching ErrSandboxFileNotFound when there
	// is no such file.
	ReadFile(ctx context.Context, path string) (string, error)
	// ReadFileBytes is not in ComputeSDK: charts and images are bytes.
	ReadFileBytes(ctx context.Context, path string) ([]byte, error)
	// WriteFile makes the parent folders it needs, and replaces a file
	// already there.
	WriteFile(ctx context.Context, path string, content []byte) error
	// ReadDir returns the folder's entries, sorted by name, or an error
	// matching ErrSandboxFileNotFound when there is no such folder.
	ReadDir(ctx context.Context, path string) ([]FileEntry, error)
	// Mkdir makes the folder and its parents; fine when it is there already.
	Mkdir(ctx context.Context, path string) error
	Exists(ctx context.Context, path string) (bool, error)
	// Remove removes a file, or a folder with everything in it; fine when
	// it is not there.
	Remove(ctx context.Context, path string) error
}

// SandboxInfo describes a sandbox.
type SandboxInfo struct {
	ID       string
	Provider string
	// Status is "running", "stopped" or "error".
	Status    string
	CreatedAt time.Time
	// ExpiresAt is when it will shut itself down, when the provider says;
	// zero otherwise.
	ExpiresAt time.Time
	// Workdir is the folder commands start in and relative paths are taken
	// from.
	Workdir  string
	Metadata map[string]string
}

// Sandbox is one sandbox.
type Sandbox interface {
	ID() string
	Provider() string
	Filesystem() SandboxFileSystem
	RunCommand(ctx context.Context, command string, opts RunCommandOptions) (CommandResult, error)
	// GetURL returns a URL for a port the sandbox serves on, or an error
	// matching ErrSandboxUnsupported where the adapter cannot give one.
	// Protocol is "http" or "https"; empty is the adapter's default.
	GetURL(ctx context.Context, port int, protocol string) (string, error)
	GetInfo(ctx context.Context) (SandboxInfo, error)
	// SetTimeout is not in ComputeSDK: the sandbox now shuts itself down d
	// from now. The runtime calls it as a thread uses its sandbox, so one
	// left idle ends by itself. It returns an error matching
	// ErrSandboxUnsupported where the provider cannot change it.
	SetTimeout(ctx context.Context, d time.Duration) error
	// Destroy ends the sandbox and its files. Fine when it is already gone.
	Destroy(ctx context.Context) error
}

var (
	// ErrSandboxGone: the sandbox no longer exists. It timed out, was
	// destroyed, or its provider lost it.
	ErrSandboxGone = errors.New("sandbox is gone")
	// ErrSandboxFileNotFound: there is no file or folder at this path.
	ErrSandboxFileNotFound = errors.New("no such file or folder")
	// ErrSandboxUnsupported: the adapter cannot do this.
	ErrSandboxUnsupported = errors.New("not supported")
)

// SandboxGone is the error for a sandbox that no longer exists.
func SandboxGone(sandboxID, why string) error {
	if why != "" {
		return fmt.Errorf("sandbox %s is gone: %s%w", sandboxID, why, errGoneMark)
	}
	return fmt.Errorf("sandbox %s is gone%w", sandboxID, errGoneMark)
}

// SandboxFileNotFound is the error for a path with nothing at it.
func SandboxFileNotFound(path string) error {
	return fmt.Errorf("sandbox: no such file or folder: %s%w", path, errNotFoundMark)
}

// SandboxUnsupported is the error for what an adapter cannot do.
func SandboxUnsupported(provider, what string) error {
	return fmt.Errorf("%s: %s is not supported%w", provider, what, errUnsupportedMark)
}

// The marks wrap the sentinels without adding to the message, so the text
// matches the TS runtime's word for word.
var (
	errGoneMark        = silent{ErrSandboxGone}
	errNotFoundMark    = silent{ErrSandboxFileNotFound}
	errUnsupportedMark = silent{ErrSandboxUnsupported}
)

type silent struct{ err error }

func (s silent) Error() string { return "" }
func (s silent) Unwrap() error { return s.err }
