// Package docker runs sandboxes as Docker containers on the machine the
// docker CLI talks to: for self-hosting. One container per thread, removed
// when it has been idle past its time. It uses the docker CLI, so it needs
// no SDK and follows your DOCKER_HOST and contexts. The TS runtime has the
// same adapter (DockerSandbox).
package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/adapters/internal/sandboxsh"
	"github.com/eadwinCode/agentic-kit/packages/go-agentenkit/ports"
)

// Options shapes the provider.
type Options struct {
	// Image is what each sandbox runs. It needs sh; bash, Python and
	// timeout are used when there. Default python:3.12-slim.
	Image string
	// Network is "none" (the default) or "all". Docker has no allow list by
	// domain.
	Network string
	// Workdir is the folder commands start in. Default /workspace.
	Workdir   string
	CPU       float64
	MemoryMiB int
	// Ports to publish on 127.0.0.1, so GetURL can give them. Needs
	// Network "all".
	Ports []int
	// Env holds environment variables every command sees.
	Env map[string]string
	// RunArgs are more docker run arguments, placed before the image.
	RunArgs []string
	// Docker is the docker CLI. Default docker on the PATH.
	Docker string
}

// Provider makes Docker sandboxes.
type Provider struct{ opts Options }

// New returns the provider.
func New(opts Options) *Provider {
	if opts.Docker == "" {
		opts.Docker = "docker"
	}
	return &Provider{opts: opts}
}

// Name implements ports.SandboxProvider.
func (p *Provider) Name() string { return "docker" }

const deadlineFile = "/tmp/.agentenkit-deadline"

// watchdog is the container's main process: it waits until the deadline in
// deadlineFile has passed and then exits, and --rm removes the container.
// So a sandbox nobody uses ends by itself, even when the app that made it
// is gone. SetTimeout moves the deadline.
const watchdog = `echo "$1" > ` + deadlineFile + `; ` +
	`while [ "$(date +%s)" -lt "$(cat ` + deadlineFile + ` 2>/dev/null || echo 0)" ]; do sleep 2; done`

// gone is what the docker CLI says when the container is not there any
// more.
var gone = regexp.MustCompile(`Error response from daemon: (No such container|container \S+ is not running)|No such object`)

// Create implements ports.SandboxProvider.
func (p *Provider) Create(ctx context.Context, opts ports.CreateSandboxOptions) (ports.Sandbox, error) {
	network := p.opts.Network
	if opts.Network != nil {
		network = opts.Network.Mode
	}
	if network == "allow" {
		return nil, ports.SandboxUnsupported("docker", "a network allow list")
	}
	name := "agentenkit-" + sandboxsh.RandomID(12)
	workdir := p.opts.Workdir
	if workdir == "" {
		workdir = "/workspace"
	}
	deadline := time.Now().Add(opts.Timeout).Unix()
	args := []string{
		"run", "-d", "--rm", "--init", "--name", name,
		"--label", "agentenkit=sandbox",
		"--label", "agentenkit.threadId=" + opts.Metadata.ThreadID,
	}
	if opts.Metadata.RunID != "" {
		args = append(args, "--label", "agentenkit.runId="+opts.Metadata.RunID)
	}
	netName := "none"
	if network == "all" {
		netName = "bridge"
	}
	args = append(args, "-w", workdir, "--network", netName, "--security-opt", "no-new-privileges")
	cpu, memory := p.opts.CPU, p.opts.MemoryMiB
	if opts.Resources.CPU > 0 {
		cpu = opts.Resources.CPU
	}
	if opts.Resources.MemoryMiB > 0 {
		memory = opts.Resources.MemoryMiB
	}
	if cpu > 0 {
		args = append(args, "--cpus", strconv.FormatFloat(cpu, 'f', -1, 64))
	}
	if memory > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", memory))
	}
	for _, port := range p.opts.Ports {
		args = append(args, "-p", fmt.Sprintf("127.0.0.1::%d", port))
	}
	for _, env := range []map[string]string{p.opts.Env, opts.Envs} {
		for k, v := range env {
			args = append(args, "-e", k+"="+v)
		}
	}
	args = append(args, p.opts.RunArgs...)
	image := opts.Image
	if image == "" {
		image = p.opts.Image
	}
	if image == "" {
		image = "python:3.12-slim"
	}
	args = append(args, image, "sh", "-c", watchdog, "sandbox", strconv.FormatInt(deadline, 10))
	r, err := cli(ctx, p.opts.Docker, args, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if r.code != 0 {
		return nil, fmt.Errorf("docker: could not start a sandbox: %s", strings.TrimSpace(r.stderr.String()))
	}
	return p.box(name, workdir), nil
}

// Connect implements ports.SandboxProvider.
func (p *Provider) Connect(ctx context.Context, sandboxID string) (ports.Sandbox, error) {
	r, err := cli(ctx, p.opts.Docker, []string{"inspect", "-f", "{{.State.Running}}\t{{.Config.WorkingDir}}", sandboxID}, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	fields := strings.SplitN(strings.TrimSpace(r.stdout.String()), "\t", 2)
	if r.code != 0 || fields[0] != "true" {
		return nil, ports.SandboxGone(sandboxID, "")
	}
	workdir := "/"
	if len(fields) == 2 && fields[1] != "" {
		workdir = fields[1]
	}
	return p.box(sandboxID, workdir), nil
}

func (p *Provider) box(id, workdir string) *box {
	b := &box{docker: p.opts.Docker, id: id, workdir: workdir}
	b.files = &files{b: b}
	return b
}

type box struct {
	docker, id, workdir string
	files               *files
}

func (b *box) ID() string                          { return b.id }
func (b *box) Provider() string                    { return "docker" }
func (b *box) Filesystem() ports.SandboxFileSystem { return b.files }
func (b *box) path(p string) string                { return sandboxsh.ResolveIn(b.workdir, p) }

// script runs a script with sh -c through docker exec; a container that is
// gone is an error.
func (b *box) script(ctx context.Context, script string, args []string, stdin []byte) (result, error) {
	argv := []string{"exec"}
	if stdin != nil {
		argv = append(argv, "-i")
	}
	argv = append(argv, b.id, "sh", "-c", script, "sandbox")
	r, err := cli(ctx, b.docker, append(argv, args...), stdin, nil, nil)
	if err != nil {
		return r, err
	}
	if r.code != 0 && gone.MatchString(r.stderr.String()) {
		return r, ports.SandboxGone(b.id, "")
	}
	return r, nil
}

func (b *box) RunCommand(ctx context.Context, command string, opts ports.RunCommandOptions) (ports.CommandResult, error) {
	limit := opts.Timeout
	if limit <= 0 {
		limit = ports.DefaultCommandTimeout
	}
	cwd := opts.Cwd
	if cwd == "" {
		cwd = "."
	}
	args := []string{"exec", "-w", b.path(cwd)}
	for k, v := range opts.Env {
		args = append(args, "-e", k+"="+v)
	}
	cmd, secs := command, sandboxsh.TimeoutSeconds(limit)
	onOut, onErr := opts.OnStdout, opts.OnStderr
	if opts.Background {
		cmd, secs, onOut, onErr = sandboxsh.BackgroundCommand(command), 0, nil, nil
	}
	args = append(append(args, b.id), sandboxsh.RunArgv(cmd, secs, false)...)
	out, errOut := sandboxsh.NewOutput(onOut), sandboxsh.NewOutput(onErr)
	started := time.Now()
	// timeout inside the container stops the command; this is the backstop
	// for an image without it.
	runCtx, cancel := context.WithTimeout(ctx, limit+5*time.Second)
	defer cancel()
	r, err := cli(runCtx, b.docker, args, nil, out, errOut)
	if ctx.Err() != nil {
		return ports.CommandResult{}, ctx.Err()
	}
	killed := runCtx.Err() != nil
	if err != nil && !killed {
		return ports.CommandResult{}, err
	}
	stdout, stderr := out.String(), errOut.String()
	if r.code != 0 && gone.MatchString(stderr) {
		return ports.CommandResult{}, ports.SandboxGone(b.id, "")
	}
	elapsed := time.Since(started)
	if opts.Background {
		return ports.CommandResult{ExitCode: r.code, DurationMs: elapsed.Milliseconds()}, nil
	}
	timedOut := killed || sandboxsh.LooksTimedOut(r.code, elapsed, limit)
	code := r.code
	if timedOut {
		code = ports.TimedOutExitCode
	}
	return ports.CommandResult{
		Stdout: stdout, Stderr: stderr, ExitCode: code, DurationMs: elapsed.Milliseconds(),
		TimedOut: timedOut, Truncated: out.Truncated || errOut.Truncated,
	}, nil
}

func (b *box) GetURL(ctx context.Context, port int, protocol string) (string, error) {
	r, err := cli(ctx, b.docker, []string{"port", b.id, fmt.Sprintf("%d/tcp", port)}, nil, nil, nil)
	if err != nil {
		return "", err
	}
	if r.code != 0 && gone.MatchString(r.stderr.String()) {
		return "", ports.SandboxGone(b.id, "")
	}
	hostPort := strings.SplitN(strings.TrimSpace(r.stdout.String()), "\n", 2)[0]
	if r.code != 0 || hostPort == "" {
		return "", ports.SandboxUnsupported("docker", fmt.Sprintf("a URL for port %d (publish it with the ports option)", port))
	}
	if protocol == "" {
		protocol = "http"
	}
	return protocol + "://" + strings.Replace(hostPort, "0.0.0.0", "127.0.0.1", 1), nil
}

func (b *box) GetInfo(ctx context.Context) (ports.SandboxInfo, error) {
	r, err := cli(ctx, b.docker, []string{"inspect", "-f", "{{.Created}}\t{{.State.Status}}\t{{json .Config.Labels}}", b.id}, nil, nil, nil)
	if err != nil {
		return ports.SandboxInfo{}, err
	}
	if r.code != 0 {
		return ports.SandboxInfo{}, ports.SandboxGone(b.id, "")
	}
	fields := strings.SplitN(strings.TrimSpace(r.stdout.String()), "\t", 3)
	for len(fields) < 3 {
		fields = append(fields, "")
	}
	created, _ := time.Parse(time.RFC3339Nano, fields[0])
	var labels map[string]string
	_ = json.Unmarshal([]byte(fields[2]), &labels)
	md := map[string]string{}
	for k, v := range labels {
		if strings.HasPrefix(k, "agentenkit.") {
			md[strings.TrimPrefix(k, "agentenkit.")] = v
		}
	}
	status := "error"
	switch fields[1] {
	case "running":
		status = "running"
	case "exited":
		status = "stopped"
	}
	info := ports.SandboxInfo{ID: b.id, Provider: "docker", Status: status, CreatedAt: created, Workdir: b.workdir, Metadata: md}
	d, err := b.script(ctx, "cat "+deadlineFile, nil, nil)
	if err != nil {
		return info, err
	}
	if secs, err := strconv.ParseInt(strings.TrimSpace(d.stdout.String()), 10, 64); err == nil && secs > 0 {
		info.ExpiresAt = time.Unix(secs, 0)
	}
	return info, nil
}

func (b *box) SetTimeout(ctx context.Context, d time.Duration) error {
	deadline := strconv.FormatInt(time.Now().Add(d).Unix(), 10)
	r, err := b.script(ctx, `echo "$1" > `+deadlineFile, []string{deadline}, nil)
	if err != nil {
		return err
	}
	if r.code != 0 {
		return fmt.Errorf("docker: could not set the sandbox's time: %s", strings.TrimSpace(r.stderr.String()))
	}
	return nil
}

func (b *box) Destroy(ctx context.Context) error {
	r, err := cli(ctx, b.docker, []string{"rm", "-f", b.id}, nil, nil, nil)
	if err != nil {
		return err
	}
	if r.code != 0 && !gone.MatchString(r.stderr.String()) {
		return fmt.Errorf("docker: could not remove %s: %s", b.id, strings.TrimSpace(r.stderr.String()))
	}
	return nil
}

type files struct{ b *box }

func (f *files) run(ctx context.Context, script, p string, stdin []byte) (result, error) {
	r, err := f.b.script(ctx, script, []string{f.b.path(p)}, stdin)
	if err != nil {
		return r, err
	}
	if r.code == sandboxsh.NotFoundExit {
		return r, ports.SandboxFileNotFound(p)
	}
	if r.code != 0 {
		msg := strings.TrimSpace(r.stderr.String())
		if msg == "" {
			msg = fmt.Sprintf("exit %d", r.code)
		}
		return r, errors.New("docker: " + msg)
	}
	return r, nil
}

func (f *files) ReadFile(ctx context.Context, p string) (string, error) {
	b, err := f.ReadFileBytes(ctx, p)
	return string(b), err
}

func (f *files) ReadFileBytes(ctx context.Context, p string) ([]byte, error) {
	r, err := f.run(ctx, sandboxsh.ReadScript, p, nil)
	if err != nil {
		return nil, err
	}
	return r.stdout.Bytes(), nil
}

func (f *files) WriteFile(ctx context.Context, p string, content []byte) error {
	if content == nil {
		content = []byte{}
	}
	_, err := f.run(ctx, sandboxsh.WriteScript, p, content)
	return err
}

func (f *files) ReadDir(ctx context.Context, p string) ([]ports.FileEntry, error) {
	r, err := f.run(ctx, sandboxsh.ListScript, p, nil)
	if err != nil {
		return nil, err
	}
	return sandboxsh.ParseListing(r.stdout.String()), nil
}

func (f *files) Mkdir(ctx context.Context, p string) error {
	_, err := f.run(ctx, sandboxsh.MkdirScript, p, nil)
	return err
}

func (f *files) Exists(ctx context.Context, p string) (bool, error) {
	r, err := f.b.script(ctx, sandboxsh.ExistsScript, []string{f.b.path(p)}, nil)
	if err != nil {
		return false, err
	}
	return r.code == 0, nil
}

func (f *files) Remove(ctx context.Context, p string) error {
	_, err := f.run(ctx, sandboxsh.RemoveScript, p, nil)
	return err
}

type result struct {
	code           int
	stdout, stderr *bytes.Buffer
}

// cli runs the docker CLI. With out and errOut the output goes there as it
// comes; otherwise it is collected whole. A run cut short by ctx returns
// what it had with ctx's error.
func cli(ctx context.Context, docker string, args []string, stdin []byte, out, errOut *sandboxsh.Output) (result, error) {
	r := result{stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
	cmd := exec.CommandContext(ctx, docker, args...)
	cmd.WaitDelay = 2 * time.Second
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	cmd.Stdout, cmd.Stderr = r.stdout, r.stderr
	if out != nil {
		cmd.Stdout = out
	}
	if errOut != nil {
		cmd.Stderr = errOut
	}
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case ctx.Err() != nil:
		r.code = -1
		return r, ctx.Err()
	case errors.As(err, &exitErr):
		r.code = exitErr.ExitCode()
		return r, nil
	case err != nil:
		return r, fmt.Errorf("docker: could not run %s: %w", docker, err)
	}
	return r, nil
}
