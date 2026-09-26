//go:build !windows

package localsandbox

import (
	"errors"
	"os/exec"
	"syscall"
)

// ownGroup runs the command in its own process group, so the time limit
// stops what it started too.
func ownGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	if syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) != nil {
		_ = cmd.Process.Kill()
	}
}

func signalOf(e *exec.ExitError) int {
	if ws, ok := e.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return int(ws.Signal())
	}
	return 0
}

func isNotDir(err error) bool { return errors.Is(err, syscall.ENOTDIR) }
