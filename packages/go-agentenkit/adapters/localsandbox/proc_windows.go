//go:build windows

package localsandbox

import "os/exec"

func ownGroup(*exec.Cmd) {}

func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func signalOf(*exec.ExitError) int { return 0 }

func isNotDir(error) bool { return false }
