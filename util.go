package rigging

import (
	"os/exec"

	"github.com/gravitational/trace"
)

// KubeCommand returns an exec.Command for kubectl with the supplied arguments.
func KubeCommand(args ...string) *exec.Cmd {
	return exec.Command("/usr/local/bin/kubectl", args...)
}

// CreateFromFile creates the Kubernetes resources specified in the path supplied as an argument.
func CreateFromFile(path string) ([]byte, error) {
	cmd := KubeCommand("create", "-f", path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}
