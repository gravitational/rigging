package rigging

import (
	"io"
	"os"
	"os/exec"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
)

type action string

const (
	ActionCreate  action = "create"
	ActionDelete  action = "delete"
	ActionReplace action = "replace"
	ActionApply   action = "apply"
)

// KubeCommand returns an exec.Command for kubectl with the supplied arguments.
func KubeCommand(args ...string) *exec.Cmd {
	return exec.Command("/usr/local/bin/kubectl", args...)
}

// FromFile performs action on the Kubernetes resources specified in the path supplied as an argument.
func FromFile(act action, path string) ([]byte, error) {
	cmd := KubeCommand(string(act), "-f", path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}

// FromStdin performs action on the Kubernetes resources specified in the string supplied as an argument.
func FromStdIn(act action, data string) error {
	cmd := KubeCommand(string(act), "-f", "-")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return trace.Wrap(err)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return trace.Wrap(err)
	}

	io.WriteString(stdin, data)
	stdin.Close()

	if err := cmd.Wait(); err != nil {
		log.Errorf("%v", err)
		return trace.Wrap(err)
	}

	return nil
}
