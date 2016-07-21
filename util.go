package rigging

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path"

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

// CreateConfigMapFromPath creates a Kubernetes ConfigMap of the supplied name, from the file or directory supplied as an argument.
func CreateConfigMapFromPath(name string, path string) ([]byte, error) {
	cmd := KubeCommand("create", "configmap", name, "--from-file="+path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}

// CreateSecretFromPath creates a Kubernetes Secret of the supplied name, from the file or directory supplied as an argument.
func CreateSecretFromPath(name string, path string) ([]byte, error) {
	cmd := KubeCommand("create", "secret", "generic", "name", "--from-file="+path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}

// CreateSecretFromMap creates a Kubernetes Secret of the supplied name, with the supplied map of keys and values.
func CreateSecretFromMap(name string, values map[string]string) ([]byte, error) {
	dir, err := ioutil.TempDir("", "rigging")
	if err != nil {
		return nil, trace.Wrap(err)
	}
	defer os.RemoveAll(dir)

	for key, val := range values {
		err := ioutil.WriteFile(path.Join(dir, key), []byte(val), 0644)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}

	cmd := KubeCommand("create", "secret", "generic", name, "--from-file="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}
