package rigging

import (
	"github.com/gravitational/trace"
)

// CreateConfigMapFromPath creates a Kubernetes ConfigMap of the supplied name, from the file or directory supplied as an argument.
func CreateConfigMapFromPath(name string, path string) ([]byte, error) {
	cmd := KubeCommand("create", "configmap", name, "--from-file="+path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}
