package rigging

import (
	"encoding/json"

	"github.com/gravitational/trace"
)

func NodesMatchingLabel(labelQuery string) (*NodeList, error) {
	var nodes NodeList

	cmd := KubeCommand("get", "nodes", "-l", labelQuery, "-o", "json")
	out, err := cmd.Output()
	if err != nil {
		return nil, trace.Wrap(err)
	}

	err = json.Unmarshal(out, &nodes)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return &nodes, nil
}

func LabelNode(name string, label string) ([]byte, error) {
	cmd := KubeCommand("label", "node", name, label)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}
