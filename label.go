// Copyright 2016-2020 Gravitational Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rigging

import (
	"github.com/gravitational/trace"
	v1 "k8s.io/api/core/v1"
	v1meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func NodesMatchingLabel(client *kubernetes.Clientset, labelQuery string) (*v1.NodeList, error) {
	nodes, err := client.CoreV1().Nodes().List(v1meta.ListOptions{LabelSelector: labelQuery})
	if err != nil {
		return nil, ConvertError(err)
	}

	return nodes, nil
}

func LabelNode(name string, label string) ([]byte, error) {
	cmd := KubeCommand("label", "node", name, label)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}
