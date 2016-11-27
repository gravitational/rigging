// Copyright 2016 Gravitational Inc.
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
	"io"

	"github.com/gravitational/trace"
	"k8s.io/client-go/1.4/pkg/api"
	"k8s.io/client-go/1.4/pkg/api/unversioned"
	"k8s.io/client-go/1.4/pkg/api/v1"
	"k8s.io/client-go/1.4/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/1.4/pkg/util/yaml"
)

type ResourceHeader struct {
	unversioned.TypeMeta `json:",inline"`
	v1.ObjectMeta        `json:"metadata,omitempty"`
}

// ParseResourceHeader parses resource header information
func ParseResourceHeader(reader io.Reader) (*ResourceHeader, error) {
	var out ResourceHeader
	err := yaml.NewYAMLOrJSONDecoder(reader, 1024).Decode(&out)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &out, nil
}

// ParseDaemonSet parses daemon set from reader
func ParseDaemonSet(r io.Reader) (*v1beta1.DaemonSet, error) {
	if r == nil {
		return nil, trace.BadParameter("missing reader")
	}
	ds := v1beta1.DaemonSet{}
	err := yaml.NewYAMLOrJSONDecoder(r, 1024).Decode(&ds)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &ds, nil
}

// ParseSerializedReference parses serialized reference object
// used in annotations
func ParseSerializedReference(r io.Reader) (*api.SerializedReference, error) {
	ref := api.SerializedReference{}
	err := yaml.NewYAMLOrJSONDecoder(r, 1024).Decode(&ref)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &ref, nil
}

// ParseReplicationController parses replication controller
func ParseReplicationController(r io.Reader) (*v1.ReplicationController, error) {
	if r == nil {
		return nil, trace.BadParameter("missing reader")
	}
	rc := v1.ReplicationController{}
	err := yaml.NewYAMLOrJSONDecoder(r, 1024).Decode(&rc)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &rc, nil
}

// ParseDeployment parses deployment
func ParseDeployment(r io.Reader) (*v1beta1.Deployment, error) {
	if r == nil {
		return nil, trace.BadParameter("missing reader")
	}
	rc := v1beta1.Deployment{}
	err := yaml.NewYAMLOrJSONDecoder(r, 1024).Decode(&rc)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &rc, nil
}
