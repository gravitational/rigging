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
	"fmt"
	"strings"
	"time"

	"github.com/gravitational/trace"
	"k8s.io/client-go/1.4/pkg/api/unversioned"
	"k8s.io/client-go/1.4/pkg/api/v1"
)

type ChangesetList struct {
	unversioned.TypeMeta `json:",inline"`
	// Standard list metadata
	// More info: http://releases.k8s.io/HEAD/docs/devel/api-conventions.md#metadata
	unversioned.ListMeta `json:"metadata,omitempty"`
	// Items is a list of third party objects
	Items []ChangesetResource `json:"items"`
}

func (tr *ChangesetList) GetObjectKind() unversioned.ObjectKind {
	return &tr.TypeMeta
}

type ChangesetResource struct {
	unversioned.TypeMeta `json:",inline"`
	v1.ObjectMeta        `json:"metadata,omitempty"`
	Spec                 ChangesetSpec `json:"spec"`
}

func (tr *ChangesetResource) GetObjectKind() unversioned.ObjectKind {
	return &tr.TypeMeta
}

func (tr *ChangesetResource) String() string {
	return fmt.Sprintf("namespace=%v, name=%v, operations=%v)", tr.Namespace, tr.Name, len(tr.Spec.Items))
}

type ChangesetSpec struct {
	Status string          `json:"status"`
	Items  []ChangesetItem `json:"items"`
}

type ChangesetItem struct {
	From              string    `json:"from"`
	To                string    `json:"to"`
	Status            string    `json:"status"`
	CreationTimestamp time.Time `json:"time"`
}

type OperationInfo struct {
	From *ResourceHeader
	To   *ResourceHeader
}

func (o *OperationInfo) Kind() string {
	if o.From != nil && o.From.Kind != "" {
		return o.From.Kind
	}
	if o.To != nil && o.To.Kind != "" {
		return o.To.Kind
	}
	return ""
}

func (o *OperationInfo) String() string {
	if o.From != nil && o.To == nil {
		return fmt.Sprintf("delete %v %v from namespace %v", o.From.Kind, o.From.Name, Namespace(o.From.Namespace))
	}
	if o.From != nil && o.To != nil {
		return fmt.Sprintf("update %v %v in namespace %v", o.To.Kind, o.To.Name, Namespace(o.To.Namespace))
	}
	if o.From == nil && o.To != nil {
		return fmt.Sprintf("upsert %v %v in namespace %v", o.To.Kind, o.To.Name, Namespace(o.To.Namespace))
	}
	return "unknown operation"
}

// GetOperationInfo returns operation information
func GetOperationInfo(item ChangesetItem) (*OperationInfo, error) {
	var info OperationInfo
	if item.From != "" {
		from, err := ParseResourceHeader(strings.NewReader(item.From))
		if err != nil {
			return nil, trace.Wrap(err)
		}
		info.From = from
	}
	if item.To != "" {
		to, err := ParseResourceHeader(strings.NewReader(item.To))
		if err != nil {
			return nil, trace.Wrap(err)
		}
		info.To = to
	}
	return &info, nil
}
