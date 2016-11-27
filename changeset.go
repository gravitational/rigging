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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	goyaml "github.com/ghodss/yaml"
	"github.com/gravitational/trace"
	"k8s.io/client-go/1.4/kubernetes"
	api "k8s.io/client-go/1.4/pkg/api"
	"k8s.io/client-go/1.4/pkg/api/errors"
	"k8s.io/client-go/1.4/pkg/api/unversioned"
	"k8s.io/client-go/1.4/pkg/api/v1"
	"k8s.io/client-go/1.4/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/1.4/pkg/runtime"
	serializer "k8s.io/client-go/1.4/pkg/runtime/serializer"
	"k8s.io/client-go/1.4/pkg/util/yaml"
	"k8s.io/client-go/1.4/rest"
)

type ChangesetConfig struct {
	// Client is k8s client
	Client *kubernetes.Clientset
	// Config is rest client config
	Config *rest.Config
}

func (c *ChangesetConfig) CheckAndSetDefaults() error {
	if c.Client == nil {
		return trace.BadParameter("missing parameter Client")
	}
	if c.Config == nil {
		return trace.BadParameter("missing parameter Config")
	}
	return nil
}

func NewChangeset(config ChangesetConfig) (*Changeset, error) {
	if err := config.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	cfg := *config.Config
	cfg.APIPath = "/apis"
	if cfg.UserAgent == "" {
		cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	cfg.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}
	cfg.GroupVersion = &unversioned.GroupVersion{Group: ChangesetGroup, Version: ChangesetVersion}

	clt, err := rest.RESTClientFor(&cfg)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &Changeset{ChangesetConfig: config, client: clt}, nil
}

// Changeset is a is a collection changeset log that can rollback a series of
// changes to the system
type Changeset struct {
	ChangesetConfig
	client *rest.RESTClient
}

// Upsert upserts resource in a context of a changeset
func (cs *Changeset) Upsert(ctx context.Context, changesetNamespace, changesetName string, data []byte) error {
	tr, err := cs.createOrRead(&ChangesetResource{
		TypeMeta: unversioned.TypeMeta{
			Kind:       KindChangeset,
			APIVersion: ChangesetAPIVersion,
		},
		ObjectMeta: v1.ObjectMeta{
			Name: changesetName,
		},
		Spec: ChangesetSpec{
			Status: ChangesetStatusInProgress,
		},
	})
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != ChangesetStatusInProgress {
		return trace.CompareFailed("can't update changeset that is not in state '%v'", ChangesetStatusInProgress)
	}
	var kind unversioned.TypeMeta
	err = yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 1024).Decode(&kind)
	if err != nil {
		return trace.Wrap(err)
	}
	switch kind.Kind {
	case KindDaemonSet:
		_, err = cs.upsertDaemonSet(ctx, tr, data)
		return err
	case KindReplicationController:
		_, err = cs.upsertRC(ctx, tr, data)
		return err
	case KindDeployment:
		_, err = cs.upsertDeployment(ctx, tr, data)
		return err
	}
	return trace.BadParameter("unsupported resource type %v", kind.Kind)
}

// Status checks all statuses for all resources updated or added in the context of a given changeset
func (cs *Changeset) Status(ctx context.Context, changesetNamespace, changesetName string, retryAttempts int, retryPeriod time.Duration) error {
	tr, err := cs.get(changesetNamespace, changesetName)
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status == ChangesetStatusReverted {
		return trace.CompareFailed("changeset has been reverted")
	}
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	return retry(ctx, retryAttempts, retryPeriod, func() error {
		for i, op := range tr.Spec.Items {
			switch op.Status {
			case OpStatusReverted:
				log.Infof("%v is rolled back, skip status check", i)
				continue
			case OpStatusCreated:
				return trace.BadParameter("%v is not completed yet", tr)
			case OpStatusCompleted:
				if op.To != "" {
					if err := cs.status(ctx, []byte(op.To)); err != nil {
						return trace.Wrap(err)
					}
				} else {
					log.Infof("%v has deleted resource, nothing to check", i)
				}
			default:
				return trace.BadParameter("unsupported operation status: %v", op.Status)
			}
		}
		return nil
	})
}

// DeleteResource deletes a resources in the context of a given changeset
func (cs *Changeset) DeleteResource(ctx context.Context, changesetNamespace, changesetName string, resourceNamespace string, resource Ref, cascade bool) error {
	if err := cs.Init(ctx); err != nil {
		return trace.Wrap(err)
	}
	tr, err := cs.createOrRead(&ChangesetResource{
		TypeMeta: unversioned.TypeMeta{
			Kind:       KindChangeset,
			APIVersion: ChangesetAPIVersion,
		},
		ObjectMeta: v1.ObjectMeta{
			Name: changesetName,
		},
		Spec: ChangesetSpec{
			Status: ChangesetStatusInProgress,
		},
	})
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != ChangesetStatusInProgress {
		return trace.CompareFailed("can't update changeset that is not in state '%v'", ChangesetStatusInProgress)
	}
	g := log.WithFields(log.Fields{
		"cs": tr.String(),
	})
	g.Infof("deleting %v from %v", resource, resourceNamespace)
	switch resource.Kind {
	case KindDaemonSet:
		return cs.deleteDaemonSet(ctx, tr, resourceNamespace, resource.Name, cascade)
	case KindReplicationController:
		return cs.deleteRC(ctx, tr, resourceNamespace, resource.Name, cascade)
	case KindDeployment:
		return cs.deleteDeployment(ctx, tr, resourceNamespace, resource.Name, cascade)
	}
	return trace.BadParameter("don't know how to delete %v yet", resource.Kind)
}

// Freeze "freezes" changeset, prohibits adding or removing any changes to it
func (cs *Changeset) Freeze(ctx context.Context, changesetNamespace, changesetName string) error {
	tr, err := cs.get(changesetNamespace, changesetName)
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != ChangesetStatusInProgress {
		return trace.CompareFailed("changeset is not in progress")
	}
	for i := len(tr.Spec.Items) - 1; i >= 0; i-- {
		item := &tr.Spec.Items[i]
		if item.Status != OpStatusCompleted {
			return trace.CompareFailed("operation %v is not completed", i)
		}
	}
	tr.Spec.Status = ChangesetStatusCommited
	_, err = cs.update(tr)
	return trace.Wrap(err)
}

// Revert rolls back all the operations in reverse order they were applied
func (cs *Changeset) Revert(ctx context.Context, changesetNamespace, changesetName string) error {
	tr, err := cs.get(changesetNamespace, changesetName)
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status == ChangesetStatusReverted {
		return trace.CompareFailed("changeset is already reverted")
	}
	g := log.WithFields(log.Fields{
		"tx": tr.String(),
	})
	g.Infof("roverting")
	for i := len(tr.Spec.Items) - 1; i >= 0; i-- {
		op := &tr.Spec.Items[i]
		if op.Status != OpStatusCompleted {
			g.Infof("skipping changeset item %v, status: %v is not %v", i, op.Status, OpStatusCompleted)
		}
		if err := cs.rollback(ctx, op); err != nil {
			return trace.Wrap(err)
		}
		op.Status = OpStatusReverted
		tr, err = cs.update(tr)
		if err != nil {
			return trace.Wrap(err)
		}
	}
	tr.Spec.Status = ChangesetStatusReverted
	_, err = cs.update(tr)
	return trace.Wrap(err)
}

func (cs *Changeset) status(ctx context.Context, data []byte) error {
	header, err := ParseResourceHeader(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	switch header.Kind {
	case KindDaemonSet:
		return cs.statusDaemonSet(ctx, data)
	case KindReplicationController:
		return cs.statusRC(ctx, data)
	case KindDeployment:
		return cs.statusDeployment(ctx, data)
	}
	return trace.BadParameter("unsupported resource type %v for resource %v", header.Kind, header.Name)
}

func (cs *Changeset) statusDaemonSet(ctx context.Context, data []byte) error {
	control, err := NewDSControl(DSConfig{Reader: bytes.NewReader(data), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status(ctx, 1, 0)
}

func (cs *Changeset) statusRC(ctx context.Context, data []byte) error {
	control, err := NewRCControl(RCConfig{Reader: bytes.NewReader(data), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status(ctx, 1, 0)
}

func (cs *Changeset) statusDeployment(ctx context.Context, data []byte) error {
	control, err := NewDeploymentControl(DeploymentConfig{Reader: bytes.NewReader(data), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status(ctx, 1, 0)
}

func (cs *Changeset) deleteDaemonSet(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	ds, err := cs.Client.Extensions().DaemonSets(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewDSControl(DSConfig{DaemonSet: ds, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, tr, ds, func() error {
		return control.Delete(ctx, cascade)
	})
}

func (cs *Changeset) withDeleteOp(ctx context.Context, tr *ChangesetResource, obj interface{}, fn func() error) error {
	data, err := goyaml.Marshal(obj)
	if err != nil {
		return trace.Wrap(err)
	}
	tr.Spec.Items = append(tr.Spec.Items, ChangesetItem{
		From:   string(data),
		Status: OpStatusCreated,
	})
	tr, err = cs.update(tr)
	if err != nil {
		return trace.Wrap(err)
	}
	err = fn()
	if err != nil {
		return trace.Wrap(err)
	}
	tr.Spec.Items[len(tr.Spec.Items)-1].Status = OpStatusCompleted
	_, err = cs.update(tr)
	return err
}

func (cs *Changeset) deleteRC(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	rc, err := cs.Client.Core().ReplicationControllers(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewRCControl(RCConfig{ReplicationController: rc, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, tr, rc, func() error {
		return control.Delete(ctx, cascade)
	})
}

func (cs *Changeset) deleteDeployment(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	deployment, err := cs.Client.Extensions().Deployments(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewDeploymentControl(DeploymentConfig{Deployment: deployment, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, tr, deployment, func() error {
		return control.Delete(ctx, cascade)
	})
}

func (cs *Changeset) rollback(ctx context.Context, item *ChangesetItem) error {
	info, err := GetOperationInfo(*item)
	if err != nil {
		if err != nil {
			return trace.Wrap(err)
		}
	}
	kind := info.Kind()
	switch info.Kind() {
	case KindDaemonSet:
		return cs.rollbackDaemonSet(ctx, item)
	case KindReplicationController:
		return cs.rollbackRC(ctx, item)
	case KindDeployment:
		return cs.rollbackDeployment(ctx, item)
	}
	return trace.BadParameter("unsupported resource type %v", kind)
}

func (cs *Changeset) rollbackDaemonSet(ctx context.Context, item *ChangesetItem) error {
	// this operation created daemon set, so we will delete it
	if len(item.From) == 0 {
		control, err := NewDSControl(DSConfig{Reader: strings.NewReader(item.To), Client: cs.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return control.Delete(ctx, true)
	}
	// this operation either created or updated daemon set, so we create a new version
	control, err := NewDSControl(DSConfig{Reader: strings.NewReader(item.From), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Upsert(ctx)
}

func (cs *Changeset) rollbackRC(ctx context.Context, item *ChangesetItem) error {
	// this operation created RC, so we will delete it
	if len(item.From) == 0 {
		control, err := NewRCControl(RCConfig{Reader: strings.NewReader(item.To), Client: cs.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return control.Delete(ctx, true)
	}
	// this operation either created or updated RC, so we create a new version
	control, err := NewRCControl(RCConfig{Reader: strings.NewReader(item.From), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Upsert(ctx)
}

func (cs *Changeset) rollbackDeployment(ctx context.Context, item *ChangesetItem) error {
	// this operation created Deployment, so we will delete it
	if len(item.From) == 0 {
		control, err := NewDeploymentControl(DeploymentConfig{Reader: strings.NewReader(item.To), Client: cs.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return control.Delete(ctx, true)
	}
	// this operation either created or updated Deployment, so we create a new version
	control, err := NewDeploymentControl(DeploymentConfig{Reader: strings.NewReader(item.From), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Upsert(ctx)
}

func (cs *Changeset) withUpsertOp(ctx context.Context, tr *ChangesetResource, old interface{}, new interface{}, fn func() error) (*ChangesetResource, error) {
	to, err := goyaml.Marshal(new)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	item := ChangesetItem{
		To:     string(to),
		Status: OpStatusCreated,
	}
	if !reflect.ValueOf(old).IsNil() {
		from, err := goyaml.Marshal(old)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		item.From = string(from)
	}
	tr.Spec.Items = append(tr.Spec.Items, item)
	tr, err = cs.update(tr)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if err := fn(); err != nil {
		return nil, trace.Wrap(err)
	}
	tr.Spec.Items[len(tr.Spec.Items)-1].Status = OpStatusCompleted
	return cs.update(tr)
}

func (cs *Changeset) upsertDaemonSet(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	ds, err := ParseDaemonSet(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	g := log.WithFields(log.Fields{
		"cs": tr.String(),
		"ds": fmt.Sprintf("%v/%v", ds.Namespace, ds.Name),
	})
	g.Infof("upsert")
	daemons := cs.Client.Extensions().DaemonSets(ds.Namespace)
	currentDS, err := daemons.Get(ds.Name)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		g.Infof("existing daemonset is not found")
		currentDS = nil
	}
	control, err := NewDSControl(DSConfig{DaemonSet: ds, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return cs.withUpsertOp(ctx, tr, currentDS, ds, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertRC(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	rc, err := ParseReplicationController(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	g := log.WithFields(log.Fields{
		"cs": tr.String(),
		"rc": fmt.Sprintf("%v/%v", rc.Namespace, rc.Name),
	})
	g.Infof("upsert")
	rcs := cs.Client.Core().ReplicationControllers(rc.Namespace)
	currentRC, err := rcs.Get(rc.Name)
	err = convertErr(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		g.Infof("existing RC not found")
		currentRC = nil
	}
	control, err := NewRCControl(RCConfig{ReplicationController: rc, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return cs.withUpsertOp(ctx, tr, currentRC, rc, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertDeployment(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	deployment, err := ParseDeployment(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	g := log.WithFields(log.Fields{
		"cs":         tr.String(),
		"deployment": fmt.Sprintf("%v/%v", deployment.Namespace, deployment.Name),
	})
	g.Infof("upsert")
	deployments := cs.Client.Extensions().Deployments(deployment.Namespace)
	currentDeployment, err := deployments.Get(deployment.Name)
	err = convertErr(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		g.Infof("existing Deployment not found")
		currentDeployment = nil
	}
	control, err := NewDeploymentControl(DeploymentConfig{Deployment: deployment, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return cs.withUpsertOp(ctx, tr, currentDeployment, deployment, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) Init(ctx context.Context) error {
	log.Infof("Changeset init")
	tpr := &v1beta1.ThirdPartyResource{
		ObjectMeta: v1.ObjectMeta{
			Name: ChangesetResourceName,
		},
		Versions: []v1beta1.APIVersion{
			{Name: ChangesetVersion},
		},
		Description: "Changeset",
	}
	_, err := cs.Client.Extensions().ThirdPartyResources().Create(tpr)
	err = convertErr(err)
	if err != nil {
		if !trace.IsAlreadyExists(err) {
			return trace.Wrap(err)
		}
	}
	// wait for the controller to init by trying to list stuff
	return retry(ctx, 30, time.Second, func() error {
		_, err := cs.list(DefaultNamespace)
		return err
	})
}

func (cs *Changeset) Get(ctx context.Context, namespace, name string) (*ChangesetResource, error) {
	if err := cs.Init(ctx); err != nil {
		return nil, trace.Wrap(err)
	}
	return cs.get(namespace, name)
}

func (cs *Changeset) List(ctx context.Context, namespace string) (*ChangesetList, error) {
	if err := cs.Init(ctx); err != nil {
		return nil, trace.Wrap(err)
	}
	return cs.list(namespace)
}

func (cs *Changeset) upsert(tr *ChangesetResource) (*ChangesetResource, error) {
	out, err := cs.create(tr)
	if err == nil {
		return out, nil
	}
	if !trace.IsAlreadyExists(err) {
		return nil, err
	}
	return cs.update(tr)
}

func (cs *Changeset) create(tr *ChangesetResource) (*ChangesetResource, error) {
	tr.Namespace = Namespace(tr.Namespace)
	data, err := json.Marshal(tr)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var raw runtime.Unknown
	err = cs.client.Post().
		SubResource("namespaces", tr.Namespace, ChangesetCollection).
		Body(data).
		Do().
		Into(&raw)
	if err != nil {
		return nil, convertErr(err)
	}
	var result ChangesetResource
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func (cs *Changeset) get(namespace, name string) (*ChangesetResource, error) {
	var raw runtime.Unknown
	err := cs.client.Get().
		SubResource("namespaces", namespace, ChangesetCollection, name).
		Do().
		Into(&raw)
	if err != nil {
		return nil, convertErr(err)
	}
	var result ChangesetResource
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func (cs *Changeset) createOrRead(tr *ChangesetResource) (*ChangesetResource, error) {
	out, err := cs.create(tr)
	if err == nil {
		return out, nil
	}
	if !trace.IsAlreadyExists(err) {
		return nil, trace.Wrap(err)
	}
	return cs.get(tr.Namespace, tr.Name)
}

func (cs *Changeset) Delete(ctx context.Context, namespace, name string) error {
	if err := cs.Init(ctx); err != nil {
		return trace.Wrap(err)
	}
	var raw runtime.Unknown
	err := cs.client.Delete().
		SubResource("namespaces", namespace, ChangesetCollection, name).
		Do().
		Into(&raw)
	if err != nil {
		return convertErr(err)
	}
	return nil
}

func (cs *Changeset) update(tr *ChangesetResource) (*ChangesetResource, error) {
	tr.Namespace = Namespace(tr.Namespace)
	data, err := json.Marshal(tr)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var raw runtime.Unknown
	err = cs.client.Put().
		SubResource("namespaces", tr.Namespace, ChangesetCollection, tr.Name).
		Body(data).
		Do().
		Into(&raw)
	if err != nil {
		return nil, convertErr(err)
	}
	var result ChangesetResource
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func (cs *Changeset) list(namespace string) (*ChangesetList, error) {
	var raw runtime.Unknown
	err := cs.client.Get().
		SubResource("namespaces", namespace, ChangesetCollection).
		Do().
		Into(&raw)
	if err != nil {
		return nil, convertErr(err)
	}
	var result ChangesetList
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func convertErr(err error) error {
	if err == nil {
		return nil
	}
	se, ok := err.(*errors.StatusError)
	if !ok {
		return err
	}
	if se.Status().Code == http.StatusConflict && se.Status().Reason == unversioned.StatusReasonAlreadyExists {
		return trace.AlreadyExists(err.Error())
	}
	if se.Status().Code == http.StatusNotFound {
		return trace.NotFound(err.Error())
	}
	return err
}
