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
	"io"
	"reflect"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	goyaml "github.com/ghodss/yaml"
	"github.com/gravitational/trace"
	"k8s.io/client-go/1.4/kubernetes"
	api "k8s.io/client-go/1.4/pkg/api"
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
		return nil, ConvertError(err)
	}
	return &Changeset{ChangesetConfig: config, client: clt}, nil
}

// Changeset is a is a collection changeset log that can revert a series of
// changes to the system
type Changeset struct {
	ChangesetConfig
	client *rest.RESTClient
}

// Upsert upserts resource in a context of a changeset
func (cs *Changeset) Upsert(ctx context.Context, changesetNamespace, changesetName string, data []byte) error {
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), DefaultBufferSize)

	for {
		var raw runtime.Unknown
		err := decoder.Decode(&raw)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return trace.Wrap(err)
		}
		err = cs.upsertResource(ctx, changesetNamespace, changesetName, raw.Raw)
		if err != nil {
			return trace.Wrap(err)
		}
	}
}

func (cs *Changeset) upsertResource(ctx context.Context, changesetNamespace, changesetName string, data []byte) error {
	tr, err := cs.createOrRead(&ChangesetResource{
		TypeMeta: unversioned.TypeMeta{
			Kind:       KindChangeset,
			APIVersion: ChangesetAPIVersion,
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      changesetName,
			Namespace: changesetNamespace,
		},
		Spec: ChangesetSpec{
			Status: ChangesetStatusInProgress,
		},
	})
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != ChangesetStatusInProgress {
		return trace.CompareFailed("cannot update changeset - expected status %q, got %q", ChangesetStatusInProgress, tr.Spec.Status)
	}
	var kind unversioned.TypeMeta
	err = yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), DefaultBufferSize).Decode(&kind)
	if err != nil {
		return trace.Wrap(err)
	}
	switch kind.Kind {
	case KindJob:
		_, err = cs.upsertJob(ctx, tr, data)
	case KindDaemonSet:
		_, err = cs.upsertDaemonSet(ctx, tr, data)
	case KindReplicationController:
		_, err = cs.upsertRC(ctx, tr, data)
	case KindDeployment:
		_, err = cs.upsertDeployment(ctx, tr, data)
	case KindService:
		_, err = cs.upsertService(ctx, tr, data)
	case KindConfigMap:
		_, err = cs.upsertConfigMap(ctx, tr, data)
	case KindSecret:
		_, err = cs.upsertSecret(ctx, tr, data)
	default:
		return trace.BadParameter("unsupported resource type %v", kind.Kind)
	}
	return trace.Wrap(err)
}

// Status checks all statuses for all resources updated or added in the context of a given changeset
func (cs *Changeset) Status(ctx context.Context, changesetNamespace, changesetName string, retryAttempts int, retryPeriod time.Duration) error {
	tr, err := cs.get(changesetNamespace, changesetName)
	if err != nil {
		return trace.Wrap(err)
	}

	// Fast path for certain states
	switch tr.Spec.Status {
	case ChangesetStatusCommitted:
		// Nothing to do
		return nil
	}

	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}

	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}

	return retry(ctx, retryAttempts, retryPeriod, func() error {
		for _, op := range tr.Spec.Items {
			switch op.Status {
			case OpStatusCreated:
				return trace.BadParameter("%v is not completed yet", tr)
			case OpStatusCompleted, OpStatusReverted:
				if op.To != "" {
					err := cs.status(ctx, []byte(op.To))
					if err != nil {
						if op.Status != OpStatusReverted || !trace.IsNotFound(err) {
							return trace.Wrap(err)
						}
					}
				} else {
					info, err := GetOperationInfo(op)
					if err == nil {
						log.Debugf("%v has deleted resource, nothing to check", info)
					}
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
			Name:      changesetName,
			Namespace: changesetNamespace,
		},
		Spec: ChangesetSpec{
			Status: ChangesetStatusInProgress,
		},
	})
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != ChangesetStatusInProgress {
		return trace.CompareFailed("cannot update changeset - expected status %q, got %q", ChangesetStatusInProgress, tr.Spec.Status)
	}
	log := log.WithFields(log.Fields{
		"cs": tr.String(),
	})
	log.Infof("deleting %v/%s", resourceNamespace, resource)
	switch resource.Kind {
	case KindDaemonSet:
		err = cs.deleteDaemonSet(ctx, tr, resourceNamespace, resource.Name, cascade)
	case KindJob:
		err = cs.deleteJob(ctx, tr, resourceNamespace, resource.Name, cascade)
	case KindReplicationController:
		err = cs.deleteRC(ctx, tr, resourceNamespace, resource.Name, cascade)
	case KindDeployment:
		err = cs.deleteDeployment(ctx, tr, resourceNamespace, resource.Name, cascade)
	case KindSecret:
		err = cs.deleteSecret(ctx, tr, resourceNamespace, resource.Name, cascade)
	case KindConfigMap:
		err = cs.deleteConfigMap(ctx, tr, resourceNamespace, resource.Name, cascade)
	case KindService:
		err = cs.deleteService(ctx, tr, resourceNamespace, resource.Name, cascade)
	default:
		return trace.BadParameter("delete: unimplemented resource %v", resource.Kind)
	}
	return trace.Wrap(err)
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
	tr.Spec.Status = ChangesetStatusCommitted
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
	log := log.WithFields(log.Fields{
		"cs": tr.String(),
	})
	for i := len(tr.Spec.Items) - 1; i >= 0; i-- {
		op := &tr.Spec.Items[i]
		info, err := GetOperationInfo(*op)
		if err != nil {
			return trace.Wrap(err)
		}
		if op.Status != OpStatusCompleted {
			log.Infof("skipping changeset item %v, status: %v is not the expected %v", info, op.Status, OpStatusCompleted)
		}
		err = cs.revert(ctx, op, info)
		if err != nil {
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
		err = cs.statusDaemonSet(ctx, data)
	case KindJob:
		err = cs.statusJob(ctx, data)
	case KindReplicationController:
		err = cs.statusRC(ctx, data)
	case KindDeployment:
		err = cs.statusDeployment(ctx, data)
	case KindService:
		err = cs.statusService(ctx, data)
	case KindSecret:
		err = cs.statusSecret(ctx, data)
	case KindConfigMap:
		err = cs.statusConfigMap(ctx, data)
	default:
		return trace.BadParameter("unsupported resource type %v for resource %v", header.Kind, header.Name)
	}
	return trace.Wrap(err)
}

func (cs *Changeset) statusDaemonSet(ctx context.Context, data []byte) error {
	control, err := NewDSControl(DSConfig{Reader: bytes.NewReader(data), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Status(ctx, 1, 0))
}

func (cs *Changeset) statusJob(ctx context.Context, data []byte) error {
	job, err := ParseJob(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewJobControl(JobConfig{job: *job, Clientset: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Status(ctx, 1, 0))
}

func (cs *Changeset) statusRC(ctx context.Context, data []byte) error {
	control, err := NewRCControl(RCConfig{Reader: bytes.NewReader(data), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Status(ctx, 1, 0))
}

func (cs *Changeset) statusDeployment(ctx context.Context, data []byte) error {
	control, err := NewDeploymentControl(DeploymentConfig{Reader: bytes.NewReader(data), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Status(ctx, 1, 0))
}

func (cs *Changeset) statusService(ctx context.Context, data []byte) error {
	control, err := NewServiceControl(ServiceConfig{Reader: bytes.NewReader(data), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Status(ctx, 1, 0))
}

func (cs *Changeset) statusSecret(ctx context.Context, data []byte) error {
	control, err := NewSecretControl(SecretConfig{Reader: bytes.NewReader(data), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Status(ctx, 1, 0))
}

func (cs *Changeset) statusConfigMap(ctx context.Context, data []byte) error {
	control, err := NewConfigMapControl(ConfigMapConfig{Reader: bytes.NewReader(data), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Status(ctx, 1, 0))
}

func (cs *Changeset) withDeleteOp(ctx context.Context, tr *ChangesetResource, obj interface{}, fn func() error) error {
	data, err := goyaml.Marshal(obj)
	if err != nil {
		return trace.Wrap(err)
	}
	tr.Spec.Items = append(tr.Spec.Items, ChangesetItem{
		From:              string(data),
		Status:            OpStatusCreated,
		CreationTimestamp: time.Now().UTC(),
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
	return trace.Wrap(err)
}

func (cs *Changeset) deleteDaemonSet(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	ds, err := cs.Client.Extensions().DaemonSets(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(ConvertError(err))
	}
	control, err := NewDSControl(DSConfig{DaemonSet: ds, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.withDeleteOp(ctx, tr, ds, func() error {
		return trace.Wrap(control.Delete(ctx, cascade))
	})
	return trace.Wrap(err)
}

func (cs *Changeset) deleteJob(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	job, err := cs.Client.Batch().Jobs(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(ConvertError(err))
	}
	control, err := NewJobControl(JobConfig{job: *job, Clientset: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.withDeleteOp(ctx, tr, job, func() error {
		return trace.Wrap(control.Delete(ctx, cascade))
	})
	return trace.Wrap(err)
}

func (cs *Changeset) deleteRC(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	rc, err := cs.Client.Core().ReplicationControllers(Namespace(namespace)).Get(name)
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewRCControl(RCConfig{ReplicationController: rc, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, tr, rc, func() error {
		return trace.Wrap(control.Delete(ctx, cascade))
	})
	return trace.Wrap(err)
}

func (cs *Changeset) deleteDeployment(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	deployment, err := cs.Client.Extensions().Deployments(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(ConvertError(err))
	}
	control, err := NewDeploymentControl(DeploymentConfig{Deployment: deployment, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.withDeleteOp(ctx, tr, deployment, func() error {
		return trace.Wrap(control.Delete(ctx, cascade))
	})
	return trace.Wrap(err)
}

func (cs *Changeset) deleteService(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	service, err := cs.Client.Core().Services(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(ConvertError(err))
	}
	control, err := NewServiceControl(ServiceConfig{Service: service, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, tr, service, func() error {
		return trace.Wrap(control.Delete(ctx, cascade))
	})
	return trace.Wrap(err)
}

func (cs *Changeset) deleteConfigMap(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	configMap, err := cs.Client.Core().ConfigMaps(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(ConvertError(err))
	}
	control, err := NewConfigMapControl(ConfigMapConfig{ConfigMap: configMap, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.withDeleteOp(ctx, tr, configMap, func() error {
		return trace.Wrap(control.Delete(ctx, cascade))
	})
	return trace.Wrap(err)
}

func (cs *Changeset) deleteSecret(ctx context.Context, tr *ChangesetResource, namespace, name string, cascade bool) error {
	secret, err := cs.Client.Core().Secrets(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(ConvertError(err))
	}
	control, err := NewSecretControl(SecretConfig{Secret: secret, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	err = cs.withDeleteOp(ctx, tr, secret, func() error {
		return trace.Wrap(control.Delete(ctx, cascade))
	})
	return trace.Wrap(err)
}

func (cs *Changeset) revert(ctx context.Context, item *ChangesetItem, info *OperationInfo) (err error) {
	log.Debugf("reverting %v", info)

	kind := info.Kind()
	switch info.Kind() {
	case KindDaemonSet:
		err = cs.revertDaemonSet(ctx, item)
	case KindJob:
		err = cs.revertJob(ctx, item)
	case KindReplicationController:
		err = cs.revertRC(ctx, item)
	case KindDeployment:
		err = cs.revertDeployment(ctx, item)
	case KindService:
		err = cs.revertService(ctx, item)
	case KindSecret:
		err = cs.revertSecret(ctx, item)
	case KindConfigMap:
		err = cs.revertConfigMap(ctx, item)
	default:
		return trace.BadParameter("unsupported resource type %v", kind)
	}
	return trace.Wrap(err)
}

func (cs *Changeset) revertDaemonSet(ctx context.Context, item *ChangesetItem) error {
	// this operation created daemon set, so we will delete it
	if len(item.From) == 0 {
		control, err := NewDSControl(DSConfig{Reader: strings.NewReader(item.To), Client: cs.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return trace.Wrap(control.Delete(ctx, true))
	}
	// this operation either created or updated daemon set, so we create a new version
	control, err := NewDSControl(DSConfig{Reader: strings.NewReader(item.From), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Upsert(ctx))
}

func (cs *Changeset) revertJob(ctx context.Context, item *ChangesetItem) error {
	jobSource := item.From
	if len(jobSource) == 0 {
		jobSource = item.To
	}

	job, err := ParseJob(strings.NewReader(jobSource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewJobControl(JobConfig{job: *job, Clientset: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}

	if len(item.From) == 0 {
		// this operation created the job, so we will delete it
		return trace.Wrap(control.Delete(ctx, true))
	}
	// this operation either created or updated the job, so we create a new version
	return trace.Wrap(control.Upsert(ctx))
}

func (cs *Changeset) revertRC(ctx context.Context, item *ChangesetItem) error {
	// this operation created RC, so we will delete it
	if len(item.From) == 0 {
		control, err := NewRCControl(RCConfig{Reader: strings.NewReader(item.To), Client: cs.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return trace.Wrap(control.Delete(ctx, true))
	}
	// this operation either created or updated RC, so we create a new version
	control, err := NewRCControl(RCConfig{Reader: strings.NewReader(item.From), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Upsert(ctx))
}

func (cs *Changeset) revertDeployment(ctx context.Context, item *ChangesetItem) error {
	// this operation created Deployment, so we will delete it
	if len(item.From) == 0 {
		control, err := NewDeploymentControl(DeploymentConfig{Reader: strings.NewReader(item.To), Client: cs.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return trace.Wrap(control.Delete(ctx, true))
	}
	// this operation either created or updated Deployment, so we create a new version
	control, err := NewDeploymentControl(DeploymentConfig{Reader: strings.NewReader(item.From), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Upsert(ctx))
}

func (cs *Changeset) revertService(ctx context.Context, item *ChangesetItem) error {
	// this operation created Service, so we will delete it
	if len(item.From) == 0 {
		control, err := NewServiceControl(ServiceConfig{Reader: strings.NewReader(item.To), Client: cs.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return trace.Wrap(control.Delete(ctx, true))
	}
	// this operation either created or updated Service, so we create a new version
	control, err := NewServiceControl(ServiceConfig{Reader: strings.NewReader(item.From), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Upsert(ctx))
}

func (cs *Changeset) revertConfigMap(ctx context.Context, item *ChangesetItem) error {
	// this operation created ConfigMap, so we will delete it
	if len(item.From) == 0 {
		control, err := NewConfigMapControl(ConfigMapConfig{Reader: strings.NewReader(item.To), Client: cs.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return trace.Wrap(control.Delete(ctx, true))
	}
	// this operation either created or updated ConfigMap, so we create a new version
	control, err := NewConfigMapControl(ConfigMapConfig{Reader: strings.NewReader(item.From), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Upsert(ctx))
}

func (cs *Changeset) revertSecret(ctx context.Context, item *ChangesetItem) error {
	// this operation created Secret, so we will delete it
	if len(item.From) == 0 {
		control, err := NewSecretControl(SecretConfig{Reader: strings.NewReader(item.To), Client: cs.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return trace.Wrap(control.Delete(ctx, true))
	}
	// this operation either created or updated Secret, so we create a new version
	control, err := NewSecretControl(SecretConfig{Reader: strings.NewReader(item.From), Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return trace.Wrap(control.Upsert(ctx))
}

func (cs *Changeset) withUpsertOp(ctx context.Context, tr *ChangesetResource, old interface{}, new interface{}, fn func() error) (*ChangesetResource, error) {
	to, err := goyaml.Marshal(new)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	item := ChangesetItem{
		CreationTimestamp: time.Now().UTC(),
		To:                string(to),
		Status:            OpStatusCreated,
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
	result, err := cs.update(tr)
	return result, trace.Wrap(err)
}

func (cs *Changeset) upsertJob(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	job, err := ParseJob(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs":  tr.String(),
		"job": fmt.Sprintf("%v/%v", job.Namespace, job.Name),
	})
	log.Infof("upsert job %v", formatMeta(job.ObjectMeta))
	jobs := cs.Client.Extensions().Jobs(job.Namespace)
	currentJob, err := jobs.Get(job.Name)
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Info("existing job not found")
		currentJob = nil
	}

	control, err := NewJobControl(JobConfig{job: *job, Clientset: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	result, err := cs.withUpsertOp(ctx, tr, currentJob, job, func() error {
		return trace.Wrap(control.Upsert(ctx))
	})
	return result, trace.Wrap(err)
}

func (cs *Changeset) upsertDaemonSet(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	ds, err := ParseDaemonSet(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": tr.String(),
		"ds": fmt.Sprintf("%v/%v", ds.Namespace, ds.Name),
	})
	log.Infof("upsert daemon set %v", formatMeta(ds.ObjectMeta))
	daemons := cs.Client.Extensions().DaemonSets(ds.Namespace)
	currentDS, err := daemons.Get(ds.Name)
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Infof("existing daemonset not found")
		currentDS = nil
	}
	control, err := NewDSControl(DSConfig{DaemonSet: ds, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	result, err := cs.withUpsertOp(ctx, tr, currentDS, ds, func() error {
		return trace.Wrap(control.Upsert(ctx))
	})
	return result, trace.Wrap(err)
}

func (cs *Changeset) upsertRC(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	rc, err := ParseReplicationController(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": tr.String(),
		"rc": fmt.Sprintf("%v/%v", rc.Namespace, rc.Name),
	})
	log.Infof("upsert replication controller %v", formatMeta(rc.ObjectMeta))
	rcs := cs.Client.Core().ReplicationControllers(rc.Namespace)
	currentRC, err := rcs.Get(rc.Name)
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Infof("existing replication controller not found")
		currentRC = nil
	}
	control, err := NewRCControl(RCConfig{ReplicationController: rc, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	result, err := cs.withUpsertOp(ctx, tr, currentRC, rc, func() error {
		return trace.Wrap(control.Upsert(ctx))
	})
	return result, trace.Wrap(err)
}

func (cs *Changeset) upsertDeployment(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	deployment, err := ParseDeployment(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs":         tr.String(),
		"deployment": fmt.Sprintf("%v/%v", deployment.Namespace, deployment.Name),
	})
	log.Infof("upsert deployment %v", formatMeta(deployment.ObjectMeta))
	deployments := cs.Client.Extensions().Deployments(deployment.Namespace)
	currentDeployment, err := deployments.Get(deployment.Name)
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Infof("existing deployment not found")
		currentDeployment = nil
	}
	control, err := NewDeploymentControl(DeploymentConfig{Deployment: deployment, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	result, err := cs.withUpsertOp(ctx, tr, currentDeployment, deployment, func() error {
		return trace.Wrap(control.Upsert(ctx))
	})
	return result, trace.Wrap(err)
}

func (cs *Changeset) upsertService(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	service, err := ParseService(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs":      tr.String(),
		"service": fmt.Sprintf("%v/%v", service.Namespace, service.Name),
	})
	log.Infof("upsert service %v", formatMeta(service.ObjectMeta))
	services := cs.Client.Core().Services(service.Namespace)
	currentService, err := services.Get(service.Name)
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Infof("existing service not found")
		currentService = nil
	}
	control, err := NewServiceControl(ServiceConfig{Service: service, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	result, err := cs.withUpsertOp(ctx, tr, currentService, service, func() error {
		return trace.Wrap(control.Upsert(ctx))
	})
	return result, trace.Wrap(err)
}

func (cs *Changeset) upsertConfigMap(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	configMap, err := ParseConfigMap(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs":        tr.String(),
		"configMap": fmt.Sprintf("%v/%v", configMap.Namespace, configMap.Name),
	})
	log.Infof("upsert configmap %v", formatMeta(configMap.ObjectMeta))
	configMaps := cs.Client.Core().ConfigMaps(configMap.Namespace)
	currentConfigMap, err := configMaps.Get(configMap.Name)
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Infof("existing configmap not found")
		currentConfigMap = nil
	}
	control, err := NewConfigMapControl(ConfigMapConfig{ConfigMap: configMap, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	result, err := cs.withUpsertOp(ctx, tr, currentConfigMap, configMap, func() error {
		return trace.Wrap(control.Upsert(ctx))
	})
	return result, trace.Wrap(err)
}

func (cs *Changeset) upsertSecret(ctx context.Context, tr *ChangesetResource, data []byte) (*ChangesetResource, error) {
	secret, err := ParseSecret(bytes.NewReader(data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs":     tr.String(),
		"secret": fmt.Sprintf("%v/%v", secret.Namespace, secret.Name),
	})
	log.Infof("upsert secret %v", formatMeta(secret.ObjectMeta))
	secrets := cs.Client.Core().Secrets(secret.Namespace)
	currentSecret, err := secrets.Get(secret.Name)
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Infof("existing secret not found")
		currentSecret = nil
	}
	control, err := NewSecretControl(SecretConfig{Secret: secret, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	result, err := cs.withUpsertOp(ctx, tr, currentSecret, secret, func() error {
		return trace.Wrap(control.Upsert(ctx))
	})
	return result, trace.Wrap(err)
}

func (cs *Changeset) Init(ctx context.Context) error {
	log.Info("changeset init")
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
	err = ConvertError(err)
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
	result, err := cs.get(namespace, name)
	return result, trace.Wrap(err)
}

func (cs *Changeset) List(ctx context.Context, namespace string) (*ChangesetList, error) {
	if err := cs.Init(ctx); err != nil {
		return nil, trace.Wrap(err)
	}
	result, err := cs.list(namespace)
	return result, trace.Wrap(err)
}

func (cs *Changeset) upsert(tr *ChangesetResource) (*ChangesetResource, error) {
	out, err := cs.create(tr)
	if err == nil {
		return out, nil
	}
	if !trace.IsAlreadyExists(err) {
		return nil, err
	}
	result, err := cs.update(tr)
	return result, trace.Wrap(err)
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
		log.Errorf("failed to create changeset resource: %v\n%s", err, data)
		return nil, ConvertError(err)
	}
	var result ChangesetResource
	err = json.Unmarshal(raw.Raw, &result)
	return &result, trace.Wrap(err)
}

func (cs *Changeset) get(namespace, name string) (*ChangesetResource, error) {
	var raw runtime.Unknown
	err := cs.client.Get().
		SubResource("namespaces", namespace, ChangesetCollection, name).
		Do().
		Into(&raw)
	if err != nil {
		return nil, ConvertError(err)
	}
	var result ChangesetResource
	err = json.Unmarshal(raw.Raw, &result)
	return &result, trace.Wrap(err)
}

func (cs *Changeset) createOrRead(tr *ChangesetResource) (*ChangesetResource, error) {
	out, err := cs.create(tr)
	if err == nil {
		return out, nil
	}
	if !trace.IsAlreadyExists(err) {
		return nil, trace.Wrap(err)
	}
	result, err := cs.get(tr.Namespace, tr.Name)
	return result, trace.Wrap(err)
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
		return trace.Wrap(ConvertError(err))
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
		return nil, trace.Wrap(ConvertError(err))
	}
	var result ChangesetResource
	err = json.Unmarshal(raw.Raw, &result)
	return &result, trace.Wrap(err)
}

func (cs *Changeset) list(namespace string) (*ChangesetList, error) {
	var raw runtime.Unknown
	err := cs.client.Get().
		SubResource("namespaces", namespace, ChangesetCollection).
		Do().
		Into(&raw)
	if err != nil {
		return nil, trace.Wrap(ConvertError(err))
	}
	var result ChangesetList
	err = json.Unmarshal(raw.Raw, &result)
	return &result, trace.Wrap(err)
}
