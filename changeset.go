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
	"io"
	"reflect"
	"strings"
	"time"

	goyaml "github.com/ghodss/yaml"
	"github.com/google/uuid"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type ChangesetConfig struct {
	// Client is k8s client
	Client *kubernetes.Clientset
	// Config is rest client config
	Config *rest.Config
	// Name of the changeset resource
	Name string
	// Namespace for the changeset resource
	Namespace string
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

func NewChangeset(ctx context.Context, config ChangesetConfig) (*Changeset, error) {
	if err := config.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	cfg := *config.Config
	cfg.APIPath = "/apis"
	if cfg.UserAgent == "" {
		cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	cfg.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: scheme.Codecs}
	cfg.GroupVersion = &schema.GroupVersion{Group: ChangesetGroup, Version: ChangesetVersion}

	clt, err := rest.RESTClientFor(&cfg)
	if err != nil {
		return nil, ConvertError(err)
	}

	apiclient, err := apiextensionsclientset.NewForConfig(&cfg)
	if err != nil {
		return nil, ConvertError(err)
	}

	cs := &Changeset{ChangesetConfig: config, client: clt, APIExtensionsClient: apiclient}
	if err := cs.Init(ctx); err != nil {
		return nil, trace.Wrap(err)
	}

	return cs, nil
}

// Changeset is a is a collection changeset log that can revert a series of
// changes to the system
type Changeset struct {
	ChangesetConfig
	client *rest.RESTClient
	// APIExtensionsClient is a client for the extensions server
	APIExtensionsClient *apiextensionsclientset.Clientset
}

// Upsert upserts resource in a context of a changeset
func (cs *Changeset) Upsert(ctx context.Context, data []byte, force bool) error {
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
		err = cs.upsertResource(ctx, cs.Namespace, cs.Name, raw.Raw, force)
		if err != nil {
			return trace.Wrap(err)
		}
	}
}

func (cs *Changeset) upsertResource(ctx context.Context, changesetNamespace, changesetName string, data []byte, force bool) error {
	tr, err := cs.createOrRead(ChangesetSpec{Status: ChangesetStatusInProgress})
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != ChangesetStatusInProgress {
		return trace.CompareFailed("cannot update changeset - expected status %q, got %q", ChangesetStatusInProgress, tr.Spec.Status)
	}
	headers, err := ParseResourceHeader(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}

	log.WithFields(log.Fields{
		"cs": tr.String(),
	}).Infof("Updating %v", formatMeta(headers.ObjectMeta, headers.TypeMeta))

	completed, err := cs.isOperationCompleted(ctx, tr, data)
	if completed && !force {
		log.WithFields(log.Fields{
			"cs": tr.String(),
		}).Infof("Resource %v is already up-to-date.", formatMeta(headers.ObjectMeta, headers.TypeMeta))
		return nil
	}

	// TODO Sergei: Provide instructions how to revert or delete a failed operation
	if err != nil {
		return trace.Wrap(err)
	}

	ur := upsertResource{
		resource: tr,
		data:     data,
	}

	switch headers.TypeMeta.Kind {
	case KindJob:
		_, err = cs.upsertJob(ctx, ur)
	case KindDaemonSet:
		_, err = cs.upsertDaemonSet(ctx, ur)
	case KindStatefulSet:
		_, err = cs.upsertStatefulSet(ctx, ur)
	case KindReplicationController:
		_, err = cs.upsertRC(ctx, ur)
	case KindDeployment:
		_, err = cs.upsertDeployment(ctx, ur)
	case KindService:
		_, err = cs.upsertService(ctx, ur)
	case KindServiceAccount:
		_, err = cs.upsertServiceAccount(ctx, ur)
	case KindConfigMap:
		_, err = cs.upsertConfigMap(ctx, ur)
	case KindSecret:
		_, err = cs.upsertSecret(ctx, ur)
	case KindRole:
		_, err = cs.upsertRole(ctx, ur)
	case KindClusterRole:
		_, err = cs.upsertClusterRole(ctx, ur)
	case KindRoleBinding:
		_, err = cs.upsertRoleBinding(ctx, ur)
	case KindClusterRoleBinding:
		_, err = cs.upsertClusterRoleBinding(ctx, ur)
	case KindPodSecurityPolicy:
		_, err = cs.upsertPodSecurityPolicy(ctx, ur)
	default:
		return trace.BadParameter("unsupported resource type %v", headers.TypeMeta.Kind)
	}
	return err
}

// isOperationCompleted checks whether operation is completed in the context of a given changeset
func (cs *Changeset) isOperationCompleted(ctx context.Context, tr *ChangesetResource, data []byte) (bool, error) {
	headers, err := ParseResourceHeader(bytes.NewReader(data))
	if err != nil {
		return false, trace.Wrap(err)
	}
	for _, op := range tr.Spec.Items {
		info, err := GetOperationInfo(op)
		if err != nil {
			return false, trace.Wrap(err)
		}

		if info.To != nil &&
			info.To.Kind == headers.Kind &&
			info.To.GetNamespace() == headers.GetNamespace() &&
			info.To.GetName() == headers.GetName() {
			if op.Status == OpStatusCompleted {
				return true, nil
			}
			return false, trace.CompareFailed("operation %s in changeset %s is not completed", op.UID, tr.Name)
		}
	}

	// Operation is not created
	return false, nil
}

// Status checks all statuses for all resources updated or added in the context of a given changeset
func (cs *Changeset) Status(ctx context.Context, id string, retryAttempts int, retryPeriod time.Duration) error {
	tr, err := cs.get()
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
					err := cs.status(ctx, []byte(op.To), "")
					if err != nil {
						if op.Status != OpStatusReverted || !trace.IsNotFound(err) {
							return trace.Wrap(err)
						}
					}
				} else {
					info, err := GetOperationInfo(op)
					if err != nil {
						return trace.Wrap(err)
					}
					err = cs.status(ctx, []byte(op.From), op.UID)
					if err == nil || !trace.IsNotFound(err) {
						return trace.CompareFailed("%v with UID %q still active: %v",
							formatMeta(info.From.ObjectMeta, info.From.TypeMeta), op.UID, err)
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
func (cs *Changeset) DeleteResource(ctx context.Context, resourceNamespace string, resource Ref, cascade bool) error {
	tr, err := cs.createOrRead(ChangesetSpec{Status: ChangesetStatusInProgress})
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != ChangesetStatusInProgress {
		return trace.CompareFailed("cannot update changeset - expected status %q, got %q", ChangesetStatusInProgress, tr.Spec.Status)
	}
	log.WithFields(log.Fields{
		"cs": tr.String(),
	}).Infof("Deleting %v", formatMeta(tr.ObjectMeta, tr.TypeMeta))

	dr := deleteResource{
		cascade:  cascade,
		resource: tr,
	}

	switch resource.Kind {
	case KindDaemonSet:
		return cs.deleteDaemonSet(ctx, dr)
	case KindStatefulSet:
		return cs.deleteStatefulSet(ctx, dr)
	case KindJob:
		return cs.deleteJob(ctx, dr)
	case KindReplicationController:
		return cs.deleteRC(ctx, dr)
	case KindDeployment:
		return cs.deleteDeployment(ctx, dr)
	case KindSecret:
		return cs.deleteSecret(ctx, dr)
	case KindConfigMap:
		return cs.deleteConfigMap(ctx, dr)
	case KindService:
		return cs.deleteService(ctx, dr)
	case KindServiceAccount:
		return cs.deleteServiceAccount(ctx, dr)
	case KindRole:
		return cs.deleteRole(ctx, dr)
	case KindClusterRole:
		return cs.deleteClusterRole(ctx, dr)
	case KindRoleBinding:
		return cs.deleteRoleBinding(ctx, dr)
	case KindClusterRoleBinding:
		return cs.deleteClusterRoleBinding(ctx, dr)
	case KindPodSecurityPolicy:
		return cs.deletePodSecurityPolicy(ctx, dr)
	}
	return trace.BadParameter("delete: unimplemented resource %v", resource.Kind)
}

// Freeze "freezes" changeset, prohibits adding or removing any changes to it
func (cs *Changeset) Freeze(ctx context.Context) error {
	tr, err := cs.get()
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
func (cs *Changeset) Revert(ctx context.Context, uid string) error {
	tr, err := cs.get()
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status == ChangesetStatusReverted {
		return trace.CompareFailed("changeset is already reverted")
	}
	log := log.WithFields(log.Fields{
		"cs": tr.String(),
	})

	if uid != "" {
		return cs.revertItem(ctx, tr, uid)
	}

	for i := len(tr.Spec.Items) - 1; i >= 0; i-- {
		op := &tr.Spec.Items[i]
		info, err := GetOperationInfo(*op)
		if err != nil {
			return trace.Wrap(err)
		}
		if op.Status != OpStatusCompleted {
			log.Infof("skipping changeset item %v, status: %v is not the expected %v", info, op.Status, OpStatusCompleted)
		}
		if err := cs.revert(ctx, op, info); err != nil {
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

func (cs *Changeset) revertItem(ctx context.Context, resource *ChangesetResource, uid string) error {
	for i := len(resource.Spec.Items) - 1; i >= 0; i-- {
		op := &resource.Spec.Items[i]
		if op.UID != uid {
			continue
		}

		info, err := GetOperationInfo(*op)
		if err != nil {
			return trace.Wrap(err)
		}
		if err := cs.revert(ctx, op, info); err != nil {
			return trace.Wrap(err)
		}
		// delete item from changeset
		resource.Spec.Items = append(resource.Spec.Items[:i], resource.Spec.Items[i+1:]...)
		resource, err = cs.update(resource)
		if err != nil {
			return trace.Wrap(err)
		}
		return nil
	}
	return trace.NotFound("item with %s not found", uid)
}

func (cs *Changeset) status(ctx context.Context, data []byte, uid string) error {
	header, err := ParseResourceHeader(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	switch header.Kind {
	case KindDaemonSet:
		return cs.statusDaemonSet(ctx, data, uid)
	case KindStatefulSet:
		return cs.statusStatefulSet(ctx, data, uid)
	case KindJob:
		return cs.statusJob(ctx, data, uid)
	case KindReplicationController:
		return cs.statusRC(ctx, data, uid)
	case KindDeployment:
		return cs.statusDeployment(ctx, data, uid)
	case KindService:
		return cs.statusService(ctx, data, uid)
	case KindServiceAccount:
		return cs.statusServiceAccount(ctx, data, uid)
	case KindSecret:
		return cs.statusSecret(ctx, data, uid)
	case KindConfigMap:
		return cs.statusConfigMap(ctx, data, uid)
	case KindRole:
		return cs.statusRole(ctx, data, uid)
	case KindClusterRole:
		return cs.statusClusterRole(ctx, data, uid)
	case KindRoleBinding:
		return cs.statusRoleBinding(ctx, data, uid)
	case KindClusterRoleBinding:
		return cs.statusClusterRoleBinding(ctx, data, uid)
	case KindPodSecurityPolicy:
		return cs.statusPodSecurityPolicy(ctx, data, uid)
	}
	return trace.BadParameter("unsupported resource type %v for resource %v", header.Kind, header.Name)
}

func (cs *Changeset) statusDaemonSet(ctx context.Context, data []byte, uid string) error {
	daemonset, err := ParseDaemonSet(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.AppsV1().DaemonSets(daemonset.Namespace).Get(daemonset.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("daemonset with UID %v not found", uid)
		}
	}
	control, err := NewDaemonSetControl(DSConfig{DaemonSet: daemonset, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusStatefulSet(ctx context.Context, data []byte, uid string) error {
	statefulSet, err := ParseStatefulSet(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.AppsV1().StatefulSets(statefulSet.Namespace).Get(statefulSet.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("statefulset with UID %v not found", uid)
		}
	}
	control, err := NewStatefulSetControl(StatefulSetConfig{StatefulSet: statefulSet, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusJob(ctx context.Context, data []byte, uid string) error {
	job, err := ParseJob(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.Batch().Jobs(job.Namespace).Get(job.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("job with UID %v not found", uid)
		}
	}
	control, err := NewJobControl(JobConfig{Job: job, Clientset: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusRC(ctx context.Context, data []byte, uid string) error {
	rc, err := ParseReplicationController(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.Core().ReplicationControllers(rc.Namespace).Get(rc.Name, metav1.GetOptions{})

		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("replication controller with UID %v not found", uid)
		}
	}
	control, err := NewReplicationControllerControl(RCConfig{ReplicationController: rc, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusDeployment(ctx context.Context, data []byte, uid string) error {
	deployment, err := ParseDeployment(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.AppsV1().Deployments(deployment.Namespace).Get(deployment.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("deployment with UID %v not found", uid)
		}
	}
	control, err := NewDeploymentControl(DeploymentConfig{Deployment: deployment, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusService(ctx context.Context, data []byte, uid string) error {
	service, err := ParseService(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.Core().Services(service.Namespace).Get(service.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("service with UID %v not found", uid)
		}
	}
	control, err := NewServiceControl(ServiceConfig{Service: service, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusSecret(ctx context.Context, data []byte, uid string) error {
	secret, err := ParseSecret(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.Core().Secrets(secret.Namespace).Get(secret.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("secret with UID %v not found", uid)
		}
	}
	control, err := NewSecretControl(SecretConfig{Secret: secret, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusConfigMap(ctx context.Context, data []byte, uid string) error {
	configMap, err := ParseConfigMap(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.Core().ConfigMaps(configMap.Namespace).Get(configMap.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("configmap with UID %v not found", uid)
		}
	}
	control, err := NewConfigMapControl(ConfigMapConfig{ConfigMap: configMap, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusServiceAccount(ctx context.Context, data []byte, uid string) error {
	account, err := ParseServiceAccount(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.Core().ServiceAccounts(account.Namespace).Get(account.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("service account with UID %v not found", uid)
		}
	}
	control, err := NewServiceAccountControl(ServiceAccountConfig{ServiceAccount: account, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusRole(ctx context.Context, data []byte, uid string) error {
	role, err := ParseRole(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.RbacV1().Roles(role.Namespace).Get(role.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("role with UID %v not found", uid)
		}
	}
	control, err := NewRoleControl(RoleConfig{Role: role, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusClusterRole(ctx context.Context, data []byte, uid string) error {
	role, err := ParseClusterRole(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.RbacV1().ClusterRoles().Get(role.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("cluster role with UID %v not found", uid)
		}
	}
	control, err := NewClusterRoleControl(ClusterRoleConfig{ClusterRole: role, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusRoleBinding(ctx context.Context, data []byte, uid string) error {
	binding, err := ParseRoleBinding(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.RbacV1().RoleBindings(binding.Namespace).Get(binding.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("role binding with UID %v not found", uid)
		}
	}
	control, err := NewRoleBindingControl(RoleBindingConfig{RoleBinding: binding, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusClusterRoleBinding(ctx context.Context, data []byte, uid string) error {
	binding, err := ParseClusterRoleBinding(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.RbacV1().ClusterRoleBindings().Get(binding.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("cluster role binding with UID %v not found", uid)
		}
	}
	control, err := NewClusterRoleBindingControl(ClusterRoleBindingConfig{ClusterRoleBinding: binding, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) statusPodSecurityPolicy(ctx context.Context, data []byte, uid string) error {
	policy, err := ParsePodSecurityPolicy(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	if uid != "" {
		existing, err := cs.Client.ExtensionsV1beta1().PodSecurityPolicies().Get(policy.Name, metav1.GetOptions{})
		if err != nil {
			return ConvertError(err)
		}
		if string(existing.GetUID()) != uid {
			return trace.NotFound("pod security policy with UID %v not found", uid)
		}
	}
	control, err := NewPodSecurityPolicyControl(PodSecurityPolicyConfig{PodSecurityPolicy: policy, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status()
}

func (cs *Changeset) withDeleteOp(ctx context.Context, tr *ChangesetResource, obj metav1.Object, fn func() error) error {
	data, err := goyaml.Marshal(obj)
	if err != nil {
		return trace.Wrap(err)
	}
	tr.Spec.Items = append(tr.Spec.Items, ChangesetItem{
		From:              string(data),
		UID:               string(obj.GetUID()),
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
	return err
}

func (cs *Changeset) deleteDaemonSet(ctx context.Context, dr deleteResource) error {
	daemonSet, err := cs.Client.AppsV1().DaemonSets(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewDaemonSetControl(DSConfig{DaemonSet: daemonSet, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.DaemonSet, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteStatefulSet(ctx context.Context, dr deleteResource) error {
	statefulSet, err := cs.Client.AppsV1().StatefulSets(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewStatefulSetControl(StatefulSetConfig{StatefulSet: statefulSet, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.StatefulSet, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteJob(ctx context.Context, dr deleteResource) error {
	job, err := cs.Client.Batch().Jobs(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewJobControl(JobConfig{Job: job, Clientset: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.Job, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteRC(ctx context.Context, dr deleteResource) error {
	rc, err := cs.Client.Core().ReplicationControllers(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewReplicationControllerControl(RCConfig{ReplicationController: rc, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.ReplicationController, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteDeployment(ctx context.Context, dr deleteResource) error {
	deployment, err := cs.Client.AppsV1().Deployments(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewDeploymentControl(DeploymentConfig{Deployment: deployment, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.Deployment, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteService(ctx context.Context, dr deleteResource) error {
	service, err := cs.Client.Core().Services(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewServiceControl(ServiceConfig{Service: service, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.Service, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteConfigMap(ctx context.Context, dr deleteResource) error {
	configMap, err := cs.Client.Core().ConfigMaps(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewConfigMapControl(ConfigMapConfig{ConfigMap: configMap, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.ConfigMap, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteSecret(ctx context.Context, dr deleteResource) error {
	secret, err := cs.Client.Core().Secrets(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewSecretControl(SecretConfig{Secret: secret, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.Secret, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteServiceAccount(ctx context.Context, dr deleteResource) error {
	account, err := cs.Client.Core().ServiceAccounts(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewServiceAccountControl(ServiceAccountConfig{ServiceAccount: account, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.ServiceAccount, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteRole(ctx context.Context, dr deleteResource) error {
	role, err := cs.Client.RbacV1().Roles(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewRoleControl(RoleConfig{Role: role, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.Role, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteClusterRole(ctx context.Context, dr deleteResource) error {
	role, err := cs.Client.RbacV1().ClusterRoles().Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewClusterRoleControl(ClusterRoleConfig{ClusterRole: role, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.ClusterRole, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteRoleBinding(ctx context.Context, dr deleteResource) error {
	binding, err := cs.Client.RbacV1().RoleBindings(Namespace(dr.resource.ObjectMeta.Namespace)).Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewRoleBindingControl(RoleBindingConfig{RoleBinding: binding, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.RoleBinding, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deleteClusterRoleBinding(ctx context.Context, dr deleteResource) error {
	binding, err := cs.Client.RbacV1().ClusterRoleBindings().Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewClusterRoleBindingControl(ClusterRoleBindingConfig{ClusterRoleBinding: binding, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.ClusterRoleBinding, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) deletePodSecurityPolicy(ctx context.Context, dr deleteResource) error {
	policy, err := cs.Client.ExtensionsV1beta1().PodSecurityPolicies().Get(dr.resource.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	control, err := NewPodSecurityPolicyControl(PodSecurityPolicyConfig{PodSecurityPolicy: policy, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return cs.withDeleteOp(ctx, dr.resource, control.PodSecurityPolicy, func() error {
		return control.Delete(ctx, dr.cascade)
	})
}

func (cs *Changeset) revert(ctx context.Context, item *ChangesetItem, info *OperationInfo) error {
	kind := info.Kind()
	switch info.Kind() {
	case KindDaemonSet:
		return cs.revertDaemonSet(ctx, item)
	case KindStatefulSet:
		return cs.revertStatefulSet(ctx, item)
	case KindJob:
		return cs.revertJob(ctx, item)
	case KindReplicationController:
		return cs.revertReplicationController(ctx, item)
	case KindDeployment:
		return cs.revertDeployment(ctx, item)
	case KindService:
		return cs.revertService(ctx, item)
	case KindServiceAccount:
		return cs.revertServiceAccount(ctx, item)
	case KindSecret:
		return cs.revertSecret(ctx, item)
	case KindConfigMap:
		return cs.revertConfigMap(ctx, item)
	case KindRole:
		return cs.revertRole(ctx, item)
	case KindClusterRole:
		return cs.revertClusterRole(ctx, item)
	case KindRoleBinding:
		return cs.revertRoleBinding(ctx, item)
	case KindClusterRoleBinding:
		return cs.revertClusterRoleBinding(ctx, item)
	case KindPodSecurityPolicy:
		return cs.revertPodSecurityPolicy(ctx, item)
	}
	return trace.BadParameter("unsupported resource type %v", kind)
}

func (cs *Changeset) revertDaemonSet(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	daemonSet, err := ParseDaemonSet(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewDaemonSetControl(DSConfig{DaemonSet: daemonSet, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created daemon set, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated daemon set, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertStatefulSet(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	statefulSet, err := ParseStatefulSet(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewStatefulSetControl(StatefulSetConfig{StatefulSet: statefulSet, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created statefulset, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated statefulset, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertJob(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	job, err := ParseJob(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewJobControl(JobConfig{Job: job, Clientset: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	if len(item.From) == 0 {
		// this operation created the job, so we will delete it
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated the job, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertReplicationController(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	rc, err := ParseReplicationController(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewReplicationControllerControl(RCConfig{ReplicationController: rc, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created RC, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated RC, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertDeployment(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	deployment, err := ParseDeployment(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewDeploymentControl(DeploymentConfig{Deployment: deployment, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created Deployment, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated Deployment, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertService(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	service, err := ParseService(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewServiceControl(ServiceConfig{Service: service, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created Service, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated Service, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertConfigMap(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	configMap, err := ParseConfigMap(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewConfigMapControl(ConfigMapConfig{ConfigMap: configMap, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created ConfigMap, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated ConfigMap, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertSecret(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	secret, err := ParseSecret(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewSecretControl(SecretConfig{Secret: secret, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created Secret, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated Secret, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertServiceAccount(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	account, err := ParseServiceAccount(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewServiceAccountControl(ServiceAccountConfig{ServiceAccount: account, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created the resource, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated the resource, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertRole(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	role, err := ParseRole(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewRoleControl(RoleConfig{Role: role, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created the resource, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated the resource, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertClusterRole(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	role, err := ParseClusterRole(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewClusterRoleControl(ClusterRoleConfig{ClusterRole: role, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created the resource, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated the resource, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertRoleBinding(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	binding, err := ParseRoleBinding(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewRoleBindingControl(RoleBindingConfig{RoleBinding: binding, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created the resource, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated the resource, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertClusterRoleBinding(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	binding, err := ParseClusterRoleBinding(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewClusterRoleBindingControl(ClusterRoleBindingConfig{ClusterRoleBinding: binding, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created the resource, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated the resource, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) revertPodSecurityPolicy(ctx context.Context, item *ChangesetItem) error {
	resource := item.From
	if len(resource) == 0 {
		resource = item.To
	}
	policy, err := ParsePodSecurityPolicy(strings.NewReader(resource))
	if err != nil {
		return trace.Wrap(err)
	}
	control, err := NewPodSecurityPolicyControl(PodSecurityPolicyConfig{PodSecurityPolicy: policy, Client: cs.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	// this operation created the resource, so we will delete it
	if len(item.From) == 0 {
		err = control.Delete(ctx, true)
		// If the resource has already been deleted, suppress the error
		if trace.IsNotFound(err) {
			return nil
		}
		return trace.Wrap(err)
	}
	// this operation either created or updated the resource, so we create a new version
	return control.Upsert(ctx)
}

func (cs *Changeset) withUpsertOp(ctx context.Context, ur upsertResource, old, new metav1.Object, fn func() error) (*ChangesetResource, error) {
	to, err := goyaml.Marshal(new)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	item := ChangesetItem{
		CreationTimestamp: time.Now().UTC(),
		To:                string(to),
		Status:            OpStatusCreated,
	}
	newUUID, err := uuid.NewRandom()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	item.UID = newUUID.String()
	if !reflect.ValueOf(old).IsNil() {
		from, err := goyaml.Marshal(old)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		item.From = string(from)
		item.UID = string(old.GetUID())
	}
	ur.resource.Spec.Items = append(ur.resource.Spec.Items, item)
	ur.resource, err = cs.update(ur.resource)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if err := fn(); err != nil {
		return nil, trace.Wrap(err)
	}
	ur.resource.Spec.Items[len(ur.resource.Spec.Items)-1].Status = OpStatusCompleted
	return cs.update(ur.resource)
}

func (cs *Changeset) upsertJob(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	job, err := ParseJob(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(job.ObjectMeta, job.TypeMeta))

	jobs := cs.Client.Batch().Jobs(job.Namespace)
	current, err := jobs.Get(job.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Info("existing job not found")
		current = nil
	}
	control, err := NewJobControl(JobConfig{Job: job, Clientset: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaJob(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.Job, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertDaemonSet(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	daemonSet, err := ParseDaemonSet(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(daemonSet.ObjectMeta, daemonSet.TypeMeta))
	daemons := cs.Client.AppsV1().DaemonSets(daemonSet.Namespace)
	current, err := daemons.Get(daemonSet.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing daemonset not found")
		current = nil
	}
	control, err := NewDaemonSetControl(DSConfig{DaemonSet: daemonSet, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaDaemonset(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.DaemonSet, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertStatefulSet(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	statefulSet, err := ParseStatefulSet(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(statefulSet.ObjectMeta, statefulSet.TypeMeta))
	statefulsets := cs.Client.AppsV1().StatefulSets(statefulSet.Namespace)
	current, err := statefulsets.Get(statefulSet.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing statefulset not found")
		current = nil
	}
	control, err := NewStatefulSetControl(StatefulSetConfig{StatefulSet: statefulSet, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaStatefulSet(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.StatefulSet, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertRC(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	rc, err := ParseReplicationController(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(rc.ObjectMeta, rc.TypeMeta))
	rcs := cs.Client.Core().ReplicationControllers(rc.Namespace)
	current, err := rcs.Get(rc.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing replication controller not found")
		current = nil
	}
	control, err := NewReplicationControllerControl(RCConfig{ReplicationController: rc, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaReplicationController(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.ReplicationController, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertDeployment(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	deployment, err := ParseDeployment(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(deployment.ObjectMeta, deployment.TypeMeta))
	deployments := cs.Client.AppsV1().Deployments(deployment.Namespace)
	current, err := deployments.Get(deployment.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing deployment not found")
		current = nil
	}
	control, err := NewDeploymentControl(DeploymentConfig{Deployment: deployment, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaDeployment(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.Deployment, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertService(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	service, err := ParseService(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(service.ObjectMeta, service.TypeMeta))
	services := cs.Client.Core().Services(service.Namespace)
	current, err := services.Get(service.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing service not found")
		current = nil
	}
	control, err := NewServiceControl(ServiceConfig{Service: service, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaService(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.Service, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertServiceAccount(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	account, err := ParseServiceAccount(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(account.ObjectMeta, account.TypeMeta))
	accounts := cs.Client.Core().ServiceAccounts(account.Namespace)
	current, err := accounts.Get(account.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing service account not found")
		current = nil
	}
	control, err := NewServiceAccountControl(ServiceAccountConfig{ServiceAccount: account, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaServiceAccount(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.ServiceAccount, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertRole(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	role, err := ParseRole(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(role.ObjectMeta, role.TypeMeta))
	roles := cs.Client.RbacV1().Roles(role.Namespace)
	current, err := roles.Get(role.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing role not found")
		current = nil
	}
	control, err := NewRoleControl(RoleConfig{Role: role, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaRole(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.Role, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertClusterRole(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	role, err := ParseClusterRole(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(role.ObjectMeta, role.TypeMeta))
	roles := cs.Client.RbacV1().ClusterRoles()
	current, err := roles.Get(role.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing cluster role not found")
		current = nil
	}
	control, err := NewClusterRoleControl(ClusterRoleConfig{ClusterRole: role, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaClusterRole(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.ClusterRole, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertRoleBinding(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	binding, err := ParseRoleBinding(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(binding.ObjectMeta, binding.TypeMeta))
	bindings := cs.Client.RbacV1().RoleBindings(binding.Namespace)
	current, err := bindings.Get(binding.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing role binding not found")
		current = nil
	}
	control, err := NewRoleBindingControl(RoleBindingConfig{RoleBinding: binding, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaRoleBinding(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.RoleBinding, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertClusterRoleBinding(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	binding, err := ParseClusterRoleBinding(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(binding.ObjectMeta, binding.TypeMeta))
	bindings := cs.Client.RbacV1().ClusterRoleBindings()
	current, err := bindings.Get(binding.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing cluster role binding not found")
		current = nil
	}
	control, err := NewClusterRoleBindingControl(ClusterRoleBindingConfig{ClusterRoleBinding: binding, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaClusterRoleBinding(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.ClusterRoleBinding, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertPodSecurityPolicy(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	policy, err := ParsePodSecurityPolicy(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(policy.ObjectMeta, policy.TypeMeta))
	policies := cs.Client.ExtensionsV1beta1().PodSecurityPolicies()
	current, err := policies.Get(policy.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing pod security policy not found")
		current = nil
	}
	control, err := NewPodSecurityPolicyControl(PodSecurityPolicyConfig{PodSecurityPolicy: policy, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaPodSecurityPolicy(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.PodSecurityPolicy, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertConfigMap(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	configMap, err := ParseConfigMap(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(configMap.ObjectMeta, configMap.TypeMeta))
	configMaps := cs.Client.Core().ConfigMaps(configMap.Namespace)
	current, err := configMaps.Get(configMap.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing configmap not found")
		current = nil
	}
	control, err := NewConfigMapControl(ConfigMapConfig{ConfigMap: configMap, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaConfigMap(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.ConfigMap, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) upsertSecret(ctx context.Context, ur upsertResource) (*ChangesetResource, error) {
	secret, err := ParseSecret(bytes.NewReader(ur.data))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	log := log.WithFields(log.Fields{
		"cs": ur.resource.String(),
	})
	log.Infof("Upsert %v", formatMeta(secret.ObjectMeta, secret.TypeMeta))
	secrets := cs.Client.Core().Secrets(secret.Namespace)
	current, err := secrets.Get(secret.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		log.Debug("existing secret not found")
		current = nil
	}
	control, err := NewSecretControl(SecretConfig{Secret: secret, Client: cs.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if current != nil {
		updateTypeMetaSecret(current)
	}
	return cs.withUpsertOp(ctx, ur, current, control.Secret, func() error {
		return control.Upsert(ctx)
	})
}

func (cs *Changeset) Init(ctx context.Context) error {
	log.Debug("changeset init")

	// kubernetes 1.8 or newer
	crd := &apiextensions.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: ChangesetResourceName,
		},
		Spec: apiextensions.CustomResourceDefinitionSpec{
			Group:   ChangesetGroup,
			Version: ChangesetVersion,
			Scope:   ChangesetScope,
			Names: apiextensions.CustomResourceDefinitionNames{
				Kind:     KindChangeset,
				Plural:   ChangesetPlural,
				Singular: ChangesetSingular,
			},
		},
	}

	_, err := cs.APIExtensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	err = ConvertError(err)
	if err != nil {
		if !trace.IsAlreadyExists(err) {
			return trace.Wrap(err)
		}
	}
	// wait for the controller to init by trying to list stuff
	return retry(ctx, 30, time.Second, func() error {
		_, err := cs.list()
		return err
	})

}

func (cs *Changeset) Get(ctx context.Context, namespace, name string) (*ChangesetResource, error) {
	return cs.get()
}

func (cs *Changeset) List(ctx context.Context) (*ChangesetList, error) {
	return cs.list()
}

// Create creates a new one given the name and namespace.
// The new changeset is created with status in-progress.
// If there's already a changeset with this name in this namespace, AlreadyExists
// error is returned.
func (cs *Changeset) Create(ctx context.Context, namespace, name string) (*ChangesetResource, error) {
	res := &ChangesetResource{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindChangeset,
			APIVersion: ChangesetAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ChangesetSpec{
			Status: ChangesetStatusInProgress,
		},
	}
	return cs.create(res)
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
		return nil, ConvertError(err)
	}
	var result ChangesetResource
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func (cs *Changeset) get() (*ChangesetResource, error) {
	var raw runtime.Unknown
	err := cs.client.Get().
		SubResource("namespaces", cs.Namespace, ChangesetCollection, cs.Name).
		Do().
		Into(&raw)
	if err != nil {
		return nil, ConvertError(err)
	}
	var result ChangesetResource
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func (cs *Changeset) createOrRead(spec ChangesetSpec) (*ChangesetResource, error) {
	res := &ChangesetResource{
		TypeMeta: metav1.TypeMeta{
			Kind:       KindChangeset,
			APIVersion: ChangesetAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cs.Name,
			Namespace: cs.Namespace,
		},
		Spec: spec,
	}
	out, err := cs.create(res)
	if err == nil {
		return out, nil
	}
	if !trace.IsAlreadyExists(err) {
		return nil, trace.Wrap(err)
	}
	return cs.get()
}

func (cs *Changeset) Delete(ctx context.Context) error {
	var raw runtime.Unknown
	err := cs.client.Delete().
		SubResource("namespaces", cs.Namespace, ChangesetCollection, cs.Name).
		Do().
		Into(&raw)
	if err != nil {
		return ConvertError(err)
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
		return nil, ConvertError(err)
	}
	var result ChangesetResource
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func (cs *Changeset) list() (*ChangesetList, error) {
	var raw runtime.Unknown
	err := cs.client.Get().
		SubResource("namespaces", cs.Namespace, ChangesetCollection).
		Do().
		Into(&raw)
	if err != nil {
		return nil, ConvertError(err)
	}
	var result ChangesetList
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

type upsertResource struct {
	resource *ChangesetResource
	data     []byte
}

type deleteResource struct {
	resource *ChangesetResource
	cascade  bool
}
