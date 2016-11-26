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
	"net/http"
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

type TransactionConfig struct {
	// Client is k8s client
	Client *kubernetes.Clientset
	// Config is rest client config
	Config *rest.Config
}

func (c *TransactionConfig) CheckAndSetDefaults() error {
	if c.Client == nil {
		return trace.BadParameter("missing parameter Client")
	}
	if c.Config == nil {
		return trace.BadParameter("missing parameter Config")
	}
	return nil
}

func NewTransaction(config TransactionConfig) (*Transaction, error) {
	if err := config.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	cfg := *config.Config
	cfg.APIPath = "/apis"
	if cfg.UserAgent == "" {
		cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	cfg.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}
	cfg.GroupVersion = &unversioned.GroupVersion{Group: txGroup, Version: txVersion}

	clt, err := rest.RESTClientFor(&cfg)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &Transaction{TransactionConfig: config, client: clt}, nil
}

type ResourceHeader struct {
	unversioned.TypeMeta `json:",inline"`
	v1.ObjectMeta        `json:"metadata,omitempty"`
}

// Transaction is a is a collection transaction log that can rollback a series of
// changes to the system
type Transaction struct {
	TransactionConfig
	client *rest.RESTClient
}

type TransactionList struct {
	unversioned.TypeMeta `json:",inline"`
	// Standard list metadata
	// More info: http://releases.k8s.io/HEAD/docs/devel/api-conventions.md#metadata
	unversioned.ListMeta `json:"metadata,omitempty"`
	// Items is a list of third party objects
	Items []TransactionResource `json:"items"`
}

func (tr *TransactionList) GetObjectKind() unversioned.ObjectKind {
	return &tr.TypeMeta
}

type TransactionResource struct {
	unversioned.TypeMeta `json:",inline"`
	v1.ObjectMeta        `json:"metadata,omitempty"`
	Spec                 TransactionSpec `json:"spec"`
}

func (tr *TransactionResource) GetObjectKind() unversioned.ObjectKind {
	return &tr.TypeMeta
}

func (tr *TransactionResource) String() string {
	return fmt.Sprintf("namespace=%v, name=%v, operations=%v)", tr.Namespace, tr.Name, len(tr.Spec.Items))
}

type TransactionSpec struct {
	Status string            `json:"status"`
	Items  []TransactionItem `json:"items"`
}

type TransactionItem struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Status string `json:"status"`
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

// ParseResourceHeader parses resource header information
func ParseResourceHeader(reader io.Reader) (*ResourceHeader, error) {
	var out ResourceHeader
	err := yaml.NewYAMLOrJSONDecoder(reader, 1024).Decode(&out)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &out, nil
}

// GetOperationInfo returns operation information
func GetOperationInfo(item TransactionItem) (*OperationInfo, error) {
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

func (tx *Transaction) status(ctx context.Context, data []byte) error {
	header, err := ParseResourceHeader(bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	switch header.Kind {
	case KindDaemonSet:
		return tx.statusDaemonSet(ctx, data)
	}
	return trace.BadParameter("unsupported resource type %v for resource %v", header.Kind, header.Name)
}

func (tx *Transaction) statusDaemonSet(ctx context.Context, data []byte) error {
	control, err := NewDSControl(DSConfig{Reader: bytes.NewReader(data), Client: tx.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Status(ctx, 1, 0)
}

func (tx *Transaction) DeleteResource(ctx context.Context, transactionNamespace, transactionName string, resourceNamespace string, resource Ref, cascade bool) error {
	if err := tx.Init(ctx); err != nil {
		return trace.Wrap(err)
	}
	tr, err := tx.createOrRead(&TransactionResource{
		TypeMeta: unversioned.TypeMeta{
			Kind:       KindTransaction,
			APIVersion: txAPIVersion,
		},
		ObjectMeta: v1.ObjectMeta{
			Name: transactionName,
		},
		Spec: TransactionSpec{
			Status: txStatusInProgress,
		},
	})
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != txStatusInProgress {
		return trace.CompareFailed("can't update transaction that is not in state '%v'", txStatusInProgress)
	}
	g := log.WithFields(log.Fields{
		"tx": tr.String(),
	})
	g.Infof("deleting %v from %v", resource, resourceNamespace)
	switch resource.Kind {
	case KindDaemonSet:
		return tx.deleteDaemonSet(ctx, tr, resourceNamespace, resource.Name, cascade)
	}
	return trace.BadParameter("don't know how to delete %v yet", resource.Kind)
}

func (tx *Transaction) deleteDaemonSet(ctx context.Context, tr *TransactionResource, namespace, name string, cascade bool) error {
	ds, err := tx.Client.Extensions().DaemonSets(Namespace(namespace)).Get(name)
	if err != nil {
		return trace.Wrap(err)
	}
	ds.Kind = KindDaemonSet
	control, err := NewDSControl(DSConfig{DaemonSet: ds, Client: tx.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	data, err := goyaml.Marshal(ds)
	if err != nil {
		return trace.Wrap(err)
	}
	tr.Spec.Items = append(tr.Spec.Items, TransactionItem{
		From:   string(data),
		Status: opStatusCreated,
	})
	tr, err = tx.update(tr)
	if err != nil {
		return trace.Wrap(err)
	}
	err = control.Delete(ctx, cascade)
	if err != nil {
		return trace.Wrap(err)
	}
	tr.Spec.Items[len(tr.Spec.Items)-1].Status = opStatusCompleted
	_, err = tx.update(tr)
	return err
}

func (tx *Transaction) Commit(ctx context.Context, transactionNamespace, transactionName string) error {
	tr, err := tx.get(transactionNamespace, transactionName)
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != txStatusInProgress {
		return trace.CompareFailed("transaction is not in progress")
	}
	for i := len(tr.Spec.Items) - 1; i >= 0; i-- {
		item := &tr.Spec.Items[i]
		if item.Status != opStatusCompleted {
			return trace.CompareFailed("operation %v is not completed", i)
		}
	}
	tr.Spec.Status = txStatusCommited
	_, err = tx.update(tr)
	return trace.Wrap(err)
}

func (tx *Transaction) Rollback(ctx context.Context, transactionNamespace, transactionName string) error {
	tr, err := tx.get(transactionNamespace, transactionName)
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status == txStatusRolledBack {
		return trace.CompareFailed("transaction is already rolled back")
	}
	g := log.WithFields(log.Fields{
		"tx": tr.String(),
	})
	g.Infof("rollback")
	for i := len(tr.Spec.Items) - 1; i >= 0; i-- {
		op := &tr.Spec.Items[i]
		if op.Status != opStatusCompleted {
			g.Infof("skipping transaction item %v, status: %v is not %v", i, op.Status, opStatusCompleted)
		}
		if err := tx.rollback(ctx, op); err != nil {
			return trace.Wrap(err)
		}
		op.Status = opStatusRolledBack
		tr, err = tx.update(tr)
		if err != nil {
			return trace.Wrap(err)
		}
	}
	tr.Spec.Status = txStatusRolledBack
	_, err = tx.update(tr)
	return trace.Wrap(err)
}

func (tx *Transaction) rollback(ctx context.Context, item *TransactionItem) error {
	info, err := GetOperationInfo(*item)
	if err != nil {
		if err != nil {
			return trace.Wrap(err)
		}
	}
	kind := info.Kind()
	switch info.Kind() {
	case KindDaemonSet:
		return tx.rollbackDaemonSet(ctx, item)
	}
	return trace.BadParameter("unsupported resource type %v", kind)
}

func (tx *Transaction) rollbackDaemonSet(ctx context.Context, item *TransactionItem) error {
	// this operation created daemon set, so we will delete it
	if len(item.From) == 0 {
		control, err := NewDSControl(DSConfig{Reader: strings.NewReader(item.To), Client: tx.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		return control.Delete(ctx, true)
	}
	// this operation either created or updated daemon set, so we create a new version
	control, err := NewDSControl(DSConfig{Reader: strings.NewReader(item.From), Client: tx.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return control.Upsert(ctx)
}

func (tx *Transaction) Status(ctx context.Context, transactionNamespace, transactionName string, retryAttempts int, retryPeriod time.Duration) error {
	tr, err := tx.get(transactionNamespace, transactionName)
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status == txStatusRolledBack {
		return trace.CompareFailed("transaction has been rolled back")
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
			case opStatusRolledBack:
				log.Infof("%v is rolled back, skip status check", i)
				continue
			case opStatusCreated:
				return trace.BadParameter("%v is not completed yet", tr)
			case opStatusCompleted:
				if op.To != "" {
					if err := tx.status(ctx, []byte(op.To)); err != nil {
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

func (tx *Transaction) Upsert(ctx context.Context, transactionNamespace, transactionName string, data []byte) error {
	tr, err := tx.createOrRead(&TransactionResource{
		TypeMeta: unversioned.TypeMeta{
			Kind:       KindTransaction,
			APIVersion: txAPIVersion,
		},
		ObjectMeta: v1.ObjectMeta{
			Name: transactionName,
		},
		Spec: TransactionSpec{
			Status: txStatusInProgress,
		},
	})
	if err != nil {
		return trace.Wrap(err)
	}
	if tr.Spec.Status != txStatusInProgress {
		return trace.CompareFailed("can't update transaction that is not in state '%v'", txStatusInProgress)
	}
	var kind unversioned.TypeMeta
	err = yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 1024).Decode(&kind)
	if err != nil {
		return trace.Wrap(err)
	}
	switch kind.Kind {
	case KindDaemonSet:
		_, err = tx.upsertDaemonSet(ctx, tr, data)
		return err
	}
	return trace.BadParameter("unsupported resource type %v", kind.Kind)
}

func (tx *Transaction) upsertDaemonSet(ctx context.Context, tr *TransactionResource, data []byte) (*TransactionResource, error) {
	ds := v1beta1.DaemonSet{}
	err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 1024).Decode(&ds)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	g := log.WithFields(log.Fields{
		"tx": tr.String(),
		"ds": fmt.Sprintf("%v/%v", ds.Namespace, ds.Name),
	})
	g.Infof("upsert")
	// 1. find current daemon set if it exists
	daemons := tx.Client.Extensions().DaemonSets(ds.Namespace)
	currentDS, err := daemons.Get(ds.Name)
	if err != nil {
		if !trace.IsNotFound(err) {
			return nil, trace.Wrap(err)
		}
		g.Infof("existing daemonset is not found")
	}

	var from []byte
	if currentDS != nil {
		from, err = goyaml.Marshal(currentDS)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}

	tr.Spec.Items = append(tr.Spec.Items, TransactionItem{
		From:   string(from),
		To:     string(data),
		Status: opStatusCreated,
	})

	tr, err = tx.update(tr)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	control, err := NewDSControl(DSConfig{DaemonSet: &ds, Client: tx.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if err := control.Upsert(ctx); err != nil {
		return nil, trace.Wrap(err)
	}
	tr.Spec.Items[len(tr.Spec.Items)-1].Status = opStatusCompleted
	return tx.update(tr)
}

func (tx *Transaction) Init(ctx context.Context) error {
	log.Infof("Transaction init")
	tpr := &v1beta1.ThirdPartyResource{
		ObjectMeta: v1.ObjectMeta{
			Name: txResourceName,
		},
		Versions: []v1beta1.APIVersion{
			{Name: txVersion},
		},
		Description: "Update transactions",
	}
	_, err := tx.Client.Extensions().ThirdPartyResources().Create(tpr)
	err = convertErr(err)
	if err != nil {
		if !trace.IsAlreadyExists(err) {
			return trace.Wrap(err)
		}
	}
	// wait for the controller to init by trying to list stuff
	return retry(ctx, 30, time.Second, func() error {
		_, err := tx.list("default")
		return err
	})
}

func (tx *Transaction) Get(ctx context.Context, namespace, name string) (*TransactionResource, error) {
	if err := tx.Init(ctx); err != nil {
		return nil, trace.Wrap(err)
	}
	return tx.get(namespace, name)
}

func (tx *Transaction) List(ctx context.Context, namespace string) (*TransactionList, error) {
	if err := tx.Init(ctx); err != nil {
		return nil, trace.Wrap(err)
	}
	return tx.list(namespace)
}

func (tx *Transaction) upsert(tr *TransactionResource) (*TransactionResource, error) {
	out, err := tx.create(tr)
	if err == nil {
		return out, nil
	}
	if !trace.IsAlreadyExists(err) {
		return nil, err
	}
	return tx.update(tr)
}

func (tx *Transaction) create(tr *TransactionResource) (*TransactionResource, error) {
	tr.Namespace = Namespace(tr.Namespace)
	data, err := json.Marshal(tr)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var raw runtime.Unknown
	err = tx.client.Post().
		SubResource("namespaces", tr.Namespace, txCollection).
		Body(data).
		Do().
		Into(&raw)
	if err != nil {
		return nil, convertErr(err)
	}
	var result TransactionResource
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func (tx *Transaction) get(namespace, name string) (*TransactionResource, error) {
	var raw runtime.Unknown
	err := tx.client.Get().
		SubResource("namespaces", namespace, txCollection, name).
		Do().
		Into(&raw)
	if err != nil {
		return nil, convertErr(err)
	}
	var result TransactionResource
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func (tx *Transaction) createOrRead(tr *TransactionResource) (*TransactionResource, error) {
	out, err := tx.create(tr)
	if err == nil {
		return out, nil
	}
	if !trace.IsAlreadyExists(err) {
		return nil, trace.Wrap(err)
	}
	return tx.get(tr.Namespace, tr.Name)
}

func (tx *Transaction) Delete(ctx context.Context, namespace, name string) error {
	if err := tx.Init(ctx); err != nil {
		return trace.Wrap(err)
	}
	var raw runtime.Unknown
	err := tx.client.Delete().
		SubResource("namespaces", namespace, txCollection, name).
		Do().
		Into(&raw)
	if err != nil {
		return convertErr(err)
	}
	return nil
}

func (tx *Transaction) update(tr *TransactionResource) (*TransactionResource, error) {
	tr.Namespace = Namespace(tr.Namespace)
	data, err := json.Marshal(tr)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var raw runtime.Unknown
	err = tx.client.Put().
		SubResource("namespaces", tr.Namespace, txCollection, tr.Name).
		Body(data).
		Do().
		Into(&raw)
	if err != nil {
		return nil, convertErr(err)
	}
	var result TransactionResource
	if err := json.Unmarshal(raw.Raw, &result); err != nil {
		return nil, trace.Wrap(err)
	}
	return &result, nil
}

func (tx *Transaction) list(namespace string) (*TransactionList, error) {
	var raw runtime.Unknown
	err := tx.client.Get().
		SubResource("namespaces", namespace, txCollection).
		Do().
		Into(&raw)
	if err != nil {
		return nil, convertErr(err)
	}
	var result TransactionList
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
