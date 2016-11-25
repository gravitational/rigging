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

const (
	txResourceName = "transaction.tx.gravitational.io"
	txGroup        = "tx.gravitational.io"
	txVersion      = "v1"
	txAPIVersion   = "tx.gravitational.io/v1"
	txCollection   = "transactions"
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

// Transaction is a update transaction log that can rollback a series of
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
	return fmt.Sprintf("Transaction(namespace=%v, name=%v, operations=%v)", tr.Namespace, tr.Name, len(tr.Spec.Items))
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

const (
	kindDaemonSet   = "DaemonSet"
	kindTransaction = "Transaction"
)

func (tx *Transaction) status(ctx context.Context, data []byte) error {
	var kind unversioned.TypeMeta
	err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 1024).Decode(&kind)
	if err != nil {
		return trace.Wrap(err)
	}
	switch kind.Kind {
	case kindDaemonSet:
		return tx.statusDaemonSet(ctx, data)
	}
	return trace.BadParameter("unsupported resource type %v", kind.Kind)
}

func (tx *Transaction) statusDaemonSet(ctx context.Context, data []byte) error {
	ds := v1beta1.DaemonSet{}
	err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 1024).Decode(&ds)
	if err != nil {
		return trace.Wrap(err)
	}
	updater, err := NewDSUpdater(DSConfig{DaemonSet: &ds, Client: tx.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return updater.Status(ctx, 1, 0)
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
	g.Infof("rolling back")
	for i := len(tr.Spec.Items) - 1; i >= 0; i-- {
		item := &tr.Spec.Items[i]
		if item.Status != opStatusCompleted {
			g.Infof("skipping transaction item %v, status: %v is not %v", i, item.Status, opStatusCompleted)
		}
		if err := tx.rollback(ctx, []byte(item.To), []byte(item.From)); err != nil {
			return trace.Wrap(err)
		}
		item.Status = opStatusRolledBack
		tr, err = tx.update(tr)
		if err != nil {
			return trace.Wrap(err)
		}
	}
	tr.Spec.Status = txStatusRolledBack
	_, err = tx.update(tr)
	return trace.Wrap(err)
}

func (tx *Transaction) rollback(ctx context.Context, from, to []byte) error {
	var fromKind unversioned.TypeMeta
	err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(from), 1024).Decode(&fromKind)
	if err != nil {
		return trace.Wrap(err)
	}
	var toKind unversioned.TypeMeta
	if len(to) != 0 {
		err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(to), 1024).Decode(&toKind)
		if err != nil {
			return trace.Wrap(err)
		}
	}
	switch fromKind.Kind {
	case kindDaemonSet:
		if toKind.Kind != "" && toKind.Kind != kindDaemonSet {
			return trace.BadParameter("don't know how to migrate %v to %v", fromKind.Kind, toKind.Kind)
		}
		return tx.rollbackDaemonSet(ctx, from, to)
	}
	return trace.BadParameter("unsupported resource type %v", fromKind.Kind)
}

func (tx *Transaction) rollbackDaemonSet(ctx context.Context, from, to []byte) error {
	dsFrom := v1beta1.DaemonSet{}
	err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(from), 1024).Decode(&dsFrom)
	if err != nil {
		return trace.Wrap(err)
	}
	if len(to) == 0 {
		log.Infof("deleting DaemonSet(namespace=%v, name=%v)", dsFrom.Name, dsFrom.Namespace)
		err := tx.Client.Extensions().DaemonSets(dsFrom.Name).Delete(dsFrom.Name, nil)
		if err != nil {
			return convertErr(err)
		}
		return nil
	}
	dsTo := v1beta1.DaemonSet{}
	err = yaml.NewYAMLOrJSONDecoder(bytes.NewReader(to), 1024).Decode(&dsTo)
	if err != nil {
		return trace.Wrap(err)
	}
	updater, err := NewDSUpdater(DSConfig{DaemonSet: &dsTo, Client: tx.Client})
	if err != nil {
		return trace.Wrap(err)
	}
	return updater.Update(ctx)
}

func (tx *Transaction) Status(ctx context.Context, transactionNamespace, transactionName string, retryAttempts int, retryPeriod time.Duration) error {
	tr, err := tx.get(transactionNamespace, transactionName)
	if err != nil {
		return trace.Wrap(err)
	}
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	return retry(ctx, retryAttempts, retryPeriod, func() error {
		for _, item := range tr.Spec.Items {
			if item.Status != opStatusCompleted {
				return trace.BadParameter("%v is not completed yet", tr)
			}
			if err := tx.status(ctx, []byte(item.To)); err != nil {
				return trace.Wrap(err)
			}
		}
		return nil
	})
}

func (tx *Transaction) Upsert(ctx context.Context, transactionNamespace, transactionName string, data []byte) error {
	tr, err := tx.createOrRead(&TransactionResource{
		TypeMeta: unversioned.TypeMeta{
			Kind:       kindTransaction,
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
	case kindDaemonSet:
		_, err = tx.upsertDaemonSet(ctx, tr, data)
		return err
	}
	return trace.BadParameter("unsupported resource type %v", kind.Kind)
}

const (
	opStatusCreated    = "created"
	opStatusCompleted  = "completed"
	opStatusRolledBack = "rolledback"
	txStatusRolledBack = "rolledback"
	txStatusInProgress = "in-progress"
	txStatusCommmited  = "commited"
)

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
	g.Infof("upsertDaemonSet(%v/%v)", ds.Namespace, ds.Name)
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

	updater, err := NewDSUpdater(DSConfig{DaemonSet: &ds, Client: tx.Client})
	if err != nil {
		return nil, trace.Wrap(err)
	}

	if err := updater.Update(ctx); err != nil {
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
	if tr.Namespace == "" {
		tr.Namespace = "default"
	}
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
	if tr.Namespace == "" {
		tr.Namespace = "default"
	}
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
