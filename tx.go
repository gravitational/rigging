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
	"context"
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
	"k8s.io/client-go/1.4/kubernetes"
	api "k8s.io/client-go/1.4/pkg/api"
	"k8s.io/client-go/1.4/pkg/api/errors"
	"k8s.io/client-go/1.4/pkg/api/unversioned"
	"k8s.io/client-go/1.4/pkg/api/v1"
	"k8s.io/client-go/1.4/pkg/apis/extensions/v1beta1"
	serializer "k8s.io/client-go/1.4/pkg/runtime/serializer"
	"k8s.io/client-go/1.4/rest"
)

const (
	txResourceName = "transaction.gravitational.io"
	txGroup        = "gravitational.io"
	txVersion      = "v1"
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

type TransactionResource struct {
	unversioned.TypeMeta `json:",inline"`
	v1.ObjectMeta        `json:"metadata,omitempty"`
	Spec                 TransactionSpec `json:"spec"`
}

type TransactionSpec struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Status string `json:"status"`
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
	return nil
}

func (tx *Transaction) List(ctx context.Context, namespace string) (*TransactionList, error) {
	if err := tx.Init(ctx); err != nil {
		return nil, trace.Wrap(err)
	}
	return tx.list(namespace)
}

func (tx *Transaction) list(namespace string) (*TransactionList, error) {
	var result TransactionList
	err := tx.client.Get().
		SubResource("namespaces", namespace, "transactions").
		Do().
		Into(&result)
	if err != nil {
		return nil, convertErr(err)
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
