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
	"fmt"
	"io"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
	"k8s.io/client-go/1.4/kubernetes"
	"k8s.io/client-go/1.4/pkg/api/v1"
)

// NewSecretControl returns new instance of Secret updater
func NewSecretControl(config SecretConfig) (*SecretControl, error) {
	err := config.CheckAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var rc *v1.Secret
	if config.Secret != nil {
		rc = config.Secret
	} else {
		rc, err = ParseSecret(config.Reader)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	rc.Kind = KindSecret
	return &SecretControl{
		SecretConfig: config,
		secret:       *rc,
		Entry: log.WithFields(log.Fields{
			"secret": fmt.Sprintf("%v/%v", Namespace(rc.Namespace), rc.Name),
		}),
	}, nil
}

// SecretConfig  is a Secret control configuration
type SecretConfig struct {
	// Reader with daemon set to update, will be used if present
	Reader io.Reader
	// Secret is already parsed daemon set, will be used if present
	Secret *v1.Secret
	// Client is k8s client
	Client *kubernetes.Clientset
}

func (c *SecretConfig) CheckAndSetDefaults() error {
	if c.Reader == nil && c.Secret == nil {
		return trace.BadParameter("missing parameter Reader or Secret")
	}
	if c.Client == nil {
		return trace.BadParameter("missing parameter Client")
	}
	return nil
}

// SecretControl is a daemon set controller,
// adds various operations, like delete, status check and update
type SecretControl struct {
	SecretConfig
	secret v1.Secret
	*log.Entry
}

func (c *SecretControl) Delete(ctx context.Context, cascade bool) error {
	c.Infof("Delete")
	err := c.Client.Core().Secrets(c.secret.Namespace).Delete(c.secret.Name, nil)
	return convertErr(err)
}

func (c *SecretControl) Upsert(ctx context.Context) error {
	c.Infof("Upsert")
	secrets := c.Client.Core().Secrets(c.secret.Namespace)
	c.secret.UID = ""
	c.secret.SelfLink = ""
	c.secret.ResourceVersion = ""
	_, err := secrets.Get(c.secret.Name)
	err = convertErr(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		_, err = secrets.Create(&c.secret)
		return convertErr(err)
	}
	_, err = secrets.Update(&c.secret)
	return convertErr(err)
}

func (c *SecretControl) Status(ctx context.Context, retryAttempts int, retryPeriod time.Duration) error {
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	c.Infof("Checking status retryAttempts=%v, retryPeriod=%v", retryAttempts, retryPeriod)

	return retry(ctx, retryAttempts, retryPeriod, func() error {
		secrets := c.Client.Core().Secrets(c.secret.Namespace)
		_, err := secrets.Get(c.secret.Name)
		if err != nil {
			return trace.Wrap(err)
		}
		return nil
	})
}
