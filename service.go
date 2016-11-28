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

// NewServiceControl returns new instance of Service updater
func NewServiceControl(config ServiceConfig) (*ServiceControl, error) {
	err := config.CheckAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var rc *v1.Service
	if config.Service != nil {
		rc = config.Service
	} else {
		rc, err = ParseService(config.Reader)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	rc.Kind = KindService
	return &ServiceControl{
		ServiceConfig: config,
		service:       *rc,
		Entry: log.WithFields(log.Fields{
			"service": fmt.Sprintf("%v/%v", Namespace(rc.Namespace), rc.Name),
		}),
	}, nil
}

// ServiceConfig  is a Service control configuration
type ServiceConfig struct {
	// Reader with daemon set to update, will be used if present
	Reader io.Reader
	// Service is already parsed daemon set, will be used if present
	Service *v1.Service
	// Client is k8s client
	Client *kubernetes.Clientset
}

func (c *ServiceConfig) CheckAndSetDefaults() error {
	if c.Reader == nil && c.Service == nil {
		return trace.BadParameter("missing parameter Reader or Service")
	}
	if c.Client == nil {
		return trace.BadParameter("missing parameter Client")
	}
	return nil
}

// ServiceControl is a daemon set controller,
// adds various operations, like delete, status check and update
type ServiceControl struct {
	ServiceConfig
	service v1.Service
	*log.Entry
}

func (c *ServiceControl) Delete(ctx context.Context, cascade bool) error {
	c.Infof("Delete")
	err := c.Client.Core().Services(c.service.Namespace).Delete(c.service.Name, nil)
	return convertErr(err)
}

func (c *ServiceControl) Upsert(ctx context.Context) error {
	c.Infof("Upsert")
	services := c.Client.Core().Services(c.service.Namespace)
	c.service.UID = ""
	c.service.SelfLink = ""
	c.service.ResourceVersion = ""
	_, err := services.Get(c.service.Name)
	err = convertErr(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		_, err = services.Create(&c.service)
		return convertErr(err)
	}
	_, err = services.Update(&c.service)
	return convertErr(err)
}

func (c *ServiceControl) Status(ctx context.Context, retryAttempts int, retryPeriod time.Duration) error {
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	c.Infof("Checking status retryAttempts=%v, retryPeriod=%v", retryAttempts, retryPeriod)

	return retry(ctx, retryAttempts, retryPeriod, func() error {
		services := c.Client.Core().Services(c.service.Namespace)
		_, err := services.Get(c.service.Name)
		if err != nil {
			return trace.Wrap(err)
		}
		return nil
	})
}
