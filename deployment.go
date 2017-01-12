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
	"k8s.io/client-go/1.4/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/1.4/pkg/labels"
)

// NewDeploymentControl returns new instance of Deployment updater
func NewDeploymentControl(config DeploymentConfig) (*DeploymentControl, error) {
	err := config.CheckAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var rc *v1beta1.Deployment
	if config.Deployment != nil {
		rc = config.Deployment
	} else {
		rc, err = ParseDeployment(config.Reader)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	rc.Kind = KindDeployment
	return &DeploymentControl{
		DeploymentConfig: config,
		deployment:       *rc,
		Entry: log.WithFields(log.Fields{
			"deployment": fmt.Sprintf("%v/%v", Namespace(rc.Namespace), rc.Name),
		}),
	}, nil
}

// DeploymentConfig  is a Deployment control configuration
type DeploymentConfig struct {
	// Reader with deployment to update, will be used if present
	Reader io.Reader
	// Deployment is already parsed deployment, will be used if present
	Deployment *v1beta1.Deployment
	// Client is k8s client
	Client *kubernetes.Clientset
}

func (c *DeploymentConfig) CheckAndSetDefaults() error {
	if c.Reader == nil && c.Deployment == nil {
		return trace.BadParameter("missing parameter Reader or Deployment")
	}
	if c.Client == nil {
		return trace.BadParameter("missing parameter Client")
	}
	return nil
}

// DeploymentControl is a deployment controller,
// adds various operations, like delete, status check and update
type DeploymentControl struct {
	DeploymentConfig
	deployment v1beta1.Deployment
	*log.Entry
}

func (c *DeploymentControl) Delete(ctx context.Context, cascade bool) error {
	c.Infof("Delete")
	rcs := c.Client.Extensions().Deployments(c.deployment.Namespace)
	currentDeployment, err := rcs.Get(c.deployment.Name)
	if err != nil {
		return trace.Wrap(err)
	}
	if cascade {
		// scale deployment down to delete all replicas and pods
		var replicas int32
		currentDeployment.Spec.Replicas = &replicas
		currentDeployment, err = rcs.Update(currentDeployment)
		if err != nil {
			return convertErr(err)
		}
	}
	err = rcs.Delete(c.deployment.Name, nil)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (c *DeploymentControl) Upsert(ctx context.Context) error {
	c.Infof("Upsert")
	deployments := c.Client.Extensions().Deployments(c.deployment.Namespace)
	c.deployment.UID = ""
	c.deployment.SelfLink = ""
	c.deployment.ResourceVersion = ""
	_, err := deployments.Get(c.deployment.Name)
	err = convertErr(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		_, err = deployments.Create(&c.deployment)
		return convertErr(err)
	}
	_, err = deployments.Update(&c.deployment)
	return convertErr(err)
}

func (c *DeploymentControl) nodeSelector() labels.Selector {
	set := make(labels.Set)
	for key, val := range c.deployment.Spec.Template.Spec.NodeSelector {
		set[key] = val
	}
	return set.AsSelector()
}

func (c *DeploymentControl) Status(ctx context.Context, retryAttempts int, retryPeriod time.Duration) error {
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	c.Infof("Checking status retryAttempts=%v, retryPeriod=%v", retryAttempts, retryPeriod)

	return retry(ctx, retryAttempts, retryPeriod, func() error {
		rcs := c.Client.Extensions().Deployments(c.deployment.Namespace)
		currentDeployment, err := rcs.Get(c.deployment.Name)
		if err != nil {
			return trace.Wrap(err)
		}
		var replicas int32 = 1
		if currentDeployment.Spec.Replicas != nil {
			replicas = *(currentDeployment.Spec.Replicas)
		}
		if currentDeployment.Status.UpdatedReplicas != replicas {
			return trace.CompareFailed("expected replicas: %v, updated: %v", replicas, currentDeployment.Status.UpdatedReplicas)
		}
		if currentDeployment.Status.AvailableReplicas != replicas {
			return trace.CompareFailed("expected replicas: %v, available: %v", replicas, currentDeployment.Status.AvailableReplicas)
		}
		return nil
	})
}
