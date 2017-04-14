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
	"io"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/pkg/labels"
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
			"deployment": formatMeta(rc.ObjectMeta),
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
	c.Infof("delete %v", formatMeta(c.deployment.ObjectMeta))

	deployments := c.Client.Extensions().Deployments(c.deployment.Namespace)
	currentDeployment, err := deployments.Get(c.deployment.Name)
	if err != nil {
		return ConvertError(err)
	}
	if cascade {
		// scale deployment down to delete all replicas and pods
		var replicas int32
		currentDeployment.Spec.Replicas = &replicas
		currentDeployment, err = deployments.Update(currentDeployment)
		if err != nil {
			return ConvertError(err)
		}
	}
	err = deployments.Delete(c.deployment.Name, nil)
	return ConvertError(err)
}

func (c *DeploymentControl) Upsert(ctx context.Context) error {
	c.Infof("upsert %v", formatMeta(c.deployment.ObjectMeta))

	deployments := c.Client.Extensions().Deployments(c.deployment.Namespace)
	c.deployment.UID = ""
	c.deployment.SelfLink = ""
	c.deployment.ResourceVersion = ""
	_, err := deployments.Get(c.deployment.Name)
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		_, err = deployments.Create(&c.deployment)
		return ConvertError(err)
	}
	_, err = deployments.Update(&c.deployment)
	return ConvertError(err)
}

func (c *DeploymentControl) nodeSelector() labels.Selector {
	set := make(labels.Set)
	for key, val := range c.deployment.Spec.Template.Spec.NodeSelector {
		set[key] = val
	}
	return set.AsSelector()
}

func (c *DeploymentControl) Status() error {
	deployments := c.Client.Extensions().Deployments(c.deployment.Namespace)
	currentDeployment, err := deployments.Get(c.deployment.Name)
	if err != nil {
		return ConvertError(err)
	}
	var replicas int32 = 1
	if currentDeployment.Spec.Replicas != nil {
		replicas = *(currentDeployment.Spec.Replicas)
	}
	deployment := formatMeta(c.deployment.ObjectMeta)
	if currentDeployment.Status.UpdatedReplicas != replicas {
		return trace.CompareFailed("deployment %v not successful: expected replicas: %v, updated: %v",
			deployment, replicas, currentDeployment.Status.UpdatedReplicas)
	}
	if currentDeployment.Status.AvailableReplicas != replicas {
		return trace.CompareFailed("deployment %v not successful: expected replicas: %v, available: %v",
			deployment, replicas, currentDeployment.Status.AvailableReplicas)
	}
	return nil
}
