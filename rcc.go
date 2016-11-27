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
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
	"k8s.io/client-go/1.4/kubernetes"
	"k8s.io/client-go/1.4/pkg/api"
	"k8s.io/client-go/1.4/pkg/api/v1"
	"k8s.io/client-go/1.4/pkg/labels"
)

// NewRCControl returns new instance of ReplicationController updater
func NewRCControl(config RCConfig) (*RCControl, error) {
	err := config.CheckAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var rc *v1.ReplicationController
	if config.ReplicationController != nil {
		rc = config.ReplicationController
	} else {
		rc, err = ParseReplicationController(config.Reader)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	rc.Kind = KindReplicationController
	return &RCControl{
		RCConfig:              config,
		replicationController: *rc,
		Entry: log.WithFields(log.Fields{
			"rc": fmt.Sprintf("%v/%v", Namespace(rc.Namespace), rc.Name),
		}),
	}, nil
}

// RCConfig is a ReplicationController control configuration
type RCConfig struct {
	// Reader with daemon set to update, will be used if present
	Reader io.Reader
	// ReplicationController is already parsed daemon set, will be used if present
	ReplicationController *v1.ReplicationController
	// Client is k8s client
	Client *kubernetes.Clientset
}

func (c *RCConfig) CheckAndSetDefaults() error {
	if c.Reader == nil && c.ReplicationController == nil {
		return trace.BadParameter("missing parameter Reader or ReplicationController")
	}
	if c.Client == nil {
		return trace.BadParameter("missing parameter Client")
	}
	return nil
}

// RCControl is a daemon set controller,
// adds various operations, like delete, status check and update
type RCControl struct {
	RCConfig
	replicationController v1.ReplicationController
	*log.Entry
}

// collectPods returns pods created by this RC
func (c *RCControl) collectPods(replicationController *v1.ReplicationController) ([]v1.Pod, error) {
	set := make(labels.Set)
	for key, val := range c.replicationController.Spec.Selector {
		set[key] = val
	}
	pods := c.Client.Core().Pods(replicationController.Namespace)
	podList, err := pods.List(api.ListOptions{
		LabelSelector: set.AsSelector(),
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	c.Infof("collectPods(%v) -> %v", set, len(podList.Items))
	currentPods := make([]v1.Pod, 0)
	for _, pod := range podList.Items {
		createdBy, ok := pod.Annotations[AnnotationCreatedBy]
		if !ok {
			continue
		}
		ref, err := ParseSerializedReference(strings.NewReader(createdBy))
		if err != nil {
			log.Warningf(trace.DebugReport(err))
			continue
		}
		c.Infof("collectPods(%v, %v, %v)", ref.Reference.Kind, ref.Reference.UID, replicationController.UID)
		if ref.Reference.Kind == KindReplicationController && ref.Reference.UID == replicationController.UID {
			currentPods = append(currentPods, pod)
			c.Infof("found pod created by this RC: %v", pod.Name)
		}
	}
	return currentPods, nil
}

func (c *RCControl) Delete(ctx context.Context, cascade bool) error {
	c.Infof("Delete")
	rcs := c.Client.Core().ReplicationControllers(c.replicationController.Namespace)
	currentRC, err := rcs.Get(c.replicationController.Name)
	if err != nil {
		return trace.Wrap(err)
	}
	pods := c.Client.Core().Pods(c.replicationController.Namespace)
	currentPods, err := c.collectPods(currentRC)
	if err != nil {
		return trace.Wrap(err)
	}
	c.Infof("deleting")
	err = rcs.Delete(c.replicationController.Name, nil)
	if err != nil {
		return trace.Wrap(err)
	}
	if !cascade {
		c.Infof("cascade not set, returning")
	}
	c.Infof("deleting pods %v", len(currentPods))
	for _, pod := range currentPods {
		log.Infof("deleting pod %v", pod.Name)
		if err := pods.Delete(pod.Name, nil); err != nil {
			return trace.Wrap(err)
		}
	}
	return nil
}

func (c *RCControl) Upsert(ctx context.Context) error {
	c.Infof("Upsert")
	rcs := c.Client.Core().ReplicationControllers(c.replicationController.Namespace)
	c.replicationController.UID = ""
	c.replicationController.SelfLink = ""
	c.replicationController.ResourceVersion = ""
	currentRC, err := rcs.Get(c.replicationController.Name)
	err = convertErr(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		_, err = rcs.Create(&c.replicationController)
		return convertErr(err)
	}
	// delete all pods and the old replication controller
	control, err := NewRCControl(RCConfig{ReplicationController: currentRC, Client: c.Client})
	err = control.Delete(ctx, true)
	if err != nil {
		return trace.Wrap(err)
	}
	_, err = rcs.Create(&c.replicationController)
	return convertErr(err)
}

func (c *RCControl) nodeSelector() labels.Selector {
	set := make(labels.Set)
	for key, val := range c.replicationController.Spec.Template.Spec.NodeSelector {
		set[key] = val
	}
	return set.AsSelector()
}

func (c *RCControl) Status(ctx context.Context, retryAttempts int, retryPeriod time.Duration) error {
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	c.Infof("Checking status retryAttempts=%v, retryPeriod=%v", retryAttempts, retryPeriod)

	return retry(ctx, retryAttempts, retryPeriod, func() error {
		rcs := c.Client.Core().ReplicationControllers(c.replicationController.Namespace)
		currentRC, err := rcs.Get(c.replicationController.Name)
		if err != nil {
			return trace.Wrap(err)
		}
		var replicas int32 = 1
		if currentRC.Spec.Replicas != nil {
			replicas = *(currentRC.Spec.Replicas)
		}
		if currentRC.Status.Replicas != replicas {
			return trace.CompareFailed("expected replicas: %v, ready: %#v", replicas, currentRC.Status.Replicas)
		}
		pods, err := c.collectPods(currentRC)
		if err != nil {
			return trace.Wrap(err)
		}
		for _, pod := range pods {
			if pod.Status.Phase != v1.PodRunning {
				return trace.CompareFailed("pod %v is not running yet: %v", pod.Name, pod.Status.Phase)
			}
		}
		return nil
	})
}
