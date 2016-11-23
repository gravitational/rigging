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
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
	"k8s.io/client-go/1.4/kubernetes"
	"k8s.io/client-go/1.4/pkg/api"
	"k8s.io/client-go/1.4/pkg/api/unversioned"
	"k8s.io/client-go/1.4/pkg/api/v1"
	v1beta1 "k8s.io/client-go/1.4/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/1.4/pkg/util/yaml"
)

const (
	// DefaultRetryAttempts specifies amount of retry attempts for checks
	DefaultRetryAttempts = 60
	// RetryPeriod is a period between Retries
	DefaultRetryPeriod = time.Second
)

// NewDSUpdater returns new instance of DaemonSet updater
func NewDSUpdater(config DSConfig) (*DSUpdater, error) {
	if err := config.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	ds := v1beta1.DaemonSet{}
	err := yaml.NewYAMLOrJSONDecoder(config.Reader, 1024).Decode(&ds)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &DSUpdater{DSConfig: config, daemonSet: ds}, nil
}

type DSConfig struct {
	// Reader with daemon set to update
	Reader io.Reader
	// Client is k8s client
	Client *kubernetes.Clientset
	// RetryAttempts specifies amount of retry attempts for checks
	RetryAttempts int
	// RetryPeriod is a period between Retries
	RetryPeriod time.Duration
}

func (c *DSConfig) CheckAndSetDefaults() error {
	if c.Reader == nil {
		return trace.BadParameter("missing parameter Reader")
	}
	if c.Client == nil {
		return trace.BadParameter("missing parameter Client")
	}
	if c.RetryAttempts == 0 {
		c.RetryAttempts = DefaultRetryAttempts
	}
	if c.RetryPeriod == 0 {
		c.RetryPeriod = DefaultRetryPeriod
	}
	return nil
}

// DSUpdater is a daemon set updater
type DSUpdater struct {
	DSConfig
	daemonSet v1beta1.DaemonSet
}

func (c *DSUpdater) collectPods() (map[string]v1.Pod, error) {
	// collect pods to be deleted
	pods := c.Client.Core().Pods(c.daemonSet.Namespace)
	podList, err := pods.List(api.ListOptions{})
	currentPods := make(map[string]v1.Pod, 0)
	for _, pod := range podList.Items {
		createdBy, ok := pod.Annotations["kubernetes.io/created-by"]
		if !ok {
			continue
		}
		ref := api.SerializedReference{}
		err = yaml.NewYAMLOrJSONDecoder(strings.NewReader(createdBy), 1024).Decode(&ref)
		if err != nil {
			log.Warningf(trace.DebugReport(err))
			continue
		}
		if ref.Reference.Kind == "DaemonSet" && ref.Reference.Name == c.daemonSet.Name {
			currentPods[pod.Spec.NodeName] = pod
			log.Infof("found pod created by this Daemon Set: %v", pod.Name)
		}
	}
	return currentPods, nil
}

func (c *DSUpdater) checkRunning(oldPods, newPods map[string]v1.Pod) error {
	for nodeName, old := range oldPods {
		new, ok := newPods[nodeName]
		if !ok {
			return trace.NotFound("not found any pod on node %v", nodeName)
		}
		if new.Name == old.Name {
			return trace.CompareFailed("stil same pod %v, waiting for pod to stop", new.Name)
		}
		if new.Status.Phase != v1.PodRunning {
			return trace.CompareFailed("pod %v is not running yet: %v", new.Name, new.Status.Phase)
		}
		log.Infof("node %v: pod %v has been updated to %v", nodeName, old.Name, new.Name)
	}
	return nil
}

func (c *DSUpdater) Run(ctx context.Context) error {
	log.Infof("Starting DaemonSet(%v/%v) update retryAttempts=%v, retryPeriod=%v", c.daemonSet.Namespace, c.daemonSet.Name, c.RetryAttempts, c.RetryPeriod)
	daemons := c.Client.Extensions().DaemonSets(c.daemonSet.Namespace)
	currentDS, err := daemons.Get(c.daemonSet.Name)
	if err != nil {
		return trace.Wrap(err)
	}
	pods := c.Client.Core().Pods(c.daemonSet.Namespace)
	currentPods, err := c.collectPods()
	if err != nil {
		return trace.Wrap(err)
	}

	return withRecover(func() error {
		log.Infof("deleting current daemon set: %v", currentDS.Name)
		err = daemons.Delete(c.daemonSet.Name, nil)
		if err != nil {
			return trace.Wrap(err)
		}
		log.Infof("creating new daemon set: %v", c.daemonSet.Name)
		_, err := daemons.Create(&c.daemonSet)
		if err != nil {
			return trace.Wrap(err)
		}
		log.Infof("new daemon set created successfully")
		log.Infof("deleting pods created by previous daemon set")
		for _, pod := range currentPods {
			log.Infof("deleting pod %v", pod.Name)
			if err := pods.Delete(pod.Name, nil); err != nil {
				return trace.Wrap(err)
			}
		}
		log.Infof("wating for new pods to start up in place of other pods")
		return retry(ctx, c.RetryAttempts, c.RetryPeriod, func() error {
			newPods, err := c.collectPods()
			if err != nil {
				return trace.Wrap(err)
			}
			return c.checkRunning(currentPods, newPods)
		})
	}, func() error {
		log.Infof("Rolling back update of %v", c.daemonSet.Name)
		log.Infof("finding any pods to delete")
		newPods, err := c.collectPods()
		if err != nil {
			return trace.Wrap(err)
		}
		log.Infof("deleting pods created by new daemon set")
		for _, pod := range newPods {
			log.Infof("deleting pod %v", pod.Name)
			if err := pods.Delete(pod.Name, nil); err != nil {
				return trace.Wrap(err)
			}
		}
		err = daemons.Delete(c.daemonSet.Name, nil)
		if err != nil {
			log.Warningf("failed to delete daemon set: %v, err: %v", c.daemonSet.Name, err)
		}
		currentDS.ResourceVersion = ""
		currentDS.UID = ""
		currentDS.CreationTimestamp = unversioned.Time{}
		_, err = daemons.Create(currentDS)
		if err != nil {
			return trace.Wrap(err)
		}
		return nil
	})
}

func retry(ctx context.Context, times int, period time.Duration, fn func() error) error {
	var err error
	for i := 0; i < times; i += 1 {
		err = fn()
		if err == nil {
			return nil
		}
		log.Infof("attempt %v, result: %v, retry in %v", i+1, err, period)
		select {
		case <-ctx.Done():
			log.Infof("context is closing, return")
		case <-time.After(period):
		}
	}
	return err
}

func withRecover(fn func() error, recoverFn func() error) error {
	shouldRecover := true
	defer func() {
		if !shouldRecover {
			log.Infof("no recovery needed, returning")
			return
		}
		log.Infof("need to recover")
		err := recoverFn()
		if err != nil {
			log.Error(trace.DebugReport(err))
			return
		}
		log.Infof("recovered successfully")
		return
	}()

	if err := fn(); err != nil {
		return err
	}
	shouldRecover = false
	return nil
}
