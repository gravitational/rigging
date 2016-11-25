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
	"k8s.io/client-go/1.4/pkg/api/v1"
	"k8s.io/client-go/1.4/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/1.4/pkg/labels"
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
	if config.DaemonSet != nil {
		return &DSUpdater{DSConfig: config, daemonSet: *config.DaemonSet}, nil
	}
	ds := v1beta1.DaemonSet{}
	err := yaml.NewYAMLOrJSONDecoder(config.Reader, 1024).Decode(&ds)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &DSUpdater{DSConfig: config, daemonSet: ds}, nil
}

type DSConfig struct {
	// Reader with daemon set to update, will be used if present
	Reader io.Reader
	// DaemonSet is already parsed daemon set, will be used if present
	DaemonSet *v1beta1.DaemonSet
	// Client is k8s client
	Client *kubernetes.Clientset
	// RetryAttempts specifies amount of retry attempts for checks
	RetryAttempts int
	// RetryPeriod is a period between Retries
	RetryPeriod time.Duration
}

func (c *DSConfig) CheckAndSetDefaults() error {
	if c.Reader == nil && c.DaemonSet == nil {
		return trace.BadParameter("missing parameter Reader or DaemonSet")
	}
	if c.Client == nil {
		return trace.BadParameter("missing parameter Client")
	}
	return nil
}

// DSUpdater is a daemon set updater
type DSUpdater struct {
	DSConfig
	daemonSet v1beta1.DaemonSet
}

// collectPods returns pods created by this daemon set
func (c *DSUpdater) collectPods(daemonSet *v1beta1.DaemonSet) (map[string]v1.Pod, error) {
	set := make(labels.Set)
	if c.daemonSet.Spec.Selector != nil {
		for key, val := range c.daemonSet.Spec.Template.Labels {
			set[key] = val
		}
	}
	pods := c.Client.Core().Pods(daemonSet.Namespace)
	podList, err := pods.List(api.ListOptions{
		LabelSelector: set.AsSelector(),
	})
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
		if ref.Reference.Kind == "DaemonSet" && ref.Reference.UID == daemonSet.UID {
			currentPods[pod.Spec.NodeName] = pod
			log.Infof("found pod created by this Daemon Set: %v", pod.Name)
		}
	}
	return currentPods, nil
}

func (c *DSUpdater) checkRunning(pods map[string]v1.Pod, nodes []v1.Node) error {
	for _, node := range nodes {
		new, ok := pods[node.Name]
		if !ok {
			return trace.NotFound("not found any pod on node %v", node.Name)
		}
		if new.Status.Phase != v1.PodRunning {
			return trace.CompareFailed("pod %v is not running yet: %v", new.Name, new.Status.Phase)
		}
		log.Infof("node %v: pod %v is up and running", node.Name, new.Name)
	}
	return nil
}

func (c *DSUpdater) Update(ctx context.Context) error {
	log.Infof("Applying DaemonSet(%v/%v) update", c.daemonSet.Namespace, c.daemonSet.Name)
	daemons := c.Client.Extensions().DaemonSets(c.daemonSet.Namespace)
	currentDS, err := daemons.Get(c.daemonSet.Name)
	if err != nil {
		return trace.Wrap(err)
	}
	pods := c.Client.Core().Pods(c.daemonSet.Namespace)
	currentPods, err := c.collectPods(currentDS)
	if err != nil {
		return trace.Wrap(err)
	}
	log.Infof("deleting current daemon set: %v", currentDS.Name)
	err = daemons.Delete(c.daemonSet.Name, nil)
	if err != nil {
		return trace.Wrap(err)
	}
	log.Infof("creating new daemon set: %v", c.daemonSet.Name)
	_, err = daemons.Create(&c.daemonSet)
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
	return nil
}

func (c *DSUpdater) nodeSelector() labels.Selector {
	set := make(labels.Set)
	for key, val := range c.daemonSet.Spec.Template.Spec.NodeSelector {
		set[key] = val
	}
	return set.AsSelector()
}

func (c *DSUpdater) Status(ctx context.Context, retryAttempts int, retryPeriod time.Duration) error {
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	log.Infof("Checking DaemonSet(%v/%v) status retryAttempts=%v, retryPeriod=%v",
		c.daemonSet.Namespace, c.daemonSet.Name, retryAttempts, retryPeriod)

	return retry(ctx, retryAttempts, retryPeriod, func() error {
		daemons := c.Client.Extensions().DaemonSets(c.daemonSet.Namespace)
		currentDS, err := daemons.Get(c.daemonSet.Name)
		if err != nil {
			return trace.Wrap(err)
		}
		currentPods, err := c.collectPods(currentDS)
		if err != nil {
			return trace.Wrap(err)
		}

		nodes, err := c.Client.Core().Nodes().List(api.ListOptions{
			LabelSelector: c.nodeSelector(),
		})
		log.Infof("nodes: %v", c.nodeSelector())
		if err != nil {
			return trace.Wrap(err)
		}

		return c.checkRunning(currentPods, nodes.Items)
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
			return err
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
