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
	"k8s.io/client-go/1.4/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/1.4/pkg/labels"
)

// NewDSControl returns new instance of DaemonSet updater
func NewDSControl(config DSConfig) (*DSControl, error) {
	err := config.CheckAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var ds *v1beta1.DaemonSet
	if config.DaemonSet != nil {
		ds = config.DaemonSet
	} else {
		ds, err = ParseDaemonSet(config.Reader)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	// sometimes existing objects pulled from the API don't have type set
	ds.Kind = KindDaemonSet
	return &DSControl{
		DSConfig:  config,
		daemonSet: *ds,
		Entry: log.WithFields(log.Fields{
			"ds": fmt.Sprintf("%v/%v", Namespace(ds.Namespace), ds.Name),
		}),
	}, nil
}

// DSConfig is a DaemonSet control configuration
type DSConfig struct {
	// Reader with daemon set to update, will be used if present
	Reader io.Reader
	// DaemonSet is already parsed daemon set, will be used if present
	DaemonSet *v1beta1.DaemonSet
	// Client is k8s client
	Client *kubernetes.Clientset
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

// DSControl is a daemon set controller,
// adds various operations, like delete, status check and update
type DSControl struct {
	DSConfig
	daemonSet v1beta1.DaemonSet
	*log.Entry
}

// collectPods returns pods created by this daemon set
func (c *DSControl) collectPods(daemonSet *v1beta1.DaemonSet) (map[string]v1.Pod, error) {
	set := make(labels.Set)
	if c.daemonSet.Spec.Selector != nil {
		for key, val := range c.daemonSet.Spec.Selector.MatchLabels {
			set[key] = val
		}
	}
	pods := c.Client.Core().Pods(daemonSet.Namespace)
	podList, err := pods.List(api.ListOptions{
		LabelSelector: set.AsSelector(),
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	currentPods := make(map[string]v1.Pod, 0)
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
		if ref.Reference.Kind == KindDaemonSet && ref.Reference.UID == daemonSet.UID {
			currentPods[pod.Spec.NodeName] = pod
			c.Infof("found pod created by this Daemon Set: %v", pod.Name)
		}
	}
	return currentPods, nil
}

func (c *DSControl) checkRunning(pods map[string]v1.Pod, nodes []v1.Node) error {
	for _, node := range nodes {
		new, ok := pods[node.Name]
		if !ok {
			return trace.NotFound("not found any pod on node %v", node.Name)
		}
		if new.Status.Phase != v1.PodRunning {
			return trace.CompareFailed("pod %v is not running yet: %v", new.Name, new.Status.Phase)
		}
		c.Infof("node %v: pod %v is up and running", node.Name, new.Name)
	}
	return nil
}

func (c *DSControl) Delete(ctx context.Context, cascade bool) error {
	c.Infof("Delete")
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
	c.Infof("deleting current daemon set")
	err = daemons.Delete(c.daemonSet.Name, nil)
	if err != nil {
		return trace.Wrap(err)
	}
	if !cascade {
		c.Infof("cascade not set, returning")
	}
	c.Infof("deleting pods")
	for _, pod := range currentPods {
		log.Infof("deleting pod %v", pod.Name)
		if err := pods.Delete(pod.Name, nil); err != nil {
			return trace.Wrap(err)
		}
	}
	return nil
}

func (c *DSControl) Upsert(ctx context.Context) error {
	c.Infof("Upsert")
	daemons := c.Client.Extensions().DaemonSets(c.daemonSet.Namespace)
	currentDS, err := daemons.Get(c.daemonSet.Name)
	err = convertErr(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		// api always returns object, this is inconvenent
		currentDS = nil
	}
	pods := c.Client.Core().Pods(c.daemonSet.Namespace)
	var currentPods map[string]v1.Pod
	if currentDS != nil {
		c.Infof("currentDS: %v", currentDS.UID)
		currentPods, err = c.collectPods(currentDS)
		if err != nil {
			return trace.Wrap(err)
		}
		c.Infof("deleting current daemon set")
		err = daemons.Delete(c.daemonSet.Name, nil)
		if err != nil {
			return trace.Wrap(err)
		}
	}
	c.Infof("creating new daemon set")
	c.daemonSet.UID = ""
	c.daemonSet.SelfLink = ""
	c.daemonSet.ResourceVersion = ""
	_, err = daemons.Create(&c.daemonSet)
	if err != nil {
		return trace.Wrap(err)
	}
	c.Infof("created successfully")
	if currentDS != nil {
		c.Infof("deleting pods created by previous daemon set")
		for _, pod := range currentPods {
			c.Infof("deleting pod %v", pod.Name)
			if err := pods.Delete(pod.Name, nil); err != nil {
				return trace.Wrap(err)
			}
		}
	}
	return nil
}

func (c *DSControl) nodeSelector() labels.Selector {
	set := make(labels.Set)
	for key, val := range c.daemonSet.Spec.Template.Spec.NodeSelector {
		set[key] = val
	}
	return set.AsSelector()
}

func (c *DSControl) Status(ctx context.Context, retryAttempts int, retryPeriod time.Duration) error {
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	c.Infof("Checking status retryAttempts=%v, retryPeriod=%v", retryAttempts, retryPeriod)

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
		c.Infof("nodes: %v", c.nodeSelector())
		if err != nil {
			return trace.Wrap(err)
		}
		return c.checkRunning(currentPods, nodes.Items)
	})
}
