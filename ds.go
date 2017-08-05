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
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/pkg/labels"
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
			"ds": formatMeta(ds.ObjectMeta),
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
	var labels map[string]string
	if daemonSet.Spec.Selector != nil {
		labels = daemonSet.Spec.Selector.MatchLabels
	}
	pods, err := CollectPods(daemonSet.Namespace, labels, c.Entry, c.Client, func(ref api.ObjectReference) bool {
		return ref.Kind == KindDaemonSet && ref.UID == daemonSet.UID
	})
	return pods, trace.Wrap(err)
}

func (c *DSControl) Delete(ctx context.Context, cascade bool) error {
	c.Infof("delete %v", formatMeta(c.daemonSet.ObjectMeta))

	daemons := c.Client.Extensions().DaemonSets(c.daemonSet.Namespace)
	currentDS, err := daemons.Get(c.daemonSet.Name)
	if err != nil {
		return ConvertError(err)
	}
	pods := c.Client.Core().Pods(c.daemonSet.Namespace)
	currentPods, err := c.collectPods(currentDS)
	if err != nil {
		return trace.Wrap(err)
	}
	c.Infof("deleting current daemon set")
	err = daemons.Delete(c.daemonSet.Name, nil)
	if err != nil {
		return ConvertError(err)
	}
	if !cascade {
		c.Infof("cascade not set, returning")
	}
	c.Infof("deleting pods")
	for _, pod := range currentPods {
		log.Infof("deleting pod %v", pod.Name)
		if err := pods.Delete(pod.Name, nil); err != nil {
			return ConvertError(err)
		}
	}
	return nil
}

func (c *DSControl) Upsert(ctx context.Context) error {
	c.Infof("upsert %v", formatMeta(c.daemonSet.ObjectMeta))

	daemons := c.Client.Extensions().DaemonSets(c.daemonSet.Namespace)
	currentDS, err := daemons.Get(c.daemonSet.Name)
	err = ConvertError(err)
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
			return ConvertError(err)
		}
	}
	c.Infof("creating new daemon set")
	c.daemonSet.UID = ""
	c.daemonSet.SelfLink = ""
	c.daemonSet.ResourceVersion = ""
	_, err = daemons.Create(&c.daemonSet)
	if err != nil {
		return ConvertError(err)
	}
	c.Infof("created successfully")
	if currentDS != nil {
		c.Infof("deleting pods created by previous daemon set")
		for _, pod := range currentPods {
			c.Infof("deleting pod %v", pod.Name)
			if err := pods.Delete(pod.Name, nil); err != nil {
				return ConvertError(err)
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

func (c *DSControl) Status() error {
	daemons := c.Client.Extensions().DaemonSets(c.daemonSet.Namespace)
	currentDS, err := daemons.Get(c.daemonSet.Name)
	if err != nil {
		return ConvertError(err)
	}
	currentPods, err := c.collectPods(currentDS)
	if err != nil {
		return trace.Wrap(err)
	}

	nodes, err := c.Client.Core().Nodes().List(v1.ListOptions{
		LabelSelector: c.nodeSelector().String(),
	})
	if err != nil {
		return ConvertError(err)
	}
	return checkRunning(currentPods, nodes.Items, c.Entry)
}
