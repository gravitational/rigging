/*
Copyright (C) 2018 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rigging

import (
	"context"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// NewStatefulSetControl returns new instance of the StatefulSet controller
func NewStatefulSetControl(config StatefulSetConfig) (*StatefulSetControl, error) {
	err := config.CheckAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}

	var statefulSet *appsv1.StatefulSet
	statefulSet.Kind = KindStatefulSet
	return &StatefulSetControl{
		StatefulSetConfig: config,
		statefulSet:       *statefulSet,
		Entry: log.WithFields(log.Fields{
			"statefulset": formatMeta(statefulSet.ObjectMeta),
		}),
	}, nil
}

// StatefulSetConfig is a StatefulSet control configuration
type StatefulSetConfig struct {
	// StatefulSet is already parsed statefulset, will be used if present
	StatefulSet *appsv1.StatefulSet
	// Client is k8s client
	Client *kubernetes.Clientset
}

// CheckAndSetDefaults validates this configuration object and sets defaults
func (c *StatefulSetConfig) CheckAndSetDefaults() error {
	var errors []error
	if c.StatefulSet == nil {
		errors = append(errors, trace.BadParameter("missing parameter StatefulSet"))
	}
	if c.Client == nil {
		errors = append(errors, trace.BadParameter("missing parameter Client"))
	}
	return trace.NewAggregate(errors...)
}

// StatefulSetControl is a statefulset controller,
// adds various operations, like delete, status check and update
type StatefulSetControl struct {
	StatefulSetConfig
	statefulSet appsv1.StatefulSet
	*log.Entry
}

// Upsert creates or updates a statefulset resource
func (c *StatefulSetControl) Upsert(ctx context.Context) error {
	c.Infof("Upsert %v", formatMeta(c.StatefulSet.ObjectMeta))

	statefulset := c.Client.AppsV1().StatefulSets(c.statefulSet.Namespace)
	currentResource, err := statefulset.Get(c.statefulSet.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		currentResource = nil
	}

	if currentResource != nil {
		control, err := NewStatefulSetControl(StatefulSetConfig{StatefulSet: currentResource, Client: c.Client})
		if err != nil {
			return trace.Wrap(err)
		}
		cascade := true
		err = control.Delete(ctx, cascade)
		if err != nil {
			return ConvertError(err)
		}
	}

	c.Info("Creating new statefulset.")
	c.statefulSet.UID = ""
	c.statefulSet.SelfLink = ""
	c.statefulSet.ResourceVersion = ""

	err = withExponentialBackoff(func() error {
		_, err = statefulset.Create(&c.statefulSet)
		return ConvertError(err)
	})
	return trace.Wrap(err)

}

// collectPods returns pods created by this statefulset
func (c *StatefulSetControl) collectPods(statefulSet *appsv1.StatefulSet) (map[string]v1.Pod, error) {
	var labels map[string]string
	if statefulSet.Spec.Selector != nil {
		labels = statefulSet.Spec.Selector.MatchLabels
	}
	pods, err := CollectPods(statefulSet.Namespace, labels, c.Entry, c.Client, func(ref metav1.OwnerReference) bool {
		return ref.Kind == KindStatefulSet && ref.UID == statefulSet.UID
	})
	return pods, trace.Wrap(err)
}

// Delete deletes this statefulset resource
func (c *StatefulSetControl) Delete(ctx context.Context, cascade bool) error {
	c.Infof("Deleting statefulset %v.", formatMeta(c.statefulSet.ObjectMeta))

	statefulset := c.Client.AppsV1().StatefulSets(c.statefulSet.Namespace)
	currentResource, err := statefulset.Get(c.statefulSet.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		return ConvertError(err)
	}
	pods := c.Client.CoreV1().Pods(c.statefulSet.Namespace)
	currentPods, err := c.collectPods(currentResource)
	if err != nil {
		return trace.Wrap(err)
	}

	c.Infof("Deleting current statefulset %v.", formatMeta(currentResource.ObjectMeta))
	deletePolicy := metav1.DeletePropagationForeground
	err = statefulset.Delete(c.statefulSet.Name, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if err != nil {
		return ConvertError(err)
	}

	err = waitForObjectDeletion(func() error {
		_, err := statefulset.Get(c.statefulSet.Name, metav1.GetOptions{})
		return ConvertError(err)
	})
	if err != nil {
		return trace.Wrap(err)
	}

	if !cascade {
		c.Debug("Cascade not set, returning.")
	}
	err = deletePods(pods, currentPods, *c.Entry)
	return trace.Wrap(err)
}

func (c *StatefulSetControl) nodeSelector() labels.Selector {
	set := make(labels.Set)
	for key, val := range c.statefulSet.Spec.Template.Spec.NodeSelector {
		set[key] = val
	}
	return set.AsSelector()
}

// Status returns status of pods for this resource
func (c *StatefulSetControl) Status() error {
	statefulset := c.Client.AppsV1().StatefulSets(c.statefulSet.Namespace)
	currentResource, err := statefulset.Get(c.statefulSet.Name, metav1.GetOptions{})
	if err != nil {
		return ConvertError(err)
	}
	currentPods, err := c.collectPods(currentResource)
	if err != nil {
		return trace.Wrap(err)
	}

	nodes, err := c.Client.CoreV1().Nodes().List(metav1.ListOptions{
		LabelSelector: c.nodeSelector().String(),
	})
	if err != nil {
		return ConvertError(err)
	}
	return checkRunning(currentPods, nodes.Items, c.Entry)
}
