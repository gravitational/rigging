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
	"time"

	"github.com/gravitational/trace"

	log "github.com/Sirupsen/logrus"
	"k8s.io/client-go/1.4/kubernetes"
	"k8s.io/client-go/1.4/pkg/api"
	"k8s.io/client-go/1.4/pkg/api/v1"
	batchv1 "k8s.io/client-go/1.4/pkg/apis/batch/v1"
)

func newJobControl(config jobConfig) (*jobControl, error) {
	err := config.checkAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}

	config.job.Kind = KindJob
	return &jobControl{
		jobConfig: config,
		Entry: log.WithFields(log.Fields{
			"job": fmt.Sprintf("%v/%v", Namespace(config.job.Namespace), config.job.Name),
		}),
	}, nil
}

func (c *jobControl) Delete(ctx context.Context, cascade bool) error {
	c.Info("Delete")

	jobs := c.Batch().Jobs(c.job.Namespace)
	currentJob, err := jobs.Get(c.job.Name)
	if err != nil {
		return trace.Wrap(err)
	}

	pods := c.Core().Pods(c.job.Namespace)
	currentPods, err := c.collectPods(currentJob)
	if err != nil {
		return trace.Wrap(err)
	}

	c.Info("deleting current job")
	err = jobs.Delete(c.job.Name, nil)
	if err != nil {
		return trace.Wrap(err)
	}

	if !cascade {
		c.Debug("cascade not set, returning")
	}

	c.Info("deleting pods")
	for _, pod := range currentPods {
		log.Infof("deleting pod %v", pod.Name)
		if err := pods.Delete(pod.Name, nil); err != nil {
			return trace.Wrap(err)
		}
	}
	return nil
}

func (c *jobControl) Upsert(ctx context.Context) error {
	c.Info("Upsert")

	jobs := c.Batch().Jobs(c.job.Namespace)
	currentJob, err := jobs.Get(c.job.Name)
	err = convertErr(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		// Get always returns an object
		currentJob = nil
	}

	pods := c.Core().Pods(c.job.Namespace)
	var currentPods map[string]v1.Pod
	if currentJob != nil {
		c.Infof("currentJob: %v", currentJob.UID)
		currentPods, err = c.collectPods(currentJob)
		if err != nil {
			return trace.Wrap(err)
		}

		c.Info("deleting current job")
		err = jobs.Delete(c.job.Name, nil)
		if err != nil {
			return trace.Wrap(err)
		}
	}

	c.Info("creating new job")
	c.job.UID = ""
	c.job.SelfLink = ""
	c.job.ResourceVersion = ""
	_, err = jobs.Create(&c.job)
	if err != nil {
		return trace.Wrap(err)
	}

	c.Info("created successfully")
	if currentJob != nil {
		c.Info("deleting pods created by previous job")
		for _, pod := range currentPods {
			c.Infof("deleting pod %v", pod.Name)
			if err := pods.Delete(pod.Name, nil); err != nil {
				return trace.Wrap(err)
			}
		}
	}
	return nil
}

func (c *jobControl) Status(ctx context.Context, retryAttempts int, retryPeriod time.Duration) error {
	return pollStatus(ctx, retryAttempts, retryPeriod, c.status, c.Entry)
}

func (c *jobControl) status() error {
	jobs := c.Batch().Jobs(c.job.Namespace)
	job, err := jobs.Get(c.job.Name)
	if err != nil {
		return trace.Wrap(err)
	}

	pods, err := c.collectPods(job)
	if err != nil {
		return trace.Wrap(err)
	}

	nodeSelector := nodeSelector(&c.job.Spec.Template.Spec)
	nodes, err := c.Core().Nodes().List(api.ListOptions{
		LabelSelector: nodeSelector,
	})
	c.Infof("nodes: %q", nodeSelector)
	if err != nil {
		return trace.Wrap(err)
	}
	return checkRunning(pods, nodes.Items, c.Entry)
}

func (c *jobControl) collectPods(job *batchv1.Job) (map[string]v1.Pod, error) {
	var labels map[string]string
	if job.Spec.Selector != nil {
		labels = job.Spec.Selector.MatchLabels
	}
	pods, err := collectPods(job.Namespace, labels, c.Entry, c.Clientset, func(ref api.ObjectReference) bool {
		return ref.Kind == KindJob && ref.UID == job.UID
	})
	return pods, trace.Wrap(err)
}

type jobControl struct {
	jobConfig
	*log.Entry
}

type jobConfig struct {
	job batchv1.Job
	*kubernetes.Clientset
}

func (c *jobConfig) checkAndSetDefaults() error {
	if c.Clientset == nil {
		return trace.BadParameter("missing parameter Clientset")
	}
	return nil
}
