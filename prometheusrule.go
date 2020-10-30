// Copyright 2019 Gravitational Inc.
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

	monitoringv1 "github.com/coreos/prometheus-operator/pkg/apis/monitoring/v1"
	monitoring "github.com/coreos/prometheus-operator/pkg/client/versioned"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewPrometheusRuleControl returns new instance of PrometheusRule updater
func NewPrometheusRuleControl(config PrometheusRuleConfig) (*PrometheusRuleControl, error) {
	if err := config.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	return &PrometheusRuleControl{
		PrometheusRuleConfig: config,
		FieldLogger: log.WithFields(log.Fields{
			"prometheusRule": formatMeta(config.ObjectMeta),
		}),
	}, nil
}

// PrometheusRuleConfig  is a PrometheusRule control configuration
type PrometheusRuleConfig struct {
	// PrometheusRule is the parsed PrometheusRule resource
	*monitoringv1.PrometheusRule
	// Client is monitoring API client
	Client *monitoring.Clientset
}

// CheckAndSetDefaults validates the config
func (c *PrometheusRuleConfig) CheckAndSetDefaults() error {
	if c.PrometheusRule == nil {
		return trace.BadParameter("missing parameter PrometheusRule")
	}
	if c.Client == nil {
		return trace.BadParameter("missing parameter Client")
	}
	updateTypeMetaPrometheusRule(c.PrometheusRule)
	return nil
}

// PrometheusRuleControl a controller for PrometheusRule resources
type PrometheusRuleControl struct {
	PrometheusRuleConfig
	log.FieldLogger
}

func (c *PrometheusRuleControl) Delete(ctx context.Context, cascade bool) error {
	c.Infof("delete %v", formatMeta(c.PrometheusRule.ObjectMeta))

	err := c.Client.MonitoringV1().PrometheusRules(c.PrometheusRule.Namespace).Delete(c.PrometheusRule.Name, nil)
	return ConvertError(err)
}

func (c *PrometheusRuleControl) Upsert(ctx context.Context) error {
	c.Infof("upsert %v", formatMeta(c.PrometheusRule.ObjectMeta))

	client := c.Client.MonitoringV1().PrometheusRules(c.PrometheusRule.Namespace)
	c.PrometheusRule.UID = ""
	c.PrometheusRule.SelfLink = ""
	c.PrometheusRule.ResourceVersion = ""
	currentRule, err := client.Get(c.PrometheusRule.Name, metav1.GetOptions{})
	err = ConvertError(err)
	if err != nil {
		if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		_, err = client.Create(c.PrometheusRule)
		return ConvertError(err)
	}

	if checkCustomerManagedResource(currentRule.Annotations) {
		c.WithField("prometheusrule", formatMeta(c.ObjectMeta)).Info("Skipping update since object is customer managed.")
		return nil
	}

	c.PrometheusRule.ResourceVersion = currentRule.ResourceVersion
	_, err = client.Update(c.PrometheusRule)
	return ConvertError(err)
}

func (c *PrometheusRuleControl) Status() error {
	client := c.Client.MonitoringV1().PrometheusRules(c.PrometheusRule.Namespace)
	_, err := client.Get(c.PrometheusRule.Name, metav1.GetOptions{})
	return ConvertError(err)
}

func updateTypeMetaPrometheusRule(r *monitoringv1.PrometheusRule) {
	r.Kind = KindPrometheusRule
	if r.APIVersion == "" {
		r.APIVersion = monitoringv1.SchemeGroupVersion.String()
	}
}
