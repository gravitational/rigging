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
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
)

func WaitForNPods(pods *PodList, desired int, delay time.Duration, tries int) error {
	for i := 0; i < tries; i++ {
		healthy := 0
		for _, pod := range pods.Items {
			for _, condition := range pod.Status.Conditions {
				if condition.Type == "Ready" && condition.Status == "True" {
					healthy++
					break
				}
			}
		}

		log.Infof("looking for %d pods, have %d pods, %d healthy", desired, len(pods.Items), healthy)
		if len(pods.Items) == desired && healthy == desired {
			return nil
		}
		time.Sleep(delay)
	}

	return trace.Errorf("timed out waiting for pods")
}
