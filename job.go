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
	"encoding/json"
	"time"

	"github.com/gravitational/trace"
)

func WaitForJobSuccess(name string, wait time.Duration) error {
	start := time.Now()

	for {
		var job Job

		cmd := KubeCommand("get", "job", name, "-o", "json")
		out, err := cmd.Output()
		if err != nil {
			return trace.Wrap(err)
		}

		err = json.Unmarshal(out, &job)
		if err != nil {
			return trace.Wrap(err)
		}

		if job.Status.Succeeded > 0 {
			return nil
		}

		time.Sleep(time.Second)

		if time.Since(start) >= wait {
			return trace.Errorf("timed out waiting for job %v to succeed", name)
		}
	}

	return nil
}
