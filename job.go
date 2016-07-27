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
