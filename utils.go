package rigging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/gravitational/trace"

	log "github.com/Sirupsen/logrus"
	"k8s.io/client-go/1.4/kubernetes"
	"k8s.io/client-go/1.4/pkg/api"
	"k8s.io/client-go/1.4/pkg/api/errors"
	"k8s.io/client-go/1.4/pkg/api/unversioned"
	"k8s.io/client-go/1.4/pkg/api/v1"
	"k8s.io/client-go/1.4/pkg/labels"
)

type action string

const (
	ActionCreate  action = "create"
	ActionDelete  action = "delete"
	ActionReplace action = "replace"
	ActionApply   action = "apply"
)

// KubeCommand returns an exec.Command for kubectl with the supplied arguments.
func KubeCommand(args ...string) *exec.Cmd {
	return exec.Command("/usr/local/bin/kubectl", args...)
}

// FromFile performs action on the Kubernetes resources specified in the path supplied as an argument.
func FromFile(act action, path string) ([]byte, error) {
	cmd := KubeCommand(string(act), "-f", path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}

// FromStdin performs action on the Kubernetes resources specified in the string supplied as an argument.
func FromStdIn(act action, data string) ([]byte, error) {
	cmd := KubeCommand(string(act), "-f", "-")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, trace.Wrap(err)
	}

	var b bytes.Buffer
	cmd.Stdout = &b
	cmd.Stderr = &b

	if err := cmd.Start(); err != nil {
		return b.Bytes(), trace.Wrap(err)
	}

	io.WriteString(stdin, data)
	stdin.Close()

	if err := cmd.Wait(); err != nil {
		log.Errorf("%v", err)
		return b.Bytes(), trace.Wrap(err)
	}

	return b.Bytes(), nil
}

func pollStatus(ctx context.Context, retryAttempts int, retryPeriod time.Duration, fn func() error, entry *log.Entry) error {
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	entry.Infof("Checking status retryAttempts=%v, retryPeriod=%v", retryAttempts, retryPeriod)

	return retry(ctx, retryAttempts, retryPeriod, fn)
}

func collectPods(namespace string, matchLabels map[string]string, entry *log.Entry, client *kubernetes.Clientset,
	fn func(api.ObjectReference) bool) (map[string]v1.Pod, error) {
	set := make(labels.Set)
	for key, val := range matchLabels {
		set[key] = val
	}

	podList, err := client.Core().Pods(namespace).List(api.ListOptions{
		LabelSelector: set.AsSelector(),
	})
	if err != nil {
		return nil, ConvertError(err)
	}

	pods := make(map[string]v1.Pod, 0)
	for _, pod := range podList.Items {
		createdBy, exists := pod.Annotations[AnnotationCreatedBy]
		if !exists {
			continue
		}

		ref, err := ParseSerializedReference(strings.NewReader(createdBy))
		if err != nil {
			log.Warning(trace.DebugReport(err))
			continue
		}

		if fn(ref.Reference) {
			pods[pod.Spec.NodeName] = pod
			entry.Infof("found pod %v on node %v", formatMeta(pod.ObjectMeta), pod.Spec.NodeName)
		}
	}
	return pods, nil
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

func nodeSelector(spec *v1.PodSpec) labels.Selector {
	set := make(labels.Set)
	for key, val := range spec.NodeSelector {
		set[key] = val
	}
	return set.AsSelector()
}

func checkRunning(pods map[string]v1.Pod, nodes []v1.Node, entry *log.Entry) error {
	ready, err := checkRunningAndReady(pods, nodes, entry)
	if ready || err == errPodCompleted {
		return nil
	}
	return trace.Wrap(err)
}

func checkRunningAndReady(pods map[string]v1.Pod, nodes []v1.Node, entry *log.Entry) (bool, error) {
	for _, node := range nodes {
		pod, ok := pods[node.Name]
		if !ok {
			entry.Infof("no pod found on node %v", node.Name)
			return false, trace.NotFound("no pod found on node %v", node.Name)
		}
		meta := formatMeta(pod.ObjectMeta)
		switch pod.Status.Phase {
		case v1.PodFailed, v1.PodSucceeded:
			entry.Infof("node %v: pod %v is %q", node.Name, meta, pod.Status.Phase)
			return false, errPodCompleted
		case v1.PodRunning:
			ready := isPodReadyConditionTrue(pod.Status)
			if ready {
				entry.Infof("node %v: pod %v is up and running", node.Name, meta)
			}
			return ready, nil
		default:
			return false, trace.CompareFailed("pod %v is not running yet, status: %q, ready: false",
				meta, pod.Status.Phase)
		}
	}
	return false, trace.NotFound("no pods %v found on any nodes %v", pods, nodes)
}

// errPodCompleted is returned by checkRunningAndReady to indicate that
// the pod has already reached completed state.
var errPodCompleted = fmt.Errorf("pod ran to completion")

// isPodReady retruns true if a pod is ready; false otherwise.
func isPodReadyConditionTrue(status v1.PodStatus) bool {
	_, condition := getPodCondition(&status, v1.PodReady)
	return condition != nil && condition.Status == v1.ConditionTrue
}

// getPodCondition extracts the provided condition from the given status and returns that.
// Returns nil and -1 if the condition is not present, and the index of the located condition.
func getPodCondition(status *v1.PodStatus, conditionType v1.PodConditionType) (int, *v1.PodCondition) {
	if status == nil {
		return -1, nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return i, &status.Conditions[i]
		}
	}
	return -1, nil
}

func ConvertError(err error) error {
	if err == nil {
		return nil
	}
	se, ok := err.(*errors.StatusError)
	if !ok {
		return err
	}

	format, args := se.DebugError()
	status := se.Status()
	switch {
	case status.Code == http.StatusConflict && status.Reason == unversioned.StatusReasonAlreadyExists:
		return trace.AlreadyExists("error: %v, details: %v", err.Error(), fmt.Sprintf(format, args...))
	case status.Code == http.StatusNotFound:
		return trace.NotFound("error: %v, details: %v", err.Error(), fmt.Sprintf(format, args...))
	case status.Code == http.StatusForbidden:
		return trace.AccessDenied("error: %v, details: %v", err.Error(), fmt.Sprintf(format, args...))
	}
	return err
}
