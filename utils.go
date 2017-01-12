package rigging

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/gravitational/trace"

	log "github.com/Sirupsen/logrus"
	"k8s.io/client-go/1.4/kubernetes"
	"k8s.io/client-go/1.4/pkg/api"
	"k8s.io/client-go/1.4/pkg/api/v1"
	"k8s.io/client-go/1.4/pkg/apis/extensions/v1beta1"
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

func collectPods(namespace string, selector *v1beta1.LabelSelector, entry *log.Entry, client *kubernetes.Clientset,
	fn func(api.ObjectReference) bool) (map[string]v1.Pod, error) {
	var set labels.Set
	if selector != nil {
		set = make(labels.Set)
		for key, val := range selector.MatchLabels {
			set[key] = val
		}
	}

	podList, err := client.Core().Pods(namespace).List(api.ListOptions{
		LabelSelector: set.AsSelector(),
	})
	if err != nil {
		return nil, trace.Wrap(err)
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
			entry.Infof("found pod: %v", pod.Name)
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

func status(ctx context.Context, retryAttempts int, retryPeriod time.Duration, fn func() error, entry *log.Entry) error {
	if retryAttempts == 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryPeriod == 0 {
		retryPeriod = DefaultRetryPeriod
	}
	entry.Infof("Checking status retryAttempts=%v, retryPeriod=%v", retryAttempts, retryPeriod)

	return retry(ctx, retryAttempts, retryPeriod, fn)
}

func nodeSelector(spec *v1.PodSpec) labels.Selector {
	set := make(labels.Set)
	for key, val := range spec.NodeSelector {
		set[key] = val
	}
	return set.AsSelector()
}

func checkRunning(pods map[string]v1.Pod, nodes []v1.Node, entry *log.Entry) error {
	for _, node := range nodes {
		pod, ok := pods[node.Name]
		if !ok {
			return trace.NotFound("not found any pod on node %v", node.Name)
		}
		if pod.Status.Phase != v1.PodRunning {
			return trace.CompareFailed("pod %v is not running yet: %v", pod.Name, pod.Status.Phase)
		}
		entry.Infof("node %v: pod %v is up and running", node.Name, pod.Name)
	}
	return nil
}
