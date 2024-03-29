package rigging

import (
	"fmt"
	"strings"

	"github.com/gravitational/trace"
)

func ParseShortcut(in, defaultVal string) (string, error) {
	if in == "" {
		return defaultVal, nil
	}
	switch strings.ToLower(in) {
	case "configmaps":
		return KindConfigMap, nil
	case "daemonsets", "ds":
		return KindDaemonSet, nil
	case "changesets", "cs":
		return KindChangeset, nil
	case "deployments":
		return KindDeployment, nil
	case "jobs":
		return KindJob, nil
	case "replicasets", "rs":
		return KindReplicaSet, nil
	case "replicationcontrollers", "rc":
		return KindReplicationController, nil
	case "secrets":
		return KindSecret, nil
	case "services", "svc":
		return KindService, nil
	case "serviceaccount", "serviceaccounts", "sa":
		return KindServiceAccount, nil
	case "statefulsets":
		return KindStatefulSet, nil
	case "alertmanagers":
		return KindAlertmanager, nil
	case "prometheuses":
		return KindPrometheus, nil
	case "prometheusrules":
		return KindPrometheusRule, nil
	case "servicemonitors":
		return KindServiceMonitor, nil
	case "roles":
		return KindRole, nil
	case "rolebindings":
		return KindRoleBinding, nil
	case "clusterrolebindings":
		return KindClusterRoleBinding, nil
	case "clusterroles":
		return KindClusterRole, nil
	case "psp", "podsecuritypolicies":
		return KindPodSecurityPolicy, nil
	case "crds", "customresourcedefinitions":
		return KindCustomResourceDefinition, nil
	}
	return "", trace.BadParameter("unsupported resource: %v", in)
}

// ParseRef parses resource reference eg daemonsets/ds1
func ParseRef(ref string) (*Ref, error) {
	if ref == "" {
		return nil, trace.BadParameter("missing value")
	}
	parts := strings.FieldsFunc(ref, isDelimiter)
	switch len(parts) {
	case 1:
		return &Ref{Kind: KindChangeset, Name: parts[0]}, nil
	case 2:
		shortcut, err := ParseShortcut(parts[0], KindChangeset)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		return &Ref{Kind: shortcut, Name: parts[1]}, nil
	}
	return nil, trace.BadParameter("failed to parse '%v'", ref)
}

// isDelimiter returns true if rune is space or /
func isDelimiter(r rune) bool {
	switch r {
	case '\t', ' ', '/':
		return true
	}
	return false
}

// Ref is a resource refernece
type Ref struct {
	Kind string
	Name string
}

func (r *Ref) IsEmtpy() bool {
	return r.Name == ""
}

func (r *Ref) Set(v string) error {
	out, err := ParseRef(v)
	if err != nil {
		return err
	}
	*r = *out
	return nil
}

func (r *Ref) String() string {
	return fmt.Sprintf("%v/%v", r.Kind, r.Name)
}
