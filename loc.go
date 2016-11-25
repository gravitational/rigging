package rigging

import (
	"strings"

	"github.com/gravitational/trace"
)

const (
	kindDaemonSet             = "DaemonSet"
	kindTransaction           = "Transaction"
	kindConfigMap             = "ConfigMap"
	kindDeployment            = "Deployment"
	kindReplicaSet            = "ReplicaSet"
	kindReplicationController = "ReplicationController"
	kindService               = "Service"
	kindSecret                = "Secret"
	kindJob                   = "Job"
)

func ParseShortcut(in, defaultVal string) (string, error) {
	if in == "" {
		return defaultVal, nil
	}
	switch strings.ToLower(in) {
	case "configmaps":
		return kindConfigMap, nil
	case "daemonsets", "ds":
		return kindDaemonSet, nil
	case "transactions", "tx":
		return kindTransaction, nil
	case "deployments":
		return kindDeployment, nil
	case "jobs":
		return kindJob, nil
	case "replicasets", "rs":
		return kindReplicaSet, nil
	case "replicationcontrollers", "rc":
		return kindReplicationController, nil
	case "secrets":
		return kindSecret, nil
	case "services", "svc":
		return kindService, nil
	}
	return "", trace.BadParameter("unsupported resource: %v", in)
}

func ParseRef(ref string) (*Ref, error) {
	return nil, trace.Wrap(err)
}

// Ref is a resource refernece
type Ref struct {
	Kind      string
	Name      string
	Namespace string
}

func (l *Locator) Set(v string) error {
	p, err := ParseLocator(v)
	if err != nil {
		return err
	}
	l.Repository = p.Repository
	l.Name = p.Name
	l.Version = p.Version
	return nil
}

func (l Locator) String() string {
	return fmt.Sprintf("%v/%v:%v", l.Repository, l.Name, l.Version)
}
