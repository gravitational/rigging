package rigging

import (
	"fmt"
	"time"

	"k8s.io/client-go/1.4/pkg/api/v1"
)

const (
	ChangesetResourceName     = "changeset.changeset.gravitational.io"
	ChangesetGroup            = "changeset.gravitational.io"
	ChangesetVersion          = "v1"
	ChangesetAPIVersion       = "changeset.gravitational.io/v1"
	ChangesetCollection       = "changesets"
	DefaultNamespace          = "default"
	KindDaemonSet             = "DaemonSet"
	KindChangeset             = "Changeset"
	KindConfigMap             = "ConfigMap"
	KindDeployment            = "Deployment"
	KindReplicaSet            = "ReplicaSet"
	KindReplicationController = "ReplicationController"
	KindService               = "Service"
	KindSecret                = "Secret"
	KindJob                   = "Job"
	AnnotationCreatedBy       = "kubernetes.io/created-by"
	OpStatusCreated           = "created"
	OpStatusCompleted         = "completed"
	OpStatusReverted          = "reverted"
	ChangesetStatusReverted   = "reverted"
	ChangesetStatusInProgress = "in-progress"
	ChangesetStatusCommitted  = "committed"
	// DefaultRetryAttempts specifies amount of retry attempts for checks
	DefaultRetryAttempts = 60
	// RetryPeriod is a period between Retries
	DefaultRetryPeriod = time.Second
	DefaultBufferSize  = 1024
)

// NamespaceOrDefault returns a default namespace if the specified namespace is empty
func Namespace(namespace string) string {
	if namespace == "" {
		return DefaultNamespace
	}
	return namespace
}

// formatMeta formats this meta as text
func formatMeta(meta v1.ObjectMeta) string {
	return fmt.Sprintf("%v/%v", Namespace(meta.Namespace), meta.Name)
}
