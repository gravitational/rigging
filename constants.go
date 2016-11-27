package rigging

import (
	"time"
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
	OpStatusRolledBack        = "rolled-back"
	ChangesetStatusRolledBack = "rolled-back"
	ChangesetStatusInProgress = "in-progress"
	ChangesetStatusCommited   = "commited"
	// DefaultRetryAttempts specifies amount of retry attempts for checks
	DefaultRetryAttempts = 60
	// RetryPeriod is a period between Retries
	DefaultRetryPeriod = time.Second
)

// Namespace sets default namespace if in is empty
func Namespace(in string) string {
	if in == "" {
		return DefaultNamespace
	}
	return in
}
