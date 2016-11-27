package rigging

import (
	"time"
)

const (
	txResourceName            = "transaction.tx.gravitational.io"
	txGroup                   = "tx.gravitational.io"
	txVersion                 = "v1"
	txAPIVersion              = "tx.gravitational.io/v1"
	txCollection              = "transactions"
	DefaultNamespace          = "default"
	KindDaemonSet             = "DaemonSet"
	KindTransaction           = "Transaction"
	KindConfigMap             = "ConfigMap"
	KindDeployment            = "Deployment"
	KindReplicaSet            = "ReplicaSet"
	KindReplicationController = "ReplicationController"
	KindService               = "Service"
	KindSecret                = "Secret"
	KindJob                   = "Job"
	annotationCreatedBy       = "kubernetes.io/created-by"
	opStatusCreated           = "created"
	opStatusCompleted         = "completed"
	opStatusRolledBack        = "rolledback"
	txStatusRolledBack        = "rolledback"
	txStatusInProgress        = "in-progress"
	txStatusCommited          = "commited"
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
