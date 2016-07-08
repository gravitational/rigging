package rigging

// types so we can load k8s JSON

type ReplicationController struct {
	Status ReplicationControllerStatus
}

type ReplicationControllerStatus struct {
	Replicas             int
	FullyLabeledReplicas int
	ObservedGeneration   int
}

type PodCondition struct {
	Type   string
	Status string
}

type PodStatus struct {
	Phase      string
	Conditions []PodCondition
}

type Pod struct {
	Status PodStatus
}

type PodList struct {
	Items []Pod
}

type Node struct {
	Metadata Metadata
}

type NodeList struct {
	Items []Node
}

type Metadata struct {
	Name   string
	Labels map[string]string
}
