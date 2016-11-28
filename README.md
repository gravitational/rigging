# Rig

Rig tool uses third-party resources to add changeset semantics to Kubernetes operations:

```sh
# upsert a daemon set (changeset is created automatically)
rig upsert change1 -f daemonset.yaml
# delete a service
rig delete change1 svc/service1
# update config map
rig delete change1 configmaps/cf1
# check status
rig status change1
# revert everything
rig revert change1
# or freeze changeset, so it can no longer be updated
rig freeze change1
```

## Changeset management

You can view your changesets to see what happened:

**View all changesets**

```sh
rig get
```

**Get detailed operations log for a changeset**

```sh
rig get change1
rig get change1 -o yaml
```

**Delete a changeset**

```sh
rig cs delete change1
```

## Details

### Supported resources

The following resources are supported:

* ConifgMap
* DaemonSet
* ReplicationController
* Service
* Secret
* Deployment

### Updates

Only `Deployment` updates are rolling, updating of daemon sets or replication controllers simply deletes all the pods

### Status checks

Rig adds additional condition to consider `Deployment`, `DaemonSet` or `ReplicatonController` as ready - all pods must be in `Running` state.



