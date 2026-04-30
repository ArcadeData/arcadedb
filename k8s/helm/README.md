# ArcadeDB Helm Chart

This Helm chart facilitates the deployment of [ArcadeDB](https://arcadedb.com/), an open-source multi-model database, on a
Kubernetes cluster.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+

## Installation

To install the chart with the release name `my-arcadedb`:

```bash
helm install my-arcadedb ./arcadedb
```

The command deploys ArcadeDB on the Kubernetes cluster using the default configuration. The [Parameters](#parameters) section lists
the configurable parameters of this chart and their default values.

## Uninstallation

To uninstall/delete the `my-arcadedb` deployment:

```bash
helm uninstall my-arcadedb
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

## Parameters

### arcadedb

| Name                         | Description                                                        | Value                                    |
|------------------------------|--------------------------------------------------------------------|------------------------------------------|
| `arcadedb.databaseDirectory` | Database storage directory inside the container                    | `/home/arcadedb/databases`              |
| `arcadedb.defaultDatabases`  | Databases to create at startup. Empty = none.                      | `""`                                    |
| `arcadedb.extraCommands`     | Extra JVM -D arguments appended to the startup command             | `["-Darcadedb.server.mode=production"]` |
| `arcadedb.extraEnvironment`  | Additional environment variables to pass to the ArcadeDB container | `[]`                                    |

### arcadedb.credentials

### arcadedb.credentials.rootPassword

### arcadedb.credentials.secret

| Name                                            | Description                   | Value |
|-------------------------------------------------|-------------------------------|-------|
| `arcadedb.credentials.rootPassword.secret.name` | Name of existing secret       | `nil` |
| `arcadedb.credentials.rootPassword.secret.key`  | Key to use in existing secret | `nil` |

### image

| Name               | Description                                                                                                                                                                                      | Value          |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| `image.registry`   | Registry for image                                                                                                                                                                               | `arcadedata`   |
| `image.repository` | Image repo                                                                                                                                                                                       | `arcadedb`     |
| `image.pullPolicy` | This sets the pull policy for images.                                                                                                                                                            | `IfNotPresent` |
| `image.tag`        | Overrides the image tag whose default is the chart appVersion.                                                                                                                                   | `""`           |
| `imagePullSecrets` | This is for the secrets for pulling an image from a private repository more information can be found here: https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/ | `[]`           |
| `nameOverride`     | This is to override the chart name.                                                                                                                                                              | `""`           |
| `fullnameOverride` |                                                                                                                                                                                                  | `""`           |

### This section builds out the service account more information can be found here: https://kubernetes.io/docs/concepts/security/service-accounts/

| Name                         | Description                                             | Value  |
|------------------------------|---------------------------------------------------------|--------|
| `serviceAccount.create`      | Specifies whether a service account should be created                                          | `true`  |
| `serviceAccount.automount`   | Mount the ServiceAccount token into pods. ArcadeDB does not call the Kubernetes API - keep false. | `false` |
| `serviceAccount.annotations` | Annotations to add to the service account                                                      | `{}`    |
| `serviceAccount.name`        | The name of the service account to use.                                                        | `""`    |
| `podAnnotations`             | Annotations added to every pod.                                                                | `{}`    |
| `podLabels`                  | Labels added to every pod.                                                                     | `{}`    |
| `podSecurityContext`         | Pod-level security context. UID/GID 1000 matches the arcadedb user in the Docker image.        | `{runAsNonRoot: true, fsGroup: 1000}` |
| `securityContext`            | Container-level security context.                                                              | `{runAsUser: 1000, runAsGroup: 1000, allowPrivilegeEscalation: false, capabilities.drop: [ALL]}` |

### This is for setting up a service more information can be found here: https://kubernetes.io/docs/concepts/services-networking/service/

### http

| Name                | Description                                                                                                                                                       | Value          |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| `service.http.type` | Service type. Use LoadBalancer or configure ingress for external access.                                                                                         | `ClusterIP`    |
| `service.http.port` | This sets the ports more information can be found here: https://kubernetes.io/docs/concepts/services-networking/service/#field-spec-ports                         | `2480`         |

### rpc

| Name               | Description                                                                                                                               | Value  |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------|--------|
| `service.rpc.port` | Raft gRPC port (ha-raft subsystem).                                                                                                       | `2434` |

### ingress This block is for setting up the ingress for more information can be found here: https://kubernetes.io/docs/concepts/services-networking/ingress/

| Name                  | Description | Value   |
|-----------------------|-------------|---------|
| `ingress.enabled`     |             | `false` |
| `ingress.className`   |             | `""`    |
| `ingress.annotations` |             | `{}`    |

### ingress.hosts

| Name                    | Description | Value                 |
|-------------------------|-------------|-----------------------|
| `ingress.hosts[0].host` |             | `chart-example.local` |

### ingress.hosts[0].paths

| Name                                 | Description | Value                    |
|--------------------------------------|-------------|--------------------------|
| `ingress.hosts[0].paths[0].path`     |             | `/`                      |
| `ingress.hosts[0].paths[0].pathType` |             | `ImplementationSpecific` |
| `ingress.tls`                        |             | `[]`                     |
| `resources`                          |             | `{}`                     |

### This is to setup the liveness and readiness probes more information can be found here: https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/

### livenessProbe.httpGet

| Name                         | Description | Value           |
|------------------------------|-------------|-----------------|
| `livenessProbe.httpGet.path` |             | `/api/v1/ready` |
| `livenessProbe.httpGet.port` |             | `http`          |

### This is to setup the liveness and readiness probes more information can be found here: https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/

### readinessProbe.httpGet

| Name                          | Description | Value           |
|-------------------------------|-------------|-----------------|
| `readinessProbe.httpGet.path` |             | `/api/v1/ready` |
| `readinessProbe.httpGet.port` |             | `http`          |

### This section is for setting up autoscaling more information can be found here: https://kubernetes.io/docs/concepts/workloads/autoscaling/

| Name                                         | Description                                                           | Value   |
|----------------------------------------------|-----------------------------------------------------------------------|---------|
| `autoscaling.enabled`                        | Enable HorizontalPodAutoscaler. When enabled, server list is pre-sized to maxReplicas for KubernetesAutoJoin. | `false` |
| `autoscaling.minReplicas`                    | Minimum replicas. Must satisfy Raft quorum when HA is active: >= floor(maxReplicas/2)+1.                      | `1`     |
| `autoscaling.maxReplicas`                    | Maximum replicas. Chart enforces quorum guard at render time.                                                 | `5`     |
| `autoscaling.targetCPUUtilizationPercentage` |                                                                                                               | `80`    |
| `volumes`                                    | Additional volumes on the output StatefulSet definition.              | `[]`    |
| `volumeMounts`                               | Additional volumeMounts on the output StatefulSet definition.         | `[]`    |
| `volumeClaimTemplates`                       | Extra StatefulSet volumeClaimTemplates (the database PVC is controlled by `persistence.enabled`). | `[]` |

### persistence

| Name                       | Description                                                                                         | Value           |
|----------------------------|-----------------------------------------------------------------------------------------------------|-----------------|
| `persistence.enabled`      | Persist the database directory with a PVC. Set false only for ephemeral/dev deployments.            | `true`          |
| `persistence.size`         | PVC size.                                                                                           | `8Gi`           |
| `persistence.accessMode`   | PVC access mode.                                                                                    | `ReadWriteOnce` |
| `persistence.storageClass` | StorageClass name. Empty string uses the cluster default.                                           | `""`            |

### networkPolicy

| Name                     | Description                                                                                                                          | Value   |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------|---------|
| `networkPolicy.enabled`  | Create NetworkPolicy resources. HTTP (2480) open to all cluster pods; Raft gRPC (2434) restricted to ArcadeDB pods only.             | `false` |
| `nodeSelector`                               |                                                                       | `{}`    |
| `tolerations`                                |                                                                       | `[]`    |

### affinity

### Set the anti-affinity selector scope to arcadedb servers.

### preferredDuringSchedulingIgnoredDuringExecution

| Name                                                                                 | Description                                       | Value |
|--------------------------------------------------------------------------------------|---------------------------------------------------|-------|
| `affinity.podAntiAffinity.preferredDuringSchedulingIgnoredDuringExecution[0].weight` |                                                   | `100` |
| `extraManifests`                                                                     | - Include any amount of extra arbitrary manifests | `{}`  |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example:

```bash
helm install my-arcadedb ./arcadedb --set image.tag=21.11.1
```

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart. For example:

```bash
helm install my-arcadedb ./arcadedb -f values.yaml
```

## Persistence

The ArcadeDB image stores data at `/home/arcadedb/databases` inside the container. By default (`persistence.enabled: true`), a
PersistentVolumeClaim named `arcadedb-data` is created and mounted at that path. Set `persistence.enabled` to `false` only for
ephemeral or development deployments - data will be written to the container's writable layer and lost when the Pod is deleted.

## Ingress

This chart provides support for Ingress resource. To enable Ingress, set `ingress.enabled` to `true` and configure the
`ingress.hosts` parameter. For example:

```yaml
ingress:
  enabled: true
  hosts:
    - host: arcadedb.local
      paths: [ ]
```

## Resources

Resource requests and limits are unset by default for dev/Minikube compatibility. For production, set them via:

```yaml
resources:
  requests:
    cpu: 500m
    memory: 2Gi    # matches -Xms2G set by ARCADEDB_OPTS_MEMORY in the Docker image
  limits:
    memory: 4Gi    # no CPU limit - avoids throttling JVM GC pauses
```

## Notes

After installing the chart, you can access ArcadeDB by running the following command:

```bash
kubectl get --namespace default service arcadedb-http
```

Replace `arcadedb-http` with your release name if it's different. The command retrieves the service details, including the IP
address and port, which you can use to connect to ArcadeDB.
