# ArcadeDB Helm Chart

This Helm chart facilitates the deployment of [ArcadeDB](https://arcadedb.com/), an open-source multi-model database, on a Kubernetes cluster.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+

## Installation

To install the chart with the release name `my-arcadedb`:

```bash
helm install my-arcadedb ./arcadedb
```

The command deploys ArcadeDB on the Kubernetes cluster using the default configuration. The [Parameters](#parameters) section lists the configurable parameters of this chart and their default values.

## Uninstallation

To uninstall/delete the `my-arcadedb` deployment:

```bash
helm uninstall my-arcadedb
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

## Parameters

### arcaddb

| Name                         | Description                                           | Value                                    |
| ---------------------------- | ----------------------------------------------------- | ---------------------------------------- |
| `arcadedb.databaseDirectory` | Enable persistence by updating this and volume/Mounts | `/home/arcadedb/databases`               |
| `arcadedb.defaultDatabases`  | Default databases                                     | `Universe[foo:bar]`                      |
| `arcadedb.extraCommands`     | Any extra comands to pass to ArcadeDB startup         | `["-Darcadedb.server.mode=development"]` |

### arcadedb.credentials


### arcadedb.credentials.rootPassword


### arcadedb.credentials.secret

| Name                                            | Description                   | Value |
| ----------------------------------------------- | ----------------------------- | ----- |
| `arcadedb.credentials.rootPassword.secret.name` | Name of existing secret       | `nil` |
| `arcadedb.credentials.rootPassword.secret.key`  | Key to use in existing secret | `nil` |

### image

| Name               | Description                                                                                                                                                                                      | Value          |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------- |
| `image.registry`   | Registry for image                                                                                                                                                                               | `arcadedata`   |
| `image.repository` | Image repo                                                                                                                                                                                       | `arcadedb`     |
| `image.pullPolicy` | This sets the pull policy for images.                                                                                                                                                            | `IfNotPresent` |
| `image.tag`        | Overrides the image tag whose default is the chart appVersion.                                                                                                                                   | `""`           |
| `imagePullSecrets` | This is for the secrets for pulling an image from a private repository more information can be found here: https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/ | `[]`           |
| `nameOverride`     | This is to override the chart name.                                                                                                                                                              | `""`           |
| `fullnameOverride` |                                                                                                                                                                                                  | `""`           |

### This section builds out the service account more information can be found here: https://kubernetes.io/docs/concepts/security/service-accounts/

| Name                         | Description                                             | Value  |
| ---------------------------- | ------------------------------------------------------- | ------ |
| `serviceAccount.create`      | Specifies whether a service account should be created   | `true` |
| `serviceAccount.automount`   | Automatically mount a ServiceAccount's API credentials? | `true` |
| `serviceAccount.annotations` | Annotations to add to the service account               | `{}`   |
| `serviceAccount.name`        | The name of the service account to use.                 | `""`   |
| `podAnnotations`             | This is for setting Kubernetes Annotations to a Pod.    | `{}`   |
| `podLabels`                  | This is for setting Kubernetes Labels to a Pod.         | `{}`   |
| `podSecurityContext`         |                                                         | `{}`   |
| `securityContext`            |                                                         | `{}`   |

### This is for setting up a service more information can be found here: https://kubernetes.io/docs/concepts/services-networking/service/


### http

| Name                | Description                                                                                                                                                       | Value          |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| `service.http.type` | This sets the service type more information can be found here: https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types | `LoadBalancer` |
| `service.http.port` | This sets the ports more information can be found here: https://kubernetes.io/docs/concepts/services-networking/service/#field-spec-ports                         | `2480`         |

### rpc

| Name               | Description                                                                                                                               | Value  |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| `service.rpc.port` | This sets the ports more information can be found here: https://kubernetes.io/docs/concepts/services-networking/service/#field-spec-ports | `2424` |

### ingress This block is for setting up the ingress for more information can be found here: https://kubernetes.io/docs/concepts/services-networking/ingress/

| Name                  | Description | Value   |
| --------------------- | ----------- | ------- |
| `ingress.enabled`     |             | `false` |
| `ingress.className`   |             | `""`    |
| `ingress.annotations` |             | `{}`    |

### ingress.hosts

| Name                    | Description | Value                 |
| ----------------------- | ----------- | --------------------- |
| `ingress.hosts[0].host` |             | `chart-example.local` |

### ingress.hosts[0].paths

| Name                                 | Description | Value                    |
| ------------------------------------ | ----------- | ------------------------ |
| `ingress.hosts[0].paths[0].path`     |             | `/`                      |
| `ingress.hosts[0].paths[0].pathType` |             | `ImplementationSpecific` |
| `ingress.tls`                        |             | `[]`                     |
| `resources`                          |             | `{}`                     |

### This is to setup the liveness and readiness probes more information can be found here: https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/


### livenessProbe.httpGet

| Name                         | Description | Value           |
| ---------------------------- | ----------- | --------------- |
| `livenessProbe.httpGet.path` |             | `/api/v1/ready` |
| `livenessProbe.httpGet.port` |             | `http`          |

### This is to setup the liveness and readiness probes more information can be found here: https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/


### readinessProbe.httpGet

| Name                          | Description | Value           |
| ----------------------------- | ----------- | --------------- |
| `readinessProbe.httpGet.path` |             | `/api/v1/ready` |
| `readinessProbe.httpGet.port` |             | `http`          |

### This section is for setting up autoscaling more information can be found here: https://kubernetes.io/docs/concepts/workloads/autoscaling/

| Name                                         | Description                                                  | Value   |
| -------------------------------------------- | ------------------------------------------------------------ | ------- |
| `autoscaling.enabled`                        |                                                              | `false` |
| `autoscaling.minReplicas`                    |                                                              | `1`     |
| `autoscaling.maxReplicas`                    |                                                              | `100`   |
| `autoscaling.targetCPUUtilizationPercentage` |                                                              | `80`    |
| `volumes`                                    | Additional volumes on the output Deployment definition.      | `[]`    |
| `volumeMounts`                               | Additional volumeMounts on the output Deployment definition. | `[]`    |
| `nodeSelector`                               |                                                              | `{}`    |
| `tolerations`                                |                                                              | `[]`    |

### affinity


### Set the anti-affinity selector scope to arcadedb servers.


### preferredDuringSchedulingIgnoredDuringExecution

| Name                                                                                 | Description                                       | Value |
| ------------------------------------------------------------------------------------ | ------------------------------------------------- | ----- |
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

The ArcadeDB image stores data at the `/opt/arcadedb/databases` path of the container. By default, a PersistentVolumeClaim is created and mounted to this directory. If you want to disable persistence, set `persistence.enabled` to `false`. Data will then be stored in an emptyDir, which is erased when the Pod is terminated.

## Ingress

This chart provides support for Ingress resource. To enable Ingress, set `ingress.enabled` to `true` and configure the `ingress.hosts` parameter. For example:

```yaml
ingress:
  enabled: true
  hosts:
    - host: arcadedb.local
      paths: []
```

## Resources

The resource requests and limits can be set by specifying the `resources` parameter. The default values are:

```yaml
resources:
  requests:
    cpu: 100m
    memory: 256Mi
  limits:
    cpu: 500m
    memory: 512Mi
```

## Notes

After installing the chart, you can access ArcadeDB by running the following command:

```bash
kubectl get --namespace default service arcadedb-http
```

Replace `arcadedb-http` with your release name if it's different. The command retrieves the service details, including the IP address and port, which you can use to connect to ArcadeDB. 
