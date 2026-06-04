# Kubernetes

The ArcadeDB Helm chart has moved to its own repository:

**https://github.com/ArcadeData/arcadedb-helm**

## Install

```bash
helm repo add arcadedb https://helm.arcadedb.com/
helm repo update
helm install my-arcadedb arcadedb/arcadedb
```

## Health probes

ArcadeDB exposes two HTTP probe endpoints on the API port (default `2480`):

- `GET /api/v1/health` - liveness. Returns `204` when the process and HTTP layer are up.
  Performs no database I/O and requires no authentication. Use this for `livenessProbe` so
  Kubernetes never restarts a node that is merely warming up.
- `GET /api/v1/ready` - readiness. Returns `204` when the server is `ONLINE`. When
  `arcadedb.server.readinessRequiresHA=true` and HA is active, it returns `503` until the node
  has joined the Raft group and caught up. Default (`false`) preserves single-node readiness
  behavior.

```yaml
livenessProbe:
  httpGet:
    path: /api/v1/health
    port: 2480
  initialDelaySeconds: 10
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /api/v1/ready
    port: 2480
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 3
```

To make readiness HA-aware in a clustered deployment, set the environment variable
`ARCADEDB_SERVER_READINESSREQUIRESHA=true` (or pass `-Darcadedb.server.readinessRequiresHA=true`).
