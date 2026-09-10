# #7389 - gRPC CreateDatabase / DropDatabase never replicate on an HA cluster

## Problem

`ArcadeDbGrpcAdminService.createDatabasePhysical` / `dropDatabasePhysical` create and drop the
database **locally only**. No Raft entry is submitted, so on an HA cluster:

- `CreateDatabase` leaves the database on the leader and nowhere else.
- `DropDatabase` removes it from the leader (through `getEmbedded().drop()`, which unwraps past the
  Raft wrapper) and leaves it on every follower.

The leader-only gate (`requireLeader`, `ArcadeDbGrpcAdminService:157` and `:188`) guarantees the
operation lands on the leader, so the divergence is created on the node the followers treat as
authoritative.

The HTTP `POST /server` commands do the opposite deliberately
(`PostServerCommandHandler:388-402` and `:646-670`), so which transport the caller happens to use
decides whether the cluster stays consistent.

## Root cause

The HA-aware create/drop lived only in the HTTP handler. `ServerControlPlane` - the transport-neutral
home the two protocols already share for `openDatabase` / `closeDatabase` / `alignDatabase` and for
restore/import - never owned create and drop, so gRPC had to reimplement them and reimplemented the
non-HA half.

## Invariant the fix establishes

> A database created or dropped through any server transport is created or dropped on the whole
> cluster: on a replicated database the operation goes through the Raft entry
> (`createInReplicas` / `dropInReplicas`), never behind the cluster's back through
> `getEmbedded().drop()`.

## Completeness

### Commands run

```
$ grep -rn "\.createDatabase(" . --include='*.java' | grep "/src/main/java/"
ha-raft/.../ArcadeStateMachine.java:2986:    server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
grpc-client/.../RemoteGrpcServer.java:401: .createDatabase(CreateDatabaseRequest...)
server/.../ServerControlPlane.java:1202:   final ServerDatabase createdDb = server.createDatabase(...);
server/.../http/handler/PostServerCommandHandler.java:397: final ServerDatabase db = server.createDatabase(...);
grpcw/.../ArcadeDbGrpcAdminService.java:1171: server.createDatabase(name, ComponentFile.MODE.READ_WRITE);

$ grep -rn "getEmbedded().drop()\|removeDatabase(" . --include='*.java' | grep "/src/main/java/"
ha-raft/.../ArcadeStateMachine.java:3603:      server.removeDatabase(databaseName);
ha-raft/.../SnapshotInstaller.java:486, :597:  server.removeDatabase(databaseName);
server/.../ServerControlPlane.java:277 (closeDatabase), :1402-1403 (dropDatabaseForRestore), :1427-1428 (dropQuietly)
server/.../ArcadeDBServer.java:1088 (removeDatabase itself), :1396-1400 (startup 'restore' command)
server/.../http/handler/PostServerCommandHandler.java:667-668
grpcw/.../ArcadeDbGrpcAdminService.java:1179-1180

$ grep -rn "createInReplicas\|dropInReplicas" . --include='*.java'
  -> zero hits under grpcw/src/main (confirms the reporter's grep)
```

### Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| gRPC `CreateDatabase` -> `createDatabasePhysical` | **yes** - now `ServerControlPlane.createDatabase` | yes - `Issue7389GrpcCreateDropDatabaseReplicationIT.createDatabaseOverGrpcReplicatesToEveryPeer` |
| gRPC `DropDatabase` -> `dropDatabasePhysical` | **yes** - now `ServerControlPlane.dropDatabase` | yes - `Issue7389GrpcCreateDropDatabaseReplicationIT.dropDatabaseOverGrpcReplicatesToEveryPeer` |
| HTTP `POST /server` `create database` | yes - was already HA-aware, now delegates to the same method | yes - `Issue7389GrpcCreateDropDatabaseReplicationIT.createDatabaseOverHttpStillReplicates` (parity guard) |
| HTTP `POST /server` `drop database` | yes - was already HA-aware, now delegates to the same method | yes - `Issue7389GrpcCreateDropDatabaseReplicationIT.dropDatabaseOverHttpStillReplicates` (parity guard) |
| `ServerControlPlane.importDatabase` (HTTP `import database` + gRPC `ImportDatabase`) | argued - already calls `createInReplicas` (`:1202-1210`), unchanged | existing coverage |
| `ServerControlPlane.dropDatabaseForRestore` (restore backup / restore database, both transports) | yes - refactored onto the shared HA-aware helper, behaviour unchanged | existing restore ITs |
| `ServerControlPlane.replicateRestoredDatabase` -> `createInReplicas(true)` | argued - already HA-aware, untouched | existing restore ITs |
| `ArcadeStateMachine` install-database apply (`:2986`) | argued - this **is** the apply side of the Raft entry; it must create locally, and it is guarded by `server.existsDatabase` | n/a |
| `ArcadeStateMachine` drop-database apply (`:3603`) | argued - same: the apply side performs the local drop once the entry is committed | n/a |
| `SnapshotInstaller` (`:486`, `:597`) | argued - deregisters the database around the on-disk snapshot swap; it is not a cluster-visible drop | n/a |
| `ArcadeDBServer` startup `restore` command (`:1396-1400`) | argued - runs inside `loadDatabases()` during startup, before the HA plugin is started, so there is no replicated wrapper and no leader to submit to | n/a |
| `ServerControlPlane.closeDatabase` (`:270-278`) / gRPC `CloseDatabase` | argued - closes and deregisters, does not delete files; a peer reopens on next access, so no divergence | n/a |
| `ServerControlPlane.dropQuietly` (`:1425-1431`) | argued - compensating drop of a database whose `createInReplicas` **failed**, i.e. one the cluster never accepted; dropping it through Raft would need an entry the cluster never got | n/a |
| `RemoteGrpcServer.createDatabase` (`grpc-client:401`) | argued - client side; it issues the `CreateDatabase` RPC fixed above | n/a |

No row is blank, so no follow-up issue was filed for this sweep.

### Reachability

`ArcadeDbGrpcAdminService` is constructed by `GrpcServerPlugin` (`:272`) and registered on the server
(`:275`), so the changed methods run whenever the gRPC plugin is enabled. `PostServerCommandHandler`
constructs its own `ServerControlPlane` (`:99`) and is the handler for `POST /server`. Both changed
paths are driven end-to-end by the new IT against a live 2-node Raft cluster.

## Residual risk

- The Raft drop entry is committed synchronously on the leader but **applied** asynchronously on the
  followers, so `DropDatabase` returns before every peer has removed its files. That is the
  pre-existing semantics of the HTTP `drop database` command; this change makes gRPC match it, it
  does not make either synchronous. The new tests poll rather than assert immediately.
- Non-HA behaviour is unchanged on both transports: the local create/drop branch is byte-for-byte
  the previous one.

## Changes

| File | Change |
|---|---|
| `server/.../ServerControlPlane.java` | new `createDatabase(String)` and `dropDatabase(String)` (transport-neutral, HA-aware), plus the private `dropDatabaseClusterWide` they share with `dropDatabaseForRestore` |
| `server/.../http/handler/PostServerCommandHandler.java` | `createDatabase` / `dropDatabase` now delegate to the control plane; the leader check, the `http.*-database` metric and the command grammar stay in the handler |
| `grpcw/.../ArcadeDbGrpcAdminService.java` | `createDatabasePhysical` / `dropDatabasePhysical` now delegate to the same two control-plane methods instead of calling `server.createDatabase` and `getEmbedded().drop()` |
| `grpcw/src/test/.../Issue7389GrpcCreateDropDatabaseReplicationIT.java` | new: 4 tests on a live 2-node Raft cluster |

The gRPC `DropDatabase` RPC now reports a missing database as `INVALID_ARGUMENT` ("does not exist")
instead of the untyped failure `server.getDatabase` raised; it is still guarded by the RPC's own
`containsDatabaseIgnoreCase` check, so the RPC's own success path is unchanged.

## Test results

Ports 2480/2481 were held by other JVMs on this machine for the whole session, so the full `server`
unit-test run reported 7 failures in classes that hardcode `http://127.0.0.1:2480`
(`Issue5675CreateIndexIfNotExistsHttpTest`, `OpenCypherSpecialCharsHttpTest`,
`Issue5023IdempotencyKeyReplayTest`, `PostCommandHandlerLargeContentTest`), each with the
`403` / "Too many failed authentication attempts" signature the CLAUDE.md build notes describe. None
of those four classes contains the string `create database`, `drop database` or `/api/v1/server`
(checked with grep), and every targeted class below was run and is green.

| Suite | Result |
|---|---|
| `Issue7389GrpcCreateDropDatabaseReplicationIT` (new) | 4/4 pass |
| the same IT with the gRPC delegation reverted to the old local-only code | 2/2 gRPC tests **fail** - "created over gRPC on the leader (server 1) must exist on server 0 too" and "dropped over gRPC ... must be gone from server 0 too"; the two HTTP parity tests still pass, which is the split the bug predicts |
| `PostServerCommandHandlerIT`, `PostServerCommandPathTraversalIT`, `GroupManagementIT` | 39/39 pass |
| `HTTPGraphIT` (includes `createAndDropDatabase`), `RestoreImportSecurityDurabilityIT`, `BackupRestoreDeleteApiIT`, `Issue7308RestoreTargetNameIT` | 31/31 pass |
| `GrpcAdminServiceIT`, `Issue5039GrpcAdminAuthorizationIT`, `Issue7304GrpcAdminLeaderRoutingIT`, `GrpcAdminAuthInterceptorIT` + the new IT | 26/26 pass |
| `Issue7035AdminServiceDoubleTerminateGuardTest` | 7/7 pass |

## Finding ledger

- [x] 1. gRPC `CreateDatabase` never submits the Raft install-database entry - fixed, covered by `createDatabaseOverGrpcReplicatesToEveryPeer`
- [x] 2. gRPC `DropDatabase` drops through `getEmbedded()`, bypassing the Raft wrapper - fixed, covered by `dropDatabaseOverGrpcReplicatesToEveryPeer`
- [x] 3. The logic lived only in the HTTP handler, so a third transport would repeat the mistake - fixed by moving it to `ServerControlPlane`, which both transports now call

## Adversarial pass

No `Task` tool was available in this session, so the pass was run by re-reading the tree against the
issue and the diff rather than by a subagent that had not seen the reasoning. Recorded as a weaker
form of the gate, not an equivalent one.

| Finding | Disposition |
|---|---|
| Both RPCs report plain success for the no-op case (create on an existing name, drop on a missing one) while the HTTP commands raise "already exists" / "does not exist". `CreateDatabaseResponse` is empty, so an OK does not tell a provisioning client whether it got a fresh database or someone else's. | **Real, out of scope** - filed as #7413. Not a replication defect, and fixing it means either a breaking status change or a proto field |
| `containsDatabaseIgnoreCase` (case-insensitive) guards a `dropDatabase` whose existence check is exact, so `DropDatabase("MyDb")` against `mydb` now fails inside the control plane with "does not exist" | **Real, pre-existing, out of scope** - the same mismatch existed before, where it surfaced as the untyped failure from `server.getDatabase`. Folded into #7413, which has to settle the case question to settle the contract |
| The `graph` variant creates V and E in a transaction *after* `createInReplicas` returns, so a follower could in principle see the schema entry before it has installed the database | **Not real as a new defect** - the HTTP `create database` followed by any DDL has the same ordering, and Raft applies the install-database entry before any later entry on the same log. The new IT asserts V and E on the node that served the call; it deliberately does not assert them on the follower, because that would be asserting schema-replication timing this change does not touch |
| `ServerControlPlane.importDatabase` still writes its own `createDatabase` + `createInReplicas` pair rather than calling the new method | **Not real** - it needs the compensating `dropQuietly` when `createInReplicas` fails, which the shared method deliberately does not do. It was already HA-aware, so it is not an instance of this bug |

## Follow-up issues

- #7413 - gRPC create/drop report success for the no-op case where HTTP reports an error
