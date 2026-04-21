# HA Raft: Server command propagation (drop / restore / import / users)

**Date:** 2026-04-10
**Branch:** `ha-redesign`
**Status:** Design, pending implementation plan
**Related:**
- `docs/plans/2026-02-15-ha-raft-redesign.md`
- `docs/plans/2026-02-15-ha-raft-impl.md`
- `docs/plans/2026-02-24-ha-raft-failure-test-scenarios-design.md`
- `docs/plans/2026-03-25-e2e-ha-raft-testing-design.md`

## 1. Problem

The Ratis-based HA implementation on this branch replicates transactions (`TX_ENTRY`), schema changes (`SCHEMA_ENTRY`), and database creation (`INSTALL_DATABASE_ENTRY`). Four destructive or state-altering server commands are NOT propagated to replicas today:

1. **`drop database`** (`server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java:435`) runs only on the leader. `RaftReplicatedDatabase.drop()` explicitly throws `UnsupportedOperationException`. After a successful drop, the leader's database is gone but every replica still holds the full directory on disk. A future failover resurrects the supposedly-dropped database.
2. **`restore database <name> <url>`** (`:232`) is a leader-only operation. Replicas never see the restored files.
3. **`import database <name> <url>`** (`:308`) calls `server.createDatabase(...)` directly at line 325, bypassing the `createInReplicas()` flow that `create database` uses. The leader ends up with a populated database; replicas get nothing.
4. **`create user` / `drop user`** (`:467`, `:486`) mutate the leader's `server-users.jsonl` only. Replicas keep a stale user set, so surviving a leader crash means logins accepted on the old leader are rejected on the new one (or vice versa).

None of these paths have integration or container tests that assert replica-side state.

## 2. Goals and scope

### In scope

- Add Raft-log propagation for `drop database` with **Raft-first** semantics: the leader does not drop its own files until the Raft entry is committed and the state machine applies the drop on every peer (including itself).
- Propagate `restore database` by reusing `INSTALL_DATABASE_ENTRY` with an added `forceSnapshot` flag. Replicas pull the populated files from the leader via the existing `/api/v1/ha/snapshot/{db}` endpoint.
- Propagate `import database` by plugging the HA create path into the importer flow. Once the database exists cluster-wide, the importer's own transactions replicate via the normal `TX_ENTRY` stream.
- Propagate `create user` and `drop user` via a new `SECURITY_USERS_ENTRY` that carries the full updated `server-users.jsonl` content. Apply is **Raft-first**: the leader computes the new user set in memory, submits the entry, and writes to disk only once the state machine applies the entry on every peer.
- Seed freshly-joined replicas with the current user set via a one-shot `SECURITY_USERS_ENTRY` triggered from the peer-add handler.
- Add unit tests for the new codec and state-machine paths, integration tests in `ha-raft` using `BaseRaftHATest`, and container-based scenarios in `e2e-ha` using `ContainersTestTemplate`.

### Out of scope (follow-up specs)

- **Symmetric refactor of `INSTALL_DATABASE_ENTRY` to Raft-first.** `create database` currently does local-first (leader creates, then logs). This spec introduces the Raft-first pattern for drop and users, but leaves create as-is. A follow-up can unify them.
- **Rolling upgrades** across differing `RaftLogEntryCodec` versions.
- **Advanced failure-scenario tests** during drop/restore/import/user ops (partitions during commit, leader crash mid-apply). Those belong to the broader failure-scenarios suite (`2026-02-24-ha-raft-failure-test-scenarios-design.md`). This spec adds smoke-level e2e coverage only.
- `close database`, `open database`, `align database`, backup configuration, server settings, set database setting. These are treated as per-node operations.
- `updateUser` / `setUserPassword` routed through HTTP endpoints other than `PostServerCommandHandler` (if any). We cover `create user` and `drop user` since those are the ones exposed by the server command handler today. `ServerSecurity.updateUser` is reachable only internally and will use the same replication hook when called from new code paths; exposing HTTP wiring for update is out of scope.

## 3. Architecture overview

### 3.1 New and extended Raft log entries

Add two new `RaftLogEntryType` constants:

- `DROP_DATABASE_ENTRY((byte) 4)` - binary layout: `type byte | UTF(databaseName)`. Mirrors `INSTALL_DATABASE_ENTRY`.
- `SECURITY_USERS_ENTRY((byte) 5)` - binary layout: `type byte | UTF("") | int(jsonLength) | UTF8 bytes(usersJson)`. The empty database-name slot keeps the decoder symmetric with the other entry types (they all read a database name first); the payload is the full JSON-lines content of `server-users.jsonl`, encoded as a UTF-8 byte array.

Extend `INSTALL_DATABASE_ENTRY`:

- New layout: `type byte | UTF(databaseName) | boolean forceSnapshot`.
- Legacy decoding (where the trailing flag is absent) defaults to `forceSnapshot=false`. The existing `encodeInstallDatabaseEntry(String)` method keeps its signature and writes `forceSnapshot=false`; a new overload `encodeInstallDatabaseEntry(String, boolean)` writes the flag.

See section 6.5 for the wire-compatibility tradeoff.

### 3.2 New interface methods

**On `com.arcadedb.server.HAReplicatedDatabase`** (per-database operations):

- `void dropInReplicas()` - encodes a `DROP_DATABASE_ENTRY` and submits via `RaftGroupCommitter.submitAndWait`.
- `void createInReplicas(boolean forceSnapshot)` - overload of the existing method. The no-arg version delegates with `false`.

**On `com.arcadedb.server.HAServerPlugin`** (server-wide operations):

- `void replicateSecurityUsers(String newUsersFileContent)` - encodes a `SECURITY_USERS_ENTRY` with the given payload and submits via the group committer. Default implementation is a no-op for non-HA setups (single-node servers simply write the file locally as today).

This is a server-level hook because users are server-wide, not per-database. It reuses the same single Ratis group that `RaftReplicatedDatabase` uses for database entries; `RaftHAServer.getGroupCommitter()` is already a singleton on the plugin.

### 3.3 State machine handling

`ArcadeStateMachine` gains three new code paths:

- **`applyDropDatabaseEntry(DecodedEntry)`**: if `server.existsDatabase(name)` is false, no-op (idempotent on replay). Otherwise resolve the `DatabaseInternal`, close and drop its files via `db.getEmbedded().drop()`, then `server.removeDatabase(name)`.
- **`applyInstallDatabaseEntry` with `forceSnapshot=true`**: do NOT early-return on "database already present". If present, close it; invoke the existing `installDatabaseSnapshot(name, leaderHttpAddr, clusterToken)` helper to replace the files on disk. The leader's own apply is a no-op because its files are already the authoritative copy.
- **`applySecurityUsersEntry(DecodedEntry)`**: atomically replace the content of `<server-root>/config/server-users.jsonl` with the payload, then call `server.getSecurity().reloadUsers()` so the in-memory `users` map matches disk. The write is atomic via write-to-temp + rename, which `SecurityUserFileRepository` already supports internally (we will expose a `saveContent(String)` helper if not already available).

### 3.4 ServerSecurity reload hook

`ServerSecurity` needs a public `reloadUsers()` (or equivalent) that re-reads `server-users.jsonl` and rebuilds the in-memory `users` map. If such a hook does not exist, add it. Implementation is straightforward: iterate the file, `new ServerSecurityUser(...)` for each JSON object, replace the `users` map atomically. No password re-hashing is needed; the file already contains encoded passwords.

### 3.5 HTTP handler changes

- **`PostServerCommandHandler.dropDatabase`** (`:435`) in HA mode: resolve the `HAReplicatedDatabase` wrapper and call `dropInReplicas()`. No local drop - the leader's own state-machine apply handles it. Non-HA mode keeps current behaviour.
- **`PostServerCommandHandler.restoreDatabase`** (`:232`): after the local `Restore.restoreDatabase()` call succeeds, open the restored database, wrap it, and call `createInReplicas(forceSnapshot=true)`. On any failure during `createInReplicas`, the leader compensates by dropping the just-restored local database so the operator can retry.
- **`PostServerCommandHandler.importDatabase`** (`:308`): replace the bare `server.createDatabase(...)` at line 325 with the HA-aware path used by `createDatabase()` (`:212`): create the database, call `createInReplicas()`, then run the importer.
- **`PostServerCommandHandler.createUser`** (`:467`): in HA mode, do NOT mutate `ServerSecurity` directly. Compute the new users file content in memory: current file bytes with the new user (hashed password) appended. Call `ha.replicateSecurityUsers(newContent)`. When the state-machine apply runs locally, it overwrites the file and reloads the in-memory map. Non-HA mode keeps current behaviour.
- **`PostServerCommandHandler.dropUser`** (`:486`): same pattern. Compute the new users file content with the target user removed. Call `ha.replicateSecurityUsers(newContent)`. If the target user does not exist, throw `IllegalArgumentException` before submitting anything.

### 3.6 Seed-on-peer-add

In the current implementation, a fresh replica joining an existing cluster receives database files via `notifyInstallSnapshotFromLeader` (the HTTP snapshot download), but that path does not include `server-users.jsonl` (which lives under `<server-root>/config/`, not under the database directory). Without a seed, a new peer would retain only the root user from its local bootstrap config until the next user mutation happens cluster-wide.

Fix: in `PostAddPeerHandler`, after Ratis reports the peer has been added successfully, the leader reads its current `server-users.jsonl` content and calls `ha.replicateSecurityUsers(currentContent)`. This produces one `SECURITY_USERS_ENTRY` that every peer (including the new one) will apply as part of catch-up, guaranteeing a consistent initial user set.

## 4. Data flow

### 4.1 DROP DATABASE (Raft-first, end-to-end)

```
Client -> POST /api/v1/server  { command: "drop database foo" }
    |
    v
PostServerCommandHandler.execute()
    |
    +-- not leader? forwardToLeaderIfReplica() [unchanged]
    |
    v (leader)
dropDatabase("foo")
    |
    +-- resolve HAReplicatedDatabase wrapper for "foo"
    +-- haDb.dropInReplicas()
            |
            v
RaftReplicatedDatabase.dropInReplicas()
    |
    +-- encode DROP_DATABASE_ENTRY(name)
    +-- RaftGroupCommitter.submitAndWait(bytes, quorumTimeout)
            |
            +-- Ratis replicates entry to quorum
            +-- applyTransaction() runs on every peer (including leader)
                    |
                    v
ArcadeStateMachine.applyDropDatabaseEntry()
    |
    +-- if !server.existsDatabase(name) -> no-op (idempotent)
    +-- database.getEmbedded().drop()   // closes + deletes files
    +-- server.removeDatabase(name)
```

Invariant: until the Raft commit returns, no node has dropped anything. Leader loss, quorum timeout, or submission error result in `submitAndWait` throwing, the HTTP handler returning a 5xx, and the database remaining intact on every peer.

### 4.2 RESTORE DATABASE

```
(leader) restoreDatabase(name, url)
    |
    +-- checkServerIsLeaderIfInHA()
    +-- Restore.restoreDatabase() writes files under databaseDirectory/name
    +-- server.getDatabase(name)        // opens and wraps in HAReplicatedDatabase
    +-- haDb.createInReplicas(forceSnapshot=true)
            |
            v
    INSTALL_DATABASE_ENTRY(name, forceSnapshot=true) -> Raft log
            |
            v (on every replica)
    applyInstallDatabaseEntry()
        if forceSnapshot:
            if server.existsDatabase(name): close it
            installDatabaseSnapshot(name, leaderHttpAddr, clusterToken)
        else:
            // existing behaviour
            if !exists: server.createDatabase(name, READ_WRITE)
```

### 4.3 IMPORT DATABASE

```
(leader) importDatabase(name, url)
    |
    +-- checkServerIsLeaderIfInHA()
    +-- server.createDatabase(name, READ_WRITE)          // creates wrapper
    +-- ((HAReplicatedDatabase) wrapped).createInReplicas()   // NEW
            |
            v
    INSTALL_DATABASE_ENTRY(name, forceSnapshot=false) -> Raft log
            -> replicas each call server.createDatabase(name)
    |
    +-- Importer.load(database)
            -> every transaction commits through RaftReplicatedDatabase
            -> TX_ENTRY stream replicates writes to peers (existing path)
```

### 4.4 CREATE / DROP USER

```
Client -> POST /api/v1/server  { command: "create user { ... }" }
    |
    v
PostServerCommandHandler.execute()
    |
    +-- not leader? forwardToLeaderIfReplica() [unchanged]
    |
    v (leader, HA mode)
createUser(payload)
    |
    +-- validate + encode password
    +-- read current server-users.jsonl bytes
    +-- compute new content with the added user
    +-- ha.replicateSecurityUsers(newContent)
            |
            v
RaftHAPlugin.replicateSecurityUsers(content)
    |
    +-- encode SECURITY_USERS_ENTRY(content)
    +-- RaftGroupCommitter.submitAndWait(bytes, quorumTimeout)
            |
            +-- Ratis replicates entry to quorum
            +-- applyTransaction() runs on every peer (including leader)
                    |
                    v
ArcadeStateMachine.applySecurityUsersEntry()
    |
    +-- atomically write content to <server-root>/config/server-users.jsonl
    +-- server.getSecurity().reloadUsers()
```

Drop user follows the same flow with a computed-delete content. Until the Raft commit returns, no node has mutated its user set on disk.

### 4.5 Ordering rules

1. **Drop vs in-flight writes**: the drop entry is serialised in the Raft log like any other entry. Transactions committed before the drop are applied first on every replica; transactions submitted after the drop are rejected by the state machine (the database no longer exists). Ratis applies log entries sequentially per group, so out-of-order apply is impossible.
2. **Drop vs in-flight reads**: state machine apply is single-threaded. When it closes the underlying `LocalDatabase`, any thread currently reading through the same instance fails. This matches non-HA drop semantics.
3. **Restore snapshot race**: if the leader's restore completes and submits the entry while a replica is lagging and still has queued `TX_ENTRY` items for the old database, entries drain in order. Old TX entries run against the old files, then the restore replaces the files. Stale page writes are absorbed by the existing "page version >= log version" tolerance in `applyTxEntry` (`ignoreErrors=true`).
4. **Concurrent user mutations**: two `create user` requests arriving simultaneously at the leader produce two independent `SECURITY_USERS_ENTRY` entries. Raft serialises them. The second entry's content is computed AFTER the first one has been applied locally on the leader (the handler re-reads the file before computing the payload). If two handlers compute the content in parallel off the same base state, the second Raft apply overwrites the file with a content that is missing the first user; this is a lost update.
   Mitigation: serialise user mutations on the leader via a `synchronized` block on `ServerSecurity` around the read-compute-submit sequence. This is cheap because user mutations are rare, and it makes the read-modify-write atomic with respect to the Raft log.
5. **User ops vs peer-add seed**: the peer-add seed is just another `SECURITY_USERS_ENTRY`. If a concurrent user mutation is in flight when a peer joins, Raft serialises them in the order they were submitted. Both are idempotent overwrites, so the final state is whichever was applied last. This is acceptable because any immediately-following user mutation will produce another entry and converge.

## 5. Components and changes

### 5.1 New files

None. All changes land in existing files.

### 5.2 Modified files

| File | Change |
|---|---|
| `ha-raft/.../RaftLogEntryType.java` | Add `DROP_DATABASE_ENTRY((byte) 4)` and `SECURITY_USERS_ENTRY((byte) 5)` enum constants. |
| `ha-raft/.../RaftLogEntryCodec.java` | Add `encodeDropDatabaseEntry(String)`, `encodeSecurityUsersEntry(String content)`, matching decode branches. Extend `encodeInstallDatabaseEntry` with an overload that takes a `boolean forceSnapshot`; extend decode to read the trailing flag when present. Extend `DecodedEntry` with a nullable `usersJson` field (or reuse `schemaJson` if semantically compatible; a distinct field is cleaner). |
| `ha-raft/.../ArcadeStateMachine.java` | Add `applyDropDatabaseEntry`, `applySecurityUsersEntry`, extend `applyInstallDatabaseEntry` to honour `forceSnapshot=true`, wire the new cases into the `switch` in `applyTransaction`. |
| `ha-raft/.../RaftReplicatedDatabase.java` | Implement `dropInReplicas()`. Add `createInReplicas(boolean forceSnapshot)` overload. The no-arg method delegates with `false`. |
| `ha-raft/.../RaftHAPlugin.java` | Implement `replicateSecurityUsers(String content)` by encoding a `SECURITY_USERS_ENTRY` and calling `getGroupCommitter().submitAndWait`. |
| `ha-raft/.../PostAddPeerHandler.java` | After Ratis confirms the peer was added, read the current `server-users.jsonl` content and call `replicateSecurityUsers(content)` once. |
| `server/.../HAReplicatedDatabase.java` | Add `void dropInReplicas()` and default `void createInReplicas(boolean forceSnapshot)` interface methods. |
| `server/.../HAServerPlugin.java` | Add `default void replicateSecurityUsers(String content) { }` interface method. |
| `server/.../ServerSecurity.java` | Add `reloadUsers()` public method. Expose a helper to read the current users file content as a UTF-8 string, or document that the handler reads the file directly via `SecurityUserFileRepository`. |
| `server/.../PostServerCommandHandler.java` | Rewrite `dropDatabase(String)` to call `dropInReplicas()` in HA mode. Rewrite `restoreDatabase` to call `createInReplicas(true)` post-restore with compensating drop on failure. Rewrite `importDatabase` to call `createInReplicas()` before running the importer. Rewrite `createUser`/`dropUser` to compute the new file content and call `replicateSecurityUsers(content)` in HA mode (serialising with a `synchronized` on `ServerSecurity` around read-modify-submit). |

### 5.3 New unit and ha-raft integration tests

| Test | Location | Purpose |
|---|---|---|
| `RaftLogEntryCodecTest` (extend) | `ha-raft/src/test/...` | Round-trip `DROP_DATABASE_ENTRY`, `SECURITY_USERS_ENTRY` (empty content, small content, content with special characters and newlines), `INSTALL_DATABASE_ENTRY` with both `forceSnapshot` values, decode legacy install payload without the flag. |
| `ArcadeStateMachineTest` (extend) | `ha-raft/src/test/...` | Apply drop on present/absent database (idempotent replay), apply install with `forceSnapshot=true` on a present database (HTTP download stubbed), apply users entry (assert file is rewritten atomically and `ServerSecurity.reloadUsers()` is invoked). |
| `RaftDropDatabase3NodesIT` | `ha-raft/src/test/java/com/arcadedb/server/ha/raft/` | 3-node cluster, create database, insert data, drop through a replica (forwarded to leader), assert removal on all peers and on-disk directory is gone on every node. |
| `RaftRestoreDatabase3NodesIT` | same | 3-node cluster, backup a known fixture, drop, restore, assert schema and data on all peers. |
| `RaftImportDatabase3NodesIT` | same | 3-node cluster, import a small fixture file, assert vertex and edge counts on all peers. |
| `RaftUserManagement3NodesIT` | same | 3-node cluster. Create user `alice` via the leader, wait for replication, verify login as `alice` succeeds on all three peers (HTTP Basic auth). Drop user `alice`, wait for replication, verify login is rejected on all three peers. Then concurrent test: create five users in quick succession and assert every peer converges to the same user set. |
| `RaftUserSeedOnPeerAdd3NodesIT` | same | 2-node cluster with known users. Create a new user `bob`. Add a third node via `PostAddPeerHandler`. Verify the new node's `server-users.jsonl` contains `bob` and login as `bob` succeeds on the new node within the seed timeout. |

### 5.4 New e2e-ha container scenarios

All new tests extend `com.arcadedb.test.support.ContainersTestTemplate`, following the pattern established by `ThreeInstancesScenarioIT`.

| Test | Purpose |
|---|---|
| `DropDatabaseScenarioIT` | 3-node containerised cluster. Create database `dropme` via node 0 (leader), insert a small fixture (schema + 30 vertices) via `DatabaseWrapper`. Verify the fixture is visible on all nodes via `Awaitility`. Drop `dropme` via node 2 (replica) to exercise forward-to-leader. Await until HTTP `list databases` on every node excludes `dropme`. Attempt a read against `dropme` on every node, assert 4xx. |
| `RestoreDatabaseScenarioIT` | 3-node containerised cluster. Create a fixture database on the leader, populate with schema and a few hundred records. Trigger a backup via HTTP; fetch the resulting zip and serve it from a lightweight HTTP server inside the test (existing pattern in the e2e-ha support utilities, or TestContainers' `MockServerContainer`). Drop the database. Issue `restore database <name> <url>` against the leader. Await until every node opens `<name>` with matching schema and record counts. |
| `ImportDatabaseScenarioIT` | 3-node containerised cluster. Copy a small GraphML or CSV fixture into the leader's container (`copyFileToContainer`). Issue `import database imported file:///arcadedb/data/fixture.graphml` against the leader. Await until every node sees `imported` with the expected vertex and edge counts. |
| `UserManagementScenarioIT` | 3-node containerised cluster. Create user `alice` via the leader. Await and verify login as `alice` succeeds on all three nodes. Drop `alice`. Verify login is rejected on all three nodes. Create user `bob` while node 2 is stopped; restart node 2 and verify `bob` lands on node 2 as part of normal Raft catch-up. Finally, leader failover: create `charlie` on the current leader, then stop the leader and verify `charlie` can still log in on the surviving nodes (and becomes visible on node 2 once it rejoins). |
| `UserSeedOnPeerAddScenarioIT` | Start a 2-node cluster with a custom user `dave`. Issue a `post add peer` for a third container that was created fresh (its local `server-users.jsonl` contains only root). Await until login as `dave` succeeds on the newly-joined third node, asserting the seed mechanism. |

## 6. Error handling and edge cases

### 6.1 DROP DATABASE

| Failure | Behaviour |
|---|---|
| Database does not exist on the leader | HTTP handler throws `IllegalArgumentException` before any Raft submission. |
| `submitAndWait` throws (no quorum, leader step-down, timeout) | Nothing dropped anywhere. Handler returns 5xx. Client retries against the new leader. |
| State machine `applyDropDatabaseEntry` throws on one replica | Ratis convention: apply failures terminate the state machine. The node is considered unhealthy and is restarted by the operator. Logged at `SEVERE` with database name and term-index. |
| Entry replayed after restart on a node that already dropped the DB | `existsDatabase(name)` returns false, apply is a no-op. Idempotent. |
| Client issues drop against a replica | Existing `forwardToLeaderIfReplica()` handles it; `DROP_DATABASE` is already in the forward list at `PostServerCommandHandler.java:114`. |
| Leader crashes before submitting to Raft | HTTP client sees connection drop or 5xx. Nothing happened. Safe. |
| Leader crashes after submit but before commit | Ratis guarantees the entry is either discarded (never reached quorum) or preserved (reached quorum, applied by the new leader on promotion). Either outcome is consistent cluster-wide. |

### 6.2 RESTORE DATABASE

| Failure | Behaviour |
|---|---|
| Local restore on leader fails | Exception bubbles up. Database may be partially written on the leader. Raft entry is never submitted. Pre-existing behaviour, not changed by this spec. |
| Leader restore succeeds, `createInReplicas(forceSnapshot=true)` fails | Leader compensates by dropping the just-restored local database, then returns the error. Operator can retry. |
| Replica fails to download the snapshot | State machine apply throws. Same Ratis outcome as any apply failure: node terminates and restarts. Consistent with how `notifyInstallSnapshotFromLeader` already handles download failures. |
| Replica was lagging and a stale `TX_ENTRY` for the old DB is still queued behind the restore entry | Drained in order; stale page writes are absorbed by the existing tolerance in `applyTxEntry`. |

### 6.3 IMPORT DATABASE

| Failure | Behaviour |
|---|---|
| `createInReplicas` succeeds, importer fails mid-stream | Empty or partial database on all peers. Operator drops and retries. Same semantics as non-HA today; import is not transactional across the whole dataset. |
| Importer transactions flood the Raft log | Normal back-pressure via `submitAndWait` on each commit. Already exercised by existing insert benchmarks. |
| `createInReplicas` itself fails | Leader drops the local database and returns the error. |

### 6.4 CREATE / DROP USER

| Failure | Behaviour |
|---|---|
| Attempt to create a user that already exists | Handler throws `SecurityException` BEFORE computing the new file content and submitting; no Raft entry produced. |
| Attempt to drop a user that does not exist | Handler throws `IllegalArgumentException` before submitting; no Raft entry. |
| `submitAndWait` fails | Nothing written on disk anywhere. The in-memory `ServerSecurity.users` map is unchanged on every node because the handler never mutated it locally. Handler returns 5xx. |
| State machine apply fails on one replica (disk full, permission denied) | Ratis terminates that state machine; the node is restarted. On restart, the node catches up and applies the entry (assuming the underlying cause is resolved). If the leader's own apply fails, Ratis will step it down and a new leader takes over. |
| Concurrent mutations on the leader | Serialised via `synchronized(ServerSecurity)` around read-compute-submit so no lost update. |
| Peer-add seed submitted while a user mutation is in flight | Raft serialises both entries; final state is whichever was applied last, both are idempotent overwrites. Any subsequent mutation re-converges. |

### 6.5 Concurrency and locks

- `ArcadeStateMachine.applyTransaction` is invoked serially by Ratis for a given group, so drop, install, users, and transaction entries cannot race each other on apply.
- No write lock is taken on the leader before submission for drop/install/users entries. The Raft log itself is the serialisation point (plus the `synchronized(ServerSecurity)` block for the compute-then-submit section of user mutations).
- On the leader, `dropInReplicas()` holds a reference to the `HAReplicatedDatabase` wrapper across the Raft submit. Once the apply closes the underlying `LocalDatabase`, subsequent accesses through that wrapper fail with "database is closed", which is correct.

### 6.6 Wire compatibility

- **`forceSnapshot` flag**: length-based detection. After reading the database name, check `dis.available() > 0` and read a `boolean` if present, defaulting to `false`. Old replicas reading new-format payloads ignore the trailing byte; safe as long as no old replica ever sees a `forceSnapshot=true` entry.
- **`SECURITY_USERS_ENTRY`**: a brand-new type byte. Old replicas that don't recognise it will hit the default branch in `RaftLogEntryType.fromId` and throw, terminating the state machine. Mixed-version clusters are not supported.

Rolling upgrades across differing `RaftLogEntryCodec` versions are not a goal today; the cluster is expected to upgrade via full restart. If rolling upgrades become a requirement, we revisit with an explicit version byte.

## 7. Known limitations

1. `create database` is still local-first; the symmetric Raft-first refactor is deferred.
2. No cross-version codec compatibility (rolling upgrades not supported).
3. If the leader's local restore succeeds but both `createInReplicas` and the compensating local drop fail, manual operator intervention is required. Exceedingly rare double-failure; logged clearly.
4. Drop and users entries are synchronous on every peer's apply thread. A replica with a very slow filesystem could delay log apply for the group; this is an inherent property of the Raft state machine model.
5. User management via pathways other than `PostServerCommandHandler` (direct calls to `ServerSecurity.createUser` from server-internal code) is NOT covered. Any such internal caller must be updated separately to use the replication hook when HA is enabled, or it will create an out-of-sync state.
6. The seed-on-peer-add mechanism covers a peer that is added via `PostAddPeerHandler`. Peers that start fresh and come up via other means (manual config, restart from empty storage) rely on the next user mutation to receive their seed. In practice the seed is triggered automatically by the first mutation after cluster startup.

## 8. Testing strategy

### 8.1 Unit tests

**`RaftLogEntryCodecTest` (extend existing)**

- Round-trip `DROP_DATABASE_ENTRY("foo")`. Assert decoded type, database name, and that unrelated `DecodedEntry` fields are null.
- Round-trip `INSTALL_DATABASE_ENTRY("foo", forceSnapshot=false)` and `forceSnapshot=true`.
- Decode a hand-crafted legacy install entry (type byte + UTF only). Assert `forceSnapshot=false`.
- Round-trip `SECURITY_USERS_ENTRY` for: empty content, a 1-user JSON line, a 5-user JSON content with newlines and unicode characters. Assert `usersJson` is preserved byte-for-byte.

**`ArcadeStateMachineTest` (extend existing)**

- `applyDropDatabaseEntry` on a present database: fake `ArcadeDBServer` and `DatabaseInternal`; assert `drop()` and `removeDatabase()` are called.
- `applyDropDatabaseEntry` on an absent database: assert neither is called (idempotent replay).
- `applyInstallDatabaseEntry` with `forceSnapshot=true` on a present database: stub the HTTP snapshot download; assert the database is closed and the helper is invoked with the correct URL and cluster token.
- `applySecurityUsersEntry`: fake server root with a pre-existing `server-users.jsonl`, apply an entry with new content, assert the file is overwritten atomically (via temp + rename) and `ServerSecurity.reloadUsers()` is invoked.

### 8.2 ha-raft integration tests (in-process 3-node clusters)

**`RaftDropDatabase3NodesIT extends BaseRaftHATest`**

- `getServerCount() = 3`, `isCreateDatabases() = false`.
- POST `create database RaftDropTest` on leader (server 0), wait until all three peers report the database in `getDatabaseNames()`.
- Insert 10 vertices via SQL against the leader; `waitForReplicationIsCompleted()` on each peer.
- POST `drop database RaftDropTest` against **server 1** (a replica) to exercise the forward-to-leader path.
- `Awaitility.await()` (10s) until `server.getDatabaseNames()` on all three peers no longer contains `RaftDropTest`.
- Assert the on-disk directory `<databaseDirectory>/RaftDropTest` is gone on every peer.
- Sanity check: drop again against the leader, expect a 4xx "database does not exist".

**`RaftRestoreDatabase3NodesIT extends BaseRaftHATest`**

- Use the auto-created fixture database from `BaseGraphServerTest`. Insert a known fixture (one type with 50 vertices plus edges).
- Run `backup database` on the leader to a local `file://` URL.
- POST `drop database <dbname>` and verify removal on all peers.
- POST `restore database <dbname> file://.../fixture.zip` on the leader.
- Await until every peer opens `<dbname>`. Assert schema types match, vertex and edge counts match on each peer, a sample record is readable.

**`RaftImportDatabase3NodesIT extends BaseRaftHATest`**

- Use a small GraphML or JSON fixture under test resources.
- POST `import database RaftImportTest file:///.../fixture.graphml` on the leader.
- Await all peers contain `RaftImportTest`. Assert vertex and edge counts and a known property match across peers.

**`RaftUserManagement3NodesIT extends BaseRaftHATest`**

- 3-node cluster. POST `create user {"name":"alice","password":"secret123","databases":{"*":["admin"]}}` on the leader.
- Await until HTTP Basic auth as `alice:secret123` succeeds (`/api/v1/ready` or similar) on all three nodes.
- POST `drop user alice` on the leader. Await until login as `alice` returns 401 on all three nodes.
- Submit five concurrent create-user requests to the leader (`u1`..`u5`) using a latch to fire them simultaneously. Assert every peer's `server-users.jsonl` contains all five users after `Awaitility` settles.
- Read each peer's `server-users.jsonl` file directly and assert byte-for-byte equality.

**`RaftUserSeedOnPeerAdd3NodesIT extends BaseRaftHATest`**

- Start a 2-node cluster. Create user `carol` via the leader.
- Start a third server in-process and issue `post add peer` to the leader to include it.
- Await until `carol` can log in against the third node, and assert the on-disk `server-users.jsonl` of the third node contains `carol`.

### 8.3 e2e-ha container scenarios

Each extends `ContainersTestTemplate`, uses the existing `createArcadeContainer` helper, and follows the 3-node pattern of `ThreeInstancesScenarioIT`. Each test runs with a 10-minute `@Timeout`.

**`DropDatabaseScenarioIT`**
- Three containers named `arcadedb-0`, `arcadedb-1`, `arcadedb-2` with majority quorum.
- Create database `dropme` on node 0, populate with schema and 30 vertices via `DatabaseWrapper`.
- `Awaitility` convergence check on vertex count across all three `DatabaseWrapper`s.
- Issue `drop database dropme` via node 2 (forced replica path).
- `Awaitility` until HTTP `list databases` on every node no longer contains `dropme`.
- Final assertion: a read attempt against `dropme` on every node returns a 4xx/5xx indicating the database is absent.

**`RestoreDatabaseScenarioIT`**
- Three containers, majority quorum.
- Create `source` on node 0, populate with a small fixture, trigger a backup to a volume-mounted path.
- Copy the resulting zip to all containers (or serve it from a side container via TestContainers, whichever matches existing e2e-ha support patterns).
- Drop `source`, issue `restore database source file:/data/backups/source.zip` on the leader.
- `Awaitility` until every node opens `source` with the expected schema and record counts.

**`ImportDatabaseScenarioIT`**
- Three containers.
- Copy a small GraphML fixture into the leader container via `copyFileToContainer`.
- Issue `import database imported file:/data/fixtures/fixture.graphml` on the leader.
- `Awaitility` until every node sees `imported` with the expected vertex and edge counts.

**`UserManagementScenarioIT`**
- Three containers.
- Create user `alice`; verify login on all three nodes.
- Drop `alice`; verify login rejected on all three nodes.
- Stop node 2. Create user `bob` on the leader; verify login on nodes 0 and 1. Restart node 2; verify `bob` can log in on node 2 once Raft catch-up completes.
- Leader failover: create `charlie` on the current leader, then stop the leader container. Verify a surviving node becomes leader and `charlie` can still log in on the surviving nodes. Restart the old leader; verify `charlie` is visible on it once it rejoins.

**`UserSeedOnPeerAddScenarioIT`**
- Start two containers with a pre-created custom user `dave` (inject `server-users.jsonl` at container creation time, or create the user after start and wait for convergence).
- Start a third container fresh (its `server-users.jsonl` contains only root).
- Issue a peer-add command against the leader to include the new container.
- `Awaitility` until login as `dave` succeeds against the newly-added container.

### 8.4 Guardrails

Inherited from existing base classes:

- `BaseRaftHATest`: per-test Raft storage cleanup, unique Raft port offsets per in-process server, `waitForReplicationIsCompleted` for write quiescence.
- `ContainersTestTemplate`: container network isolation per test class, deterministic server wrapping, `DatabaseWrapper` helpers for schema/data operations.

### 8.5 Verification commands before claiming the change works

1. `mvn -pl ha-raft -am test` - unit tests pass, including the new codec and state-machine cases.
2. `mvn -pl ha-raft verify` - the six new ha-raft integration tests pass.
3. `mvn -pl server,ha-raft -am install` - full compile of affected modules with no warnings.
4. `mvn -pl e2e-ha verify` - the five new container scenarios pass (tests are tagged as IT and run only under the verify phase with Docker available).
5. Re-run `RaftHTTP2ServersCreateReplicatedDatabaseIT`, `RaftReplication3NodesIT`, and `ThreeInstancesScenarioIT` to confirm the changes do not break the existing create/write paths.

## 9. Open items for follow-up specs

1. **Symmetric Raft-first refactor of `INSTALL_DATABASE_ENTRY`**: unify the pattern between create and drop so neither operation can leave the cluster in an inconsistent state across a leader crash during submission.
2. **Failure-scenario tests** for drop/restore/import/user ops: partition during commit, leader crash mid-apply, follower lag past the compaction horizon while a drop is pending. These belong with the broader failure-scenarios suite.
3. **Rolling-upgrade story**: a version byte in the codec and documented compatibility windows.
4. **Internal user-management callers**: audit code paths that call `ServerSecurity.createUser`, `dropUser`, `updateUser`, or `setUserPassword` directly (outside `PostServerCommandHandler`) and route them through `replicateSecurityUsers` when HA is enabled.
