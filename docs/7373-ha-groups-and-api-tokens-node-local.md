# #7373 - HA: groups and API tokens are node-local while users are replicated

Branch: `fix/7373-ha-groups-and-api-tokens-node-local`

## The defect

Server users go through `ServerSecurity.*ClusterWide` and submit a `SECURITY_USERS_ENTRY`, so a user change
made on any node reaches every node. Groups and API tokens did not: `ServerSecurity.saveGroup`/`deleteGroup`
wrote `server-groups.json` and `ApiTokenConfiguration.createToken`/`deleteToken` wrote
`server-api-tokens.json`, both on whichever node served the request, and no peer ever heard about it.

Consequences, as the report states them:

- a group created on node A did not exist on B or C. The user holding it authenticated everywhere (the user
  document *is* replicated) and then resolved to no permissions on two nodes out of three - the same
  credentials getting different authorization depending on which node answered;
- an API token minted on node A authenticated only against node A, which behind a load balancer is an
  intermittent 401 with no pattern the operator can see;
- deleting either on one node left it live on the others. For a **revoked token** that is a security failure,
  not a consistency one: revocation that silently applies to one third of the cluster reads as success.

## Completeness

### 1. The invariant

**A group or API-token mutation accepted by any node of an HA cluster is applied on every node of that
cluster, or it fails.**

### 2. Enumerating every way to violate it

Writers of the group document:

```
$ grep -rn "groupRepository.save(\|\.saveGroup(\|\.deleteGroup(\|saveGroups()" --include="*.java" \
    server/src/main/java ha-raft/src/main/java grpcw/src/main/java
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:586:    server.getSecurity().saveGroup(database, name, normalized);
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:606:    if (!server.getSecurity().deleteGroup(database, name))
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:915:  public void saveGroups() {
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:917:      groupRepository.save(groupsToJSON());
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:939:      groupRepository.save(root);
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:964:      groupRepository.save(root);
server/src/main/java/com/arcadedb/server/http/handler/DeleteGroupHandler.java:50:      controlPlane.deleteGroup(database, name);
server/src/main/java/com/arcadedb/server/http/handler/PostGroupHandler.java:53:      controlPlane.saveGroup(database, name, payload);
grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcAdminService.java:440:      controlPlane.saveGroup(...)
grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcAdminService.java:452:      controlPlane.deleteGroup(...)
```

Writers of the token store (`save()` call sites inside `ApiTokenConfiguration`, plus every caller reaching it):

```
$ grep -n "save()" server/src/main/java/com/arcadedb/server/security/ApiTokenConfiguration.java
95:        save();        # load(): startup expiry prune + plaintext->hash migration
154:    save();           # createToken
166:      save();         # deleteToken
181:      save();         # getToken: lazy eviction of an expired token on the READ path

$ grep -rn "createApiToken(\|deleteApiToken(\|listApiTokens(" --include="*.java" server grpcw | grep -v /test/
server/.../ServerControlPlane.java:675:  public JSONObject createApiToken(...)
server/.../ServerControlPlane.java:707:  public void deleteApiToken(...)
server/.../http/handler/PostApiTokenHandler.java:54:      tokenJson = controlPlane.createApiToken(
server/.../http/handler/DeleteApiTokenHandler.java:47:      controlPlane.deleteApiToken(...)
grpcw/.../ArcadeDbGrpcAdminService.java:493:      final JSONObject token = controlPlane.createApiToken(...)
grpcw/.../ArcadeDbGrpcAdminService.java:511:      controlPlane.deleteApiToken(...)
```

The load-bearing result: **every mutating transport converges on `ServerControlPlane`**. HTTP
(`PostGroupHandler`, `DeleteGroupHandler`, `PostApiTokenHandler`, `DeleteApiTokenHandler`) and gRPC
(`ArcadeDbGrpcAdminService`) call the same four methods, and nothing else in `src/main/java` writes either
document. There is no third writer to miss.

Sibling shape - "security state written to a file with no HA counterpart":

```
$ grep -rn "saveGroup|deleteGroup|groupRepository|ApiToken" --include="*.java" \
    server/src/main/java/com/arcadedb/server/ha/
(no matches)                       # before this change
```

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| HTTP `POST /api/v1/server/groups/{db}/{name}` -> `ServerControlPlane.saveGroup` | yes | yes - `savingAGroupInAClusterIsReplicatedAndNotJustWrittenLocally` |
| HTTP `DELETE /api/v1/server/groups/{db}/{name}` -> `ServerControlPlane.deleteGroup` | yes | yes - `deletingAGroupInAClusterIsReplicated`, `deletingAGroupThatDoesNotExistReplicatesNothing` |
| gRPC `SaveGroup` / `DeleteGroup` -> the same two `ServerControlPlane` methods | yes | yes - same tests; the grep above is the evidence that the gRPC adapter has no other path to the store |
| HTTP `POST /api/v1/server/api-tokens` -> `ServerControlPlane.createApiToken` | yes | yes - `mintingAnApiTokenInAClusterIsReplicatedAndAuthenticatesOnAPeer`, `aMintWhoseEntryFailsLeavesNoTokenOnTheServingNode` |
| HTTP `DELETE /api/v1/server/api-tokens` -> `ServerControlPlane.deleteApiToken` | yes | yes - `revokingAnApiTokenInAClusterIsReplicatedAndTakesEffectOnAPeer`, `revokingAnUnknownApiTokenReplicatesNothing` |
| gRPC `CreateApiToken` / `DeleteApiToken` -> the same two `ServerControlPlane` methods | yes | yes - same tests, same evidence |
| Raft apply on a peer -> `ServerSecurity.applyReplicatedGroups` | yes | yes - `aPeerApplyingTheReplicatedGroupDocumentGetsTheGroup`, `aGroupDocumentWithoutDatabasesIsRefusedAndChangesNothing`, `aGroupDocumentWithoutAVersionIsRefused`, `aGroupDocumentWhoseDatabasesSectionIsNotAnObjectIsRefused` |
| Raft apply on a peer -> `ServerSecurity.applyReplicatedApiTokens` | yes | yes - the two token tests above, `anApiTokenDocumentThatCannotBeReadChangesNothing` |
| Raft wire: `SECURITY_GROUPS_ENTRY` / `SECURITY_API_TOKENS_ENTRY` encode/decode | yes | yes - `Issue7373SecurityConfigEntryCodecTest` (6 tests) |
| `POST /api/v1/server/addPeer` seeding a newly-joined peer | yes | yes - `theSeedPayloadsRoundTripIntoAFreshNode` covers the payload builders and the apply; the handler wiring itself is the same three-line shape as the pre-existing users seed |
| Non-HA (single node): both documents still written locally | yes | yes - `savingAGroupWithoutHAStillWritesLocally`, `mintingAnApiTokenWithoutHAStillWritesLocally` |
| `ApiTokenConfiguration.getToken` lazy eviction of an EXPIRED token (`save()` at line 181) | **argued** | n/a |
| `ApiTokenConfiguration.load()` startup expiry prune + plaintext->hash migration (`save()` at line 95) | **argued** | n/a |
| `ServerSecurity.saveGroups()` | **argued** | n/a |
| Concurrent group/token change on two DIFFERENT nodes | **filed** - #7509 | n/a |
| Peers' cached per-database permissions refresh only on the file-watcher tick | **filed** - #7510 | n/a |
| A peer running a version older than this change receives entry type 7/8 | **filed** - #7511 | n/a |

Arguments, with evidence:

- **`getToken` lazy eviction.** Expiry is a wall-clock deadline stored inside the token document, and every node
  holds the same document, so every node evicts the same token at the same instant without being told to. The
  write is a local rewrite of a document whose *effect* is already identical cluster-wide. Replicating it would
  put a Raft round trip on the authentication path - `authenticateByApiToken` -> `getToken` runs on every
  API-token request - which is exactly what `createUserClusterWide`'s javadoc forbids: "Nothing on a request hot
  path may be put inside this monitor."
- **`load()` startup prune/migration.** Per-node normalization of that node's own file at startup: dropping
  already-expired entries and hashing a legacy plaintext token. It does not change which tokens authenticate
  (an expired one already did not), and it runs before the node is serving.
- **`ServerSecurity.saveGroups()`** has no caller in `src/main/java`:
  `grep -rn "saveGroups()" --include="*.java" */src/main/java` returns only its own declaration. It cannot
  violate the invariant because nothing reaches it. Left as-is rather than deleted, being public API.

### 4. Reachability

The changed code is on the live path, not only under test:

- `ServerControlPlane.saveGroup`/`deleteGroup`/`createApiToken`/`deleteApiToken` are the bodies of the four
  HTTP handlers registered in `HttpServer` and of the four gRPC RPCs in `ArcadeDbGrpcAdminService`.
- `RaftHAPlugin.replicateSecurityGroups`/`replicateSecurityApiTokens` override the `HAServerPlugin` defaults and
  are reached through `server.getHA()`, which `RaftHAPlugin.startService()` sets via `server.setHA(this)`.
- `ArcadeStateMachine.applySecurityGroupsEntry`/`applySecurityApiTokensEntry` are reached from the `switch` on
  `decoded.type()` in the state machine's `applyTransaction`, which is the only apply path.
- No new configuration flag gates any of it: the branch is taken whenever `server.getHA() != null`, which is the
  same condition the user path already uses.

### 5. Residual risk

What this fix does **not** cover:

1. Two administrators changing groups (or tokens) at the same instant on two *different* nodes can lose one
   change: the payload is the whole document, read-modify-write, with the monitor serialising only within one
   node. This is the pre-existing shape of the user path as well - `createUserClusterWide`'s javadoc says so -
   and it is now filed as **#7509** rather than left implicit.
2. On a peer, the *cached* per-database permissions derived from the group document are refreshed by
   `SecurityGroupFileRepository`'s file watcher, on the `arcadedb.server.security.reloadEvery` interval, not by
   the apply (the apply runs on the Raft state-machine thread, which must never block, and `updateSchema` walks
   every open database). The node that served the request refreshes immediately. Filed as **#7510**.
3. A node older than this change halts on entry type 7 or 8 with the deliberate "unknown entry type" halt
   (issue #4798). Group and token administration therefore has to wait until the whole cluster is upgraded.
   Filed as **#7511**.
4. A concurrent `getToken` expiry-eviction `save()` can write a document built from a token map that a
   replicated apply is swapping underneath it, so the file can briefly hold a stale set. The in-memory set -
   the one that authenticates - is correct, being a single reference swap, and the next replicated apply
   rewrites the file. No test pins this.

## Implementation

### server

- `HAServerPlugin` - two new default no-ops, `replicateSecurityGroups` and `replicateSecurityApiTokens`,
  modelled on `replicateSecurityUsers`.
- `ServerSecurity`
  - `groupsDocumentWith(database, name, groupConfig|null)` - the single place the save/delete walk is
    expressed, shared by the local and the replicated path (the reason `snapshotWith` exists for users);
    returns `null` when a delete finds nothing.
  - `saveGroupClusterWide` / `deleteGroupClusterWide` - submit the resulting document as a Raft entry when
    `getHA() != null`, otherwise fall back to the existing local mutators. The monitor is held across the round
    trip, exactly as `createUserClusterWide` does and for the same reason.
  - `createApiTokenClusterWide` / `deleteApiTokenClusterWide` - same shape. The mint generates the token
    locally but installs nothing: the store is mutated only by the apply.
  - `applyReplicatedGroups` / `applyReplicatedApiTokens` - the peer side. Neither takes the `ServerSecurity`
    monitor, or it would deadlock against the submitter waiting for this very entry.
  - `getGroupsJsonPayload` / `getApiTokensJsonPayload` - the peer-seed payloads.
- `SecurityGroupFileRepository.applyReplicated` - publish in memory first, persist second, **return** the write
  failure. The `save()` path keeps the opposite order.
- `ApiTokenConfiguration` - `mintToken` (generate without installing), `documentWithout`, `toJsonPayload`,
  `applyReplicated`; `save()` refactored onto a `persist(document)` that returns its failure; the token map
  became a volatile reference that is replaced rather than cleared in place, so a concurrent
  `authenticateByApiToken` can never see the empty window a `clear()`-then-repopulate leaves behind.
- `ReplicatedSecurityConfigPersistenceException` - the groups/tokens counterpart of
  `ReplicatedUsersPersistenceException`, so the apply site can tell "already in force, only the write failed"
  from "this node cannot read a committed entry".
- `ServerControlPlane` - the four mutators now call the cluster-wide variants.

### ha-raft

- `RaftLogEntryType` - `SECURITY_GROUPS_ENTRY((byte) 7)`, `SECURITY_API_TOKENS_ENTRY((byte) 8)`.
- `RaftLogEntryCodec` - `encodeSecurityGroupsEntry` / `encodeSecurityApiTokensEntry` over a shared
  `encodeSecurityEntry`, and one `decodeSecurityEntry(dis, type)` for all three security entries, which share a
  wire shape and the `DecodedEntry.usersJson` payload slot.
- `ArcadeStateMachine` - two new `case`s and two apply methods, with the same failure classification as
  `applySecurityUsersEntry`.
- `RaftTransactionBroker`, `RaftHAPlugin` - the submit path.
- `PostAddPeerHandler` - seeds a newly-joined peer with all three documents instead of only the users file,
  each independently so one failing seed does not skip the other two.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not read any of the above and asks it to write the
follow-up issue it would file against the patch. **No subagent tool was available in this session**, so the
pass was run by hand against the diff instead, which is weaker - the reviewer had already been persuaded. It
found four things worth writing down.

1. **Real, in scope - fixed here.** `applyReplicatedGroups` checked only that the document had a `databases`
   key. Two ways past that check mattered:
   - a `databases` that is not an object installs, and then every authorization lookup on the node
     (`getDatabaseGroupsConfiguration`, called per request) throws instead of answering;
   - **a document with no `version` installs and is written out, and then**
     `SecurityGroupFileRepository.load()` **discards a versionless file at the next restart and falls back to**
     `createDefault()` - the silent widening back to default permissions that the repository's own atomic-write
     comment exists to prevent. A permission change that shows up at an unrelated restart is the worst shape
     this bug could have taken.

   Both are now refused before any mutation, which puts them on the halt side of the #4798 line rather than the
   "stay up" side. Pinned by `aGroupDocumentWithoutAVersionIsRefused` and
   `aGroupDocumentWhoseDatabasesSectionIsNotAnObjectIsRefused`.

2. **Real, in scope - fixed here.** `groupsDocumentWith` reads a null replacement as "remove this group", so
   `saveGroupClusterWide(db, name, null)` would have replicated a deletion under the name of a save. Guarded,
   pinned by `savingANullGroupDefinitionIsRejected`.

3. **Not real.** "`groupsDocumentWith` mutates the live group document, so a failed Raft submit leaves the
   change applied locally anyway." It does not: `JSONObject.copy()` is `new JSONObject(object.deepCopy())`
   (`engine/src/main/java/com/arcadedb/serializer/json/JSONObject.java:99`), so the walk mutates a detached
   copy and a failed submit leaves the node exactly as it was. `aMintWhoseEntryFailsLeavesNoTokenOnTheServingNode`
   pins the same property on the token half.

4. **Not real.** "Holding the `ServerSecurity` monitor across the Raft round trip blocks authentication for the
   duration of consensus." Neither `authenticate` (`ServerSecurity.java:215`) nor `authenticateByApiToken`
   (`ServerSecurity.java:1302`) is `synchronized`; the users path has held that monitor across consensus since
   #6808 for the same reason.

Noted and deliberately not filed: `ApiTokenConfiguration.createToken`/`deleteToken` stay public and now bypass
replication. Nothing in `src/main/java` calls them any more - the grep in section 2 is the evidence - and both
now carry javadoc naming them the local half, the same way `ServerSecurity.dropUserLocally` does for users.

## Test results

```
server:   Issue7373ClusterWideGroupsAndTokensTest                 16/16 green
          + ApiTokenConfigurationTest, SecurityGroupFileRepositoryTest, SecurityUserFileRepositoryTest,
            ServerSecurityUsersConcurrencyTest, Issue7137ReplicatedUsersAppliedOnWriteFailureTest,
            Issue6806GroupPermissionRefreshTest, ServerSecurityAuthHardeningTest, ServerSecuritySaltCacheTest,
            ServerSecurityDatabaseUserConcurrencyTest                          Tests run: 58, Failures: 0

ha-raft:  Issue7373SecurityConfigEntryCodecTest                     6/6 green
          whole module, unit lane (-DexcludedGroups=benchmark,slow,vector)     Tests run: 415, Failures: 0

whole reactor: mvn -o install -DskipTests  and  mvn -o test-compile   both clean
```

**Falsification.** With `ServerControlPlane` reverted to the pre-fix calls (`saveGroup`, `deleteGroup`,
`getApiTokenConfiguration().createToken`, `...deleteToken`) and everything else unchanged, 6 of the 16 new
tests fail - the four "is replicated" assertions plus the two peer round trips. The remaining 10 are the
non-HA controls and the apply-path tests, which exercise methods that did not exist before.

**Not run here:** every `BaseRaftHATest` subclass (`LeaveClusterTest`, `WaitForApplyTest`,
`DynamicMembershipTest`, ...). They derive from `BaseGraphServerTest`, which binds fixed ports from 2480, and
on this machine 2480 is held by two foreign JVMs (another agent's worktree and a standalone server). All three
classes crash their surefire fork identically, with or without this change, and none of them is touched by it.
