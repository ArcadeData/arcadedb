# #7509 - a replicated security document is read-modify-write, so a concurrent admin change on another node is lost

## The defect

All three replicated security documents - the user list (`SECURITY_USERS_ENTRY`), the group document
(`SECURITY_GROUPS_ENTRY`) and the API-token document (`SECURITY_API_TOKENS_ENTRY`) - are replicated as the
**whole document**: the submitter reads the current one, mutates a copy, and submits the result.

`ServerSecurity.createUserClusterWide` serialises that read-compute-submit sequence with
`synchronized (this)`. That monitor is per-node. Node B doing the same thing at the same time builds its
payload from its own view; Raft linearises the two entries and the second one carries a document that was
built without the first one in it. The first change is reverted on every node, including the one that
accepted it and answered 200.

## Root cause

There is no conditional apply. Every `SECURITY_*_ENTRY` is applied unconditionally, so the last writer of the
log wins whatever it happened to read.

## The invariant the fix establishes

> A replicated security document entry that was built from version V of that document is applied only when
> every node's current version of that document is still V; otherwise nothing is installed anywhere and the
> submitter is told, so a concurrent change made on another node can never be silently overwritten.

## Completeness

### Entry points that submit a replicated security document

```
$ grep -rn --include='*.java' "replicateSecurityUsers(\|replicateSecurityGroups(\|replicateSecurityApiTokens(" . | grep "/src/main/"
ha-raft/.../RaftTransactionBroker.java:441,449,457     (transport)
ha-raft/.../RaftHAPlugin.java:204,209,219,224,234,239  (transport)
server/.../HAServerPlugin.java:308,323,338             (interface)
server/.../ServerSecurity.java:422   createUserClusterWide
server/.../ServerSecurity.java:448   updateUserClusterWide
server/.../ServerSecurity.java:475   dropUserClusterWide
server/.../ServerSecurity.java:1024  saveGroupClusterWide
server/.../ServerSecurity.java:1043  deleteGroupClusterWide
server/.../ServerSecurity.java:1151  seedUsersClusterWide        (seed)
server/.../ServerSecurity.java:1158  seedGroupsClusterWide       (seed)
server/.../ServerSecurity.java:1165  seedApiTokensClusterWide    (seed)
server/.../ServerSecurity.java:1187  createApiTokenClusterWide
server/.../ServerSecurity.java:1208  deleteApiTokenClusterWide
server/.../ServerControlPlane.java:226 connectCluster users seed  <-- reads OUTSIDE the monitor, seeds users only
```

### Apply sites

```
$ grep -rn --include='*.java' "applyReplicatedUsers(\|applyReplicatedGroups(\|applyReplicatedApiTokens(" . | grep "/src/main/"
ha-raft/.../ArcadeStateMachine.java:3648, 3692, 3723
server/.../ServerSecurity.java:840, 1067, 1220
```

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `ServerSecurity.createUserClusterWide` | yes - CAS on the users version | yes |
| `ServerSecurity.updateUserClusterWide` | yes | yes |
| `ServerSecurity.dropUserClusterWide` (also the `SecurityManager.dropUser` / Cypher `DROP USER` door) | yes | yes |
| `ServerSecurity.saveGroupClusterWide` | yes - CAS on the groups version | yes |
| `ServerSecurity.deleteGroupClusterWide` | yes | yes |
| `ServerSecurity.createApiTokenClusterWide` | yes - CAS on the API-token version | yes |
| `ServerSecurity.deleteApiTokenClusterWide` | yes | yes |
| `seedUsers/Groups/ApiTokensClusterWide` (peer seeding) | deliberately UNCONDITIONAL, but stamps the new version so every node converges | yes |
| `ServerControlPlane.connectCluster` users seed | yes - rerouted to `seedSecurityStateClusterWide()`, so it reads under the monitor and seeds all three documents | yes |
| wire: `SECURITY_*_ENTRY` from a peer that predates the fix (no CAS section) | applies unconditionally, exactly as today | yes (codec test) |

## Design

The version is a **per-document-type monotonic counter that is part of the replicated state**, not something
derived from the node's local documents. That distinction is the whole safety argument:

- Deriving the check from a digest of the local document would make the accept/reject decision depend on
  per-node state that can legitimately differ (a node's own `root` password hash before the first seed, a
  hand-edited group file, the file watcher). Two nodes reaching different decisions on the same committed
  entry is divergence - the failure #6808 and #7373 exist to prevent - which is strictly worse than the lost
  update being fixed.
- A counter that is only ever advanced by an apply is identical on every node that has applied the same
  prefix of the log, so every node reaches the same verdict on every entry.

The counter has to survive a restart, or a restarted node would disagree with its peers, so it is persisted
next to the documents in `server-security-versions.json` (absent = 0 on every node, which is what an upgrading
cluster converges on).

The entry carries `(expectedVersion, newVersion)` in a `RaftLogEntryCodec` **extension section** (the #7138
forward-compatibility frame), so a peer that predates the fix skips the section and applies unconditionally -
today's behaviour - rather than halting on an unknown field.

`expectedVersion = -1` means "apply unconditionally" and is what peer seeding submits: a seed must never be
refused, because the joining peer has nothing yet. It still carries a `newVersion`, so every node ends on the
same counter.

## Residual risk

- A node whose `server-security-versions.json` write fails (a full or read-only config volume, the #7137
  condition) keeps the new document in force in memory but reloads a stale counter after a restart, and then
  disagrees with its peers about the next entry. Filed as a follow-up; see below.
- Automatic re-read-and-retry on the submitting node is **not** implemented: the conflict surfaces to the
  caller as an error. The reported failure is that a lost update is *silent*; it no longer is. Filed as a
  follow-up.

## Adversarial pass

The orchestrator's `Task` tool was not available in this session, so no subagent could be spawned in ignorance of
the above. The pass was run by hand against the staged diff and the issue body instead; that is weaker, and it is
recorded here rather than claimed as an isolated review.

| Finding | Disposition |
|---|---|
| During a rolling upgrade a peer with #7138 but not #7509 skips the compare-and-set section and applies an entry the upgraded nodes refuse - divergent verdicts on a committed entry | **Real, out of scope.** Filed as #7540 with the interim mitigation. Closing it needs a cluster-wide capability signal the HA layer does not have. |
| The counter is written to its own file, so a failed write leaves a node that disagrees with its peers after a restart | **Real, out of scope.** Filed as #7536 (it needs a per-document format change, `server-users.jsonl` included). |
| A conflict is reported to the caller but never re-read and retried | **Real, out of scope.** Filed as #7537. The reported defect is that the lost update was SILENT, and it no longer is. |
| The submitter reads the document and the counter a moment apart, so it can stamp a version the apply refuses | **Real, fixed here by ordering**: the apply records the counter AFTER installing the document, so the only reachable window is a spurious refusal (costs a retry), never an accepted stale entry. Written into `recordReplicatedSecurityVersion`'s javadoc, because the opposite order silently reintroduces the bug. |
| `checkReplicatedSecurityVersion` runs before the payload is parsed, so a conflicting entry that is ALSO unreadable is refused instead of halting the node | **Not a defect.** Every node reaches the same verdict - the conflict is decided on the counter, which is identical everywhere - so the unreadable payload is parsed nowhere and diverges nothing. |
| `recordReplicatedSecurityVersion` does file I/O on the Raft apply thread, which `applyReplicatedUsers` documents as a no-blocking thread | **Not a defect.** That invariant is about not taking the `ServerSecurity` monitor and not waiting on a lock the blocked submitter holds; the apply already writes the document itself on this thread. `SecurityDocumentVersions.saveLock` is taken by nothing but its own writer, and no submitter path calls `record`. The cost is one extra fsync per security entry, on a path that runs when an operator changes a user. |
| A conflicting `createApiTokenClusterWide` might leave a minted token behind on the serving node | **Not a defect.** `mintToken` builds the document without installing anything; installation happens in the apply. Already pinned by `Issue7373ClusterWideGroupsAndTokensTest.aMintWhoseEntryFailsLeavesNoTokenOnTheServingNode`, which still passes. |

## Tests

- `server`: `Issue7509ReplicatedSecurityDocumentCasTest` - 12 tests, one race per fixed entry point plus the
  seed, restart and no-versions paths. With the compare-and-set disabled, 7 of the 12 fail - one for each of the
  seven submitting entry points.
- `ha-raft`: `Issue7509SecurityEntryCasCodecTest` - 7 tests, the wire round trip plus the two compatibility
  directions (an entry with no section, and an unknown section that must still be skipped).
- Regression: `server` 105 tests (the whole `com.arcadedb.server.security` package plus the control-plane
  tests) and `ha-raft` 96 tests (codec, extension-section, and every security-entry apply test) all pass.
