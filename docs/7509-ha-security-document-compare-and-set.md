# #7509 - HA: a replicated security document is read-modify-write, so a concurrent admin change on another node is lost

Issue: https://github.com/ArcadeData/arcadedb/issues/7509

## Problem

The three node-scoped security documents - the user list (`SECURITY_USERS_ENTRY`), the group document
(`SECURITY_GROUPS_ENTRY`) and the API-token document (`SECURITY_API_TOKENS_ENTRY`) - are replicated as the
WHOLE document, built by reading the current one, mutating a copy and submitting it. The read-compute-submit
sequence is serialised by `synchronized (this)` on `ServerSecurity`, which is a per-NODE monitor. Two nodes
doing it at the same time each build a document from their own view; Raft linearises the two entries and the
second one, built without the first in it, silently reverts the first - on every node, including the one that
accepted it and answered 200.

## Root cause

`ServerSecurity.createUserClusterWide` and its six siblings submit an unconditional "install this document"
entry. The apply has no way to tell a document built from the current state from one built from a stale one,
so it installs whatever arrives.

## Completeness

### Invariant

**A replicated security document is installed only when the document its submitter read is still the one in
force at the moment the entry applies; an entry built on a stale view is refused identically on every node,
and its submitter re-reads and retries instead of reporting success.**

### Entry points found by command

Writers of the three documents (every caller of the `replicate*` API, main sources only):

```
$ grep -rn "replicateSecurityUsers\|replicateSecurityGroups\|replicateSecurityApiTokens" --include='*.java' */src/main/java \
    | grep -v "HAServerPlugin.java\|RaftHAPlugin.java\|RaftTransactionBroker.java"
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:226      (seed after connect cluster)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:422 (createUserClusterWide)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:448 (updateUserClusterWide)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:475 (dropUserClusterWide)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:1024 (saveGroupClusterWide)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:1043 (deleteGroupClusterWide)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:1151 (seedUsersClusterWide)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:1158 (seedGroupsClusterWide)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:1165 (seedApiTokensClusterWide)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:1187 (createApiTokenClusterWide)
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:1208 (deleteApiTokenClusterWide)
```

Callers of the seven cluster-wide mutators (main sources only) all funnel through those seven methods:
`ServerControlPlane` (REST/gRPC `POST /api/v1/server` commands and the `/server/users`, `/server/groups`,
`/server/api-tokens` routes), `PostUserHandler`, `PutUserHandler`, `DeleteUserHandler`,
`DeleteDropUserHandler`, `ServerSecurity.dropUser` (the openCypher `DROP USER` path) and
`ServerSecurity.setUserPassword`. No caller reaches `HAServerPlugin.replicateSecurity*` directly except the
seed sites above.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `ServerSecurity.createUserClusterWide` | yes - CAS + retry | yes |
| `ServerSecurity.updateUserClusterWide` | yes - CAS + retry | yes |
| `ServerSecurity.dropUserClusterWide` | yes - CAS + retry | yes |
| `ServerSecurity.saveGroupClusterWide` | yes - CAS + retry | yes |
| `ServerSecurity.deleteGroupClusterWide` | yes - CAS + retry | yes |
| `ServerSecurity.createApiTokenClusterWide` | yes - CAS + retry | yes |
| `ServerSecurity.deleteApiTokenClusterWide` | yes - CAS + retry | yes |
| `ServerSecurity.seed{Users,Groups,ApiTokens}ClusterWide` | **argued** - seeding is deliberately unconditional | n/a |
| `ServerControlPlane.connectCluster` users seed | **argued** - same: a seed must overwrite | n/a |
| apply of an entry from a node that predates this fix | **argued** - no precondition section, applies as today | yes (codec) |
| a node that predates this fix APPLYING a conditional entry | yes - the precondition is gated on peer capability | yes |
| a mutation submitted on a FOLLOWER | **filed as #7559** - the capability registry is leader-only | n/a |

**Argued rows.** A seed exists precisely to overwrite whatever a joining peer holds with the cluster's
current document; giving it a precondition would make an `addPeer` fail whenever an unrelated admin change
raced it, which is the opposite of what it is for. The three seed helpers and `connectCluster` therefore
submit with a `null` precondition, which is the pre-fix behaviour, and they already hold the monitor across
the read and the submit (issue #7373), so the document they seed is a real point-in-time state of this node.

A node that predates this fix writes no precondition section; a node that has it skips the absent section and
applies unconditionally, exactly as before. See "Residual risk" for the reverse direction.

## Fix

1. **Carry the precondition.** The submitter fingerprints the document it read and ships that fingerprint in
   a framed extension section on the security entry (the #7138 mechanism, which exists so a non-schema entry
   can grow a field without halting older peers).
2. **Check it at the apply**, which is the only point that is linearised across nodes. Fingerprint mismatch
   means the document changed between the read and the apply: the entry is NOT installed, on every node, and
   the outcome is identical everywhere because applies are ordered and deterministic.
3. **Tell the submitter.** The state machine answers the Ratis client with `SECURITY_ENTRY_SUPERSEDED`
   instead of `OK`, so the verdict comes from the LEADER's apply and reaches the submitter whether it is the
   leader or a follower. The submitter waits for its own node to catch up, re-reads, rebuilds and resubmits,
   up to a bounded number of attempts - an MVCC retry, affordable because security administration is rare.
4. **Write the precondition only when every peer can read one**, through the peer-capability negotiation of
   #7219 - the gate `RaftLogEntryCodec`'s own javadoc demands of every new optional section. A peer that
   predates the section skips it and installs unconditionally, so an ungated precondition would have a losing
   entry refused on the upgraded nodes and applied on the older one: a DIVERGENCE, which is worse than the
   lost update, because the same credentials then resolve differently depending on which node answers.
   Withholding it keeps the pre-#7509 behaviour uniform, is fail-closed (an unknown, unreachable or stale peer
   counts as missing the capability), and needs no operator step: the check engages by itself once the last
   node is upgraded. A throttled INFO line names the peers responsible, so "the check is not running" is never
   silent.

The fingerprint is a SHA-256 over a canonicalised form of the document (object keys sorted, the user list and
the token list sorted by their identity field), so two nodes holding the same logical document always agree on
it regardless of map iteration order.

## Adversarial pass

The `Task` tool was not available in this session, so the Phase 1.5 subagent could not be spawned and the pass
was run by hand against the diff. It produced one finding that is real and one clarification:

1. **The compare-and-set engages only for mutations submitted on the LEADER** - `RaftHAServer` starts the
   peer-capability monitor on gaining leadership and stops it on losing it, so a follower's registry is empty
   or stale, `peersMissingCapability` names every peer, and the precondition is withheld. Verified by reading
   `startCapabilityMonitor`/`stopCapabilityMonitor`, and by reading every handler: the `/server/users` routes,
   the `POST /api/v1/server` commands and the gRPC admin RPCs all reach the leader already, while
   `DeleteDropUserHandler`, the group and API-token routes and openCypher `DROP USER` do not. Real, and the
   mechanism fix belongs to #7219 rather than here, so it is **filed as #7559**.
2. **Number canonicalisation** - `SecurityDocumentFingerprint` renders a non-string scalar with its
   `toString`, so an `Integer -1` read back from a file and a `Long -1L` held in memory fingerprint the same
   (both `"-1"`), which is what these documents need. It would NOT hold for a fractional value whose
   `toString` differs between representations; none of the three documents carries one - `version`,
   `resultSetLimit`, `readTimeout`, `createdAt` and `expiresAt` are all integral. Not a defect, recorded
   because the next field added to one of these documents could make it one.

## Residual risk

- **A half-upgraded cluster keeps the old behaviour, and that is on purpose.** While any peer has not
  advertised `security-precondition`, no precondition is written at all, so two concurrent security changes on
  two nodes can still lose one of them exactly as they did before this fix. It is reported (throttled INFO,
  naming the peers) rather than silent, and it resolves itself when the last node is upgraded. Issue #7557 was
  filed for this gap and then closed, because the gate is in this PR.
- **The retry budget is finite.** Sustained concurrent security administration from several nodes can exhaust
  it; the caller then gets an explicit "concurrent change, retry" error instead of a silent loss, which is the
  behaviour the issue asks for.
- **A follower-submitted security mutation still replicates unconditionally** (#7559): the compare-and-set is
  withheld there because the peer-capability registry is leader-only. This is NOT limited to the upgrade
  window - on a fully upgraded cluster, any admin request a load balancer routes to a follower
  (`DeleteDropUserHandler`, the group and API-token REST routes, openCypher `DROP USER`) keeps the pre-fix
  behaviour indefinitely. Nothing regresses relative to today, but the fix does not reach those callers.
- **Nothing here changes the local (non-HA) path.** `createUser`/`updateUser`/`dropUserLocally` are already
  serialised by the same monitor on the only node that holds the document.

## Review cycles

### Cycle 1 - `cfb2ceddc9`

The `claude-review` job raised four items. Three were verified as real and fixed on this branch; one was a
framing correction to an already-filed follow-up.

1. **TOCTOU between the payload and its precondition** (correctness, fixed). Every mutator read the volatile
   document twice - once to build the payload, once to fingerprint the precondition - and `applyReplicated*`
   swaps that reference from the Raft apply thread *without* this monitor, by design. A swap landing between
   the two reads produced a payload built from the OLD document under a precondition describing the NEW one:
   the compare-and-set then PASSES and the stale payload installs, which is #7509 reopened on a window a few
   bytecodes wide. Fixed by reading the document once per attempt and deriving both halves from that snapshot:
   `snapshotWith(Map, ...)`, `usersFingerprintOf(Map)`, `groupsDocumentWith(JSONObject, ...)`, `groupsJsonOf`,
   and - for tokens, where the read lives behind another object's monitor -
   `ApiTokenConfiguration.MintedToken.documentBeforeJson()` and `DocumentChange(before, after)`, both produced
   inside the same `synchronized` block as the payload. Covered by the three
   `the*PreconditionFingerprintsTheDocumentTheSubmittedPayloadWasBuiltFrom` tests and by
   `Issue7509TokenDocumentPairAtomicityTest`.
2. **A corrupted precondition section failed OPEN** (correctness, fixed). `readSecurityPrecondition` wrapped
   both `readUTF` calls in one `try` returning `null`, so a section that identified itself as ours and was then
   unreadable degraded to "no precondition, install unconditionally" - the one default this field must never
   fall back to. The name read and the fingerprint read are now separate: an unreadable NAME is still an
   unrecognised section and is skipped (the #7138 contract), while a failure after the name matched is reported
   as corruption and reaches the caller as a `RaftLogEntryDecodeException`. Covered by
   `aCorruptedPreconditionSectionFailsTheEntryInsteadOfApplyingItUnconditionally` and
   `aSectionWhoseNameCannotBeReadIsStillSkipped`.
3. **The retry's catch-up wait was done under the shared monitor** (availability, fixed).
   `RaftHAPlugin.reportSecurityOutcome` called `waitForLocalApply()` - bounded by `arcadedb.ha.quorumTimeout`,
   10s by default - before returning to `ServerSecurity`, i.e. still inside `synchronized (this)`. Five attempts
   meant one caller could hold the monitor all seven mutators share for ~50s, queueing unrelated group and
   token changes behind a retry storm. The wait moved to `HAServerPlugin.awaitLocalApply()`, called from
   `ServerSecurity.awaitSupersededChange` OUTSIDE the monitor. Covered by
   `theCatchUpWaitBetweenRetriesHappensOutsideTheSharedMonitor`, which asserts with `Thread.holdsLock` that the
   wait happens and that the monitor is not held while it does.
4. **The #7559 framing undersells it** (accepted, no code change). The follower-submission gap is not only a
   rolling-upgrade transient: on a fully upgraded cluster any admin request a load balancer routes to a
   follower still gets the precondition withheld indefinitely. Recorded on #7559 and in *Residual risk* below.

Deliberately not changed, with reasons:

- **Caching the fingerprint of the document in force.** It would make `isSuperseded` O(1) instead of O(document
  size), but it adds a second piece of state that has to be invalidated on every path that installs a document
  - including the failure paths of `applyReplicated*` - and a stale cache there is a compare-and-set that
  passes when it should refuse. The cost it saves is one canonicalisation per security entry per node, and
  security entries are administration rather than traffic.
- **Extracting the seven retry loops into one helper.** Their early exits genuinely differ (`return`,
  `return false`, `return true`, `return response`, and two `throw`s on preconditions that are not the CAS), so
  the shared shape is the four lines around them. The dead `boolean applied = false;` initialisers the review
  flagged are gone; every one is now a definitely-assigned `final boolean`.

### Cycle 2 - `45965839e9`

No blocking issues: the reviewer re-verified the cycle-1 fixes against the source, including that the three
"in force" fingerprint helpers serialise the same way the submitter-side snapshot helpers do. Three
non-blocking observations:

1. **`SecurityDocumentFingerprint.appendString` escapes only `"` and `\`** - correct, but worth saying so
   explicitly. Applied: the javadoc now states that the canonical form is a COMPARISON KEY and not a
   serialization format, that it escapes only what could make two different documents produce the same bytes,
   and that the structural characters a JSON writer would escape cannot move a boundary inside a string this
   method delimits itself.
2. **Per-apply hashing cost** - the reviewer agreed with the cycle-1 decision not to cache, flagging it only as
   something to revisit if these documents ever grow to thousands of entries. No change.
3. **#7559** - agreed it is out of scope here and belongs to #7219/#7559. No change.

## Test results

- `server`: `com.arcadedb.server.security.*Test` - 109 tests, 0 failures (15 of them new in
  `Issue7509ConcurrentSecurityChangeTest`, 9 in `SecurityDocumentFingerprintTest`, 3 in
  `Issue7509TokenDocumentPairAtomicityTest`).
- `ha-raft`: 1411 tests, 2 failures - `ArcadeStateMachinePerDatabaseHaltTest.perDatabaseApplyErrorDoesNot
  TripNodeWideHalt` and `.otherDatabasesKeepApplyingAfterOneDatabaseFails`. **Both fail identically on
  unmodified `origin/main`** (verified in a pristine worktree: 1391 tests, the same 2 failures), and neither
  touches a security entry - they assert on a `TX_ENTRY` WAL decode message. Pre-existing, not a regression
  from this branch.
- Five `ha-raft` classes that stand up a real multi-node cluster (`LeaveClusterTest`, `DynamicMembershipTest`,
  `WaitForApplyTest`, `Issue6965SharedPageCrossNodeWritersTest`, `BaseCompactionIndexCompletenessTest`) could
  not run in this environment: port 2480 was held by another process, and they take the forked JVM down with
  them. `LeaveClusterTest` crashes the same way on pristine `main` here, so this is the environment and not
  the branch.
- **Proof the new tests can fail:** with `ServerSecurity.isSuperseded` forced to return `false` - the
  pre-#7509 unconditional apply - 10 of the 11 `Issue7509ConcurrentSecurityChangeTest` methods fail. The
  eleventh (`theApplyInstallsAnEntryThatCarriesNoPreconditionAtAll`) passes both ways by design: it pins the
  back-compatible path.
