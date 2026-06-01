# Issue #4446 - Configurable Raft Storage Directory

## Goal

Allow the parent directory of Raft storage folders to be configured via
`arcadedb.ha.raftStorageDirectory`, enabling Kubernetes deployments with
`readOnlyRootFilesystem` to mount a known, fixed path for Raft persistence.

## Problem

The Raft storage directory is hardcoded as:
`${arcadedb.server.rootPath}/raft-storage-<peerId>`

The `<peerId>` suffix is dynamic (derived from hostname and Raft port), making it
impossible to pre-declare a Kubernetes PVC mount for it. Users must mount the entire
server root on writable storage to accommodate it.

## Solution

Add a new `GlobalConfiguration` entry `HA_RAFT_STORAGE_DIRECTORY`
(key: `arcadedb.ha.raftStorageDirectory`) with an empty-string default.

When the setting is empty (default), existing behaviour is preserved:
`${rootPath}/raft-storage-<peerId>`.

When the setting is non-empty, Raft storage is created under the configured path:
`<configuredPath>/raft-storage-<peerId>`.

Kubernetes users can set `arcadedb.ha.raftStorageDirectory=/var/lib/arcadedb/raft`
and mount `/var/lib/arcadedb/raft` on a PVC.

## Affected Files

- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` - new config entry
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` - helper + 3 path usages
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseRaftHATest.java` - deleteDatabaseFolders cleanup
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftStorageDirectoryIT.java` - new regression test

## Verification

- New test `RaftStorageDirectoryIT`: PASS (1 test, 0 failures)
- Regression tests: `RaftBootstrapDoesNotEngageOnRestartIT`, `RaftBootstrapLateNewerJoinerIT`,
  `RaftReplicaCrashAndRecoverIT`: all PASS (3 tests, 0 failures)

## PR

https://github.com/ArcadeData/arcadedb/pull/4447

## Review Cycles

**Cycle 1** — head `75cb6b31e`
- gemini: 3 inline comments - use `File(parent,child)` constructor (all 3 sites), use `isBlank()` instead of `isEmpty()`
- claude: comment about GlobalConfig/ContextConfig split in test cleanup, recommend `isBlank()`, `@Tag("IntegrationTest")` (uncertain/deferred)
- Applied: `File(parent,child)` constructor in all 3 locations, `isBlank()`, added Javadoc warning to `deleteDatabaseFolders()`
- Deferred: `@Tag("IntegrationTest")` - unclear if intentional per project convention

**Cycle 2** — head `b04fba1c3`
- gemini: did not re-review (consistent with project history)
- claude: duplicate Javadoc block (must fix), `docs/review-deferred-75cb6b3.md` should be removed (must fix), add negative assertion to test (should fix), `@Tag("IntegrationTest")` (minor)
- Applied: removed duplicate Javadoc, removed deferred doc, added negative assertion in `RaftStorageDirectoryIT`
- Deferred: `@Tag("IntegrationTest")` - style decision for developer

**Cycle 3** — head `9aae58bdd`
- gemini: no comments (clean)
- claude: accepted all cycle 2 fixes; flagged `@Tag("slow")` as the correct tag (not `@Tag("IntegrationTest")`), per `RaftLeaderCrashWithExternalPropertyIT` etc.
- Applied: `@Tag("slow")` on `RaftStorageDirectoryIT`

**Cycle 4** — head `6c82cdca9`
- claude: accepted `@Tag("slow")`; minor - `RaftBootstrapLateNewerJoinerIT` still uses hardcoded path; "ready to merge after that is addressed"
- Applied: updated `RaftBootstrapLateNewerJoinerIT` cleanup to use `HA_RAFT_STORAGE_DIRECTORY` with fallback

**Final head:** `bbe624abb`
**Final state:** max-cycles-reached (4/4 cycles used, all actionable items addressed)

## Changes Made

### `GlobalConfiguration.java`
- Added `HA_RAFT_STORAGE_DIRECTORY` (`arcadedb.ha.raftStorageDirectory`) with empty-string default.

### `RaftHAServer.java`
- Added private `getRaftStorageDir()` helper that returns the configured parent directory
  (falls back to `arcadeServer.getRootPath()` when setting is empty).
- Replaced 3 hardcoded `new File(arcadeServer.getRootPath() + ...)` path constructions
  with `getRaftStorageDir()`.

### `BaseRaftHATest.java`
- Updated `deleteDatabaseFolders()` to use `HA_RAFT_STORAGE_DIRECTORY` when set,
  with null guard preventing NPE when `SERVER_ROOT_PATH` is not globally configured.

### `RaftStorageDirectoryIT.java`
- New IT class extending `BaseRaftHATest`.
- Configures `HA_RAFT_STORAGE_DIRECTORY=./target/custom-raft-storage` per server.
- Asserts that after cluster startup, `raft-storage-<peerId>` directories exist
  under the custom path.
