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
