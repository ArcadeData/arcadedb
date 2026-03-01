# Issue #2946: Fix removeServer() Type Mismatch

**Issue URL**: https://github.com/ArcadeData/arcadedb/issues/2946
**Branch**: feature/2043-ha-test
**Priority**: P0 - Critical
**Phase**: Phase 1 - Fix Critical Bugs

## Overview

Fix type mismatch in `removeServer()` method that is incompatible with the new `ServerInfo` based replica connections map.

## Problem Description

The `removeServer()` method in `HAServer.java:749` still uses `String` parameter but `replicaConnections` has been migrated to `Map<ServerInfo, Leader2ReplicaNetworkExecutor>`, causing a type mismatch.

## Implementation Plan

1. **Analyze current code** - Understand how `removeServer()` is used
2. **Write tests** - Create tests for server removal scenarios
3. **Implement fix** - Update method signature to use `ServerInfo`
4. **Verify** - Ensure all tests pass

## Progress Log

### Step 1: Analysis and Test Creation
**Status**: Completed
**Started**: 2025-12-14
**Completed**: 2025-12-14

**Activities**:
- Analyzed the `removeServer()` method and its usage of `Map<ServerInfo, Leader2ReplicaNetworkExecutor>`
- Understood that `ServerInfo` is a record with `host`, `port`, and `alias` fields
- Added tests to `HAServerAliasResolutionTest.java` for ServerInfo lookup by exact match and by host:port

### Step 2: Implementation
**Status**: Completed
**Started**: 2025-12-14
**Completed**: 2025-12-14

**Changes Made**:

1. **Added helper method to HACluster** (`HAServer.java:155-170`)
   - `findByHostAndPort(String host, int port)` - finds ServerInfo by network address

2. **Updated removeServer() method** (`HAServer.java:839-891`)
   - Created primary method: `removeServer(ServerInfo serverInfo)` - accepts ServerInfo directly
   - Created compatibility method: `removeServer(String remoteServerName)` - accepts String and looks up ServerInfo by:
     1. Alias in cluster
     2. Matching alias or "host:port" in replicaConnections keys

3. **Fixed enum naming** (`BaseGraphServerTest.java`)
   - Changed `HAServer.SERVER_ROLE` to `HAServer.ServerRole` (PascalCase)
   - Changed `ArcadeDBServer.STATUS` to `ArcadeDBServer.Status` (PascalCase)
   - This aligns with Java naming conventions and the architectural changes in the feature branch

**Test Results**:
- All 9 tests in `HAServerAliasResolutionTest` pass
- New tests added for ServerInfo lookups
- Compilation successful after fixing enum naming

### Step 3: Verification
**Status**: Completed
**Started**: 2025-12-14
**Completed**: 2025-12-14

**Files Modified**:
1. `server/src/main/java/com/arcadedb/server/ha/HAServer.java`
   - Added `findByHostAndPort()` method to HACluster
   - Updated `removeServer()` with both ServerInfo and String signatures

2. `server/src/test/java/com/arcadedb/server/ha/HAServerAliasResolutionTest.java`
   - Added tests for ServerInfo lookup methods

3. `test-utils/src/main/java/com/arcadedb/test/BaseGraphServerTest.java`
   - Fixed enum naming: `SERVER_ROLE` → `ServerRole`
   - Fixed enum naming: `STATUS` → `Status`

## Summary

**Issue**: The `removeServer()` method in `HAServer.java` was still using `String` parameter while `replicaConnections` had been migrated to use `ServerInfo` as the key type.

**Solution**:
1. Created a new primary method `removeServer(ServerInfo serverInfo)` that uses ServerInfo directly
2. Created a backward-compatible `removeServer(String remoteServerName)` method that looks up the ServerInfo
3. Added `findByHostAndPort()` helper method to HACluster for ServerInfo lookups
4. Fixed enum naming conventions to align with Java standards (PascalCase)

**Test Coverage**:
- All existing tests in `HAServerAliasResolutionTest` pass (9/9)
- New tests added for ServerInfo lookup by exact match and by host:port

**Impact Analysis**:
- **Low Risk**: The changes are backward compatible - the String-based method is still available
- **Type Safety**: Using ServerInfo as the primary parameter improves type safety
- **Consistency**: Aligns with the ServerInfo migration across the HA subsystem

**Next Steps**:
This fix is part of Phase 1 (Fix Critical Bugs) from the HA_IMPROVEMENT_PLAN.md. After merging:
- Continue with other Phase 1 tasks (alias resolution, HTTP address propagation)
- Complete Phase 2: ServerInfo migration across all HA classes
- Enable resilience tests
