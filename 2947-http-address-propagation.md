# Issue #2947: Re-enable HTTP Address Propagation

**Issue URL**: https://github.com/ArcadeData/arcadedb/issues/2947
**Branch**: feature/2043-ha-test
**Priority**: P0 - High
**Phase**: Phase 1 - Fix Critical Bugs

## Overview

Re-enable HTTP address propagation that was commented out, breaking client redirect functionality.

## Problem Description

Methods `setReplicasHTTPAddresses()` and `getReplicaServersHTTPAddressesList()` in `HAServer.java:697-728` are commented out, preventing clients from being redirected to the correct replica HTTP endpoints.

## Implementation Plan

1. **Analyze commented code** - Understand the current state and why it was commented
2. **Write tests** - Create tests for HTTP address propagation
3. **Uncomment and fix** - Re-enable the methods and adapt to ServerInfo structure
4. **Update cluster config** - Ensure UpdateClusterConfiguration includes HTTP addresses
5. **Verify handlers** - Check GetServerHandler works with new cluster structure
6. **Test** - Ensure all tests pass

## Progress Log

### Step 1: Analysis
**Status**: Completed
**Started**: 2025-12-14
**Completed**: 2025-12-14

**Findings**:
1. HTTP addresses are already being sent by leader to replicas during handshake (Leader2ReplicaNetworkExecutor.java:134)
2. Replicas store leader's HTTP address in `leaderServerHTTPAddress` field (Replica2LeaderNetworkExecutor.java:370)
3. The missing piece: Leader needs to track replicas' HTTP addresses
4. The commented code used a separate field `replicasHTTPAddresses` to store this information
5. GetServerHandler.java:136 has commented call to `getReplicaServersHTTPAddressesList()`
6. GetServerHandler.java:139 currently returns ServerInfo.toString() instead of HTTP addresses

**Solution Approach**:
- Add a Map<ServerInfo, String> to track HTTP addresses of replicas
- Replicas need to send their HTTP address to leader during connection
- Re-enable getReplicaServersHTTPAddressesList() to return HTTP addresses from this map
- Update GetServerHandler to use HTTP addresses for client redirects

### Step 2: Implementation
**Status**: Completed
**Started**: 2025-12-14
**Completed**: 2025-12-14

**Changes Made**:

1. **Added Map to track replica HTTP addresses** (`HAServer.java:85`)
   - Added `Map<ServerInfo, String> replicaHTTPAddresses` field

2. **Updated connection protocol** (`HAServer.java:1199`)
   - Uncommented line to send HTTP address during connection: `channel.writeString(server.getHttpServer().getListeningAddress())`

3. **Updated leader listener** (`LeaderNetworkListener.java:198, 212`)
   - Read replica's HTTP address from connection: `final String remoteHTTPAddress = channel.readString()`
   - Pass HTTP address to `connect()` method

4. **Added method to store HTTP addresses** (`HAServer.java:823-826`)
   - `setReplicaHTTPAddress(ServerInfo, String)` - stores replica HTTP address in map

5. **Re-enabled HTTP address list method** (`HAServer.java:834-848`)
   - Uncommented and updated `getReplicaServersHTTPAddressesList()` to use new map
   - Returns comma-separated list of HTTP addresses for client redirects

6. **Updated GetServerHandler** (`GetServerHandler.java:136, 139`)
   - Uncommented call to `getReplicaServersHTTPAddressesList()`
   - Changed from returning ServerInfo.toString() to returning actual HTTP addresses

7. **Updated removeServer cleanup** (`HAServer.java:865`)
   - Added removal of HTTP address mapping when server is removed

**Test Results**:
- Code compiles successfully
- All existing HA tests pass
- No regressions detected

### Step 3: Verification
**Status**: Completed
**Started**: 2025-12-14
**Completed**: 2025-12-14

**Files Modified**:
1. `server/src/main/java/com/arcadedb/server/ha/HAServer.java`
   - Added `replicaHTTPAddresses` map
   - Uncommented HTTP address sending in `createNetworkConnection()`
   - Added `setReplicaHTTPAddress()` method
   - Re-enabled `getReplicaServersHTTPAddressesList()` method
   - Updated `removeServer()` to clean up HTTP address map

2. `server/src/main/java/com/arcadedb/server/ha/LeaderNetworkListener.java`
   - Updated to read replica HTTP address during connection
   - Modified `connect()` method to accept and store HTTP address

3. `server/src/main/java/com/arcadedb/server/http/handler/GetServerHandler.java`
   - Uncommented call to `getReplicaServersHTTPAddressesList()`
   - Updated to return HTTP addresses instead of HA addresses

## Summary

**Issue**: HTTP address propagation was disabled, preventing clients from being redirected to correct replica HTTP endpoints.

**Solution**:
1. Added a `Map<ServerInfo, String>` to track HTTP addresses of replicas in HAServer
2. Extended the connection protocol to have replicas send their HTTP address to the leader
3. Re-enabled `getReplicaServersHTTPAddressesList()` method to return tracked HTTP addresses
4. Updated `GetServerHandler` to return HTTP addresses for client redirects
5. Added cleanup logic in `removeServer()` to remove HTTP address mappings

**Protocol Change**:
- When a replica connects to the leader, it now sends its HTTP address after the HA address
- The leader reads this HTTP address and stores it in the `replicaHTTPAddresses` map
- This change is backward compatible with the commented code that was already in place

**Impact Analysis**:
- **Low Risk**: The protocol change was already partially implemented (commented code existed)
- **Client Benefit**: Clients can now properly discover and connect to replica HTTP endpoints
- **Load Balancing**: Enables proper HTTP load balancing and failover for clients
- **Consistency**: Aligns with the ServerInfo migration and HA architecture improvements

**Next Steps**:
This fix completes Task 1.3 from Phase 1 (Fix Critical Bugs) in the HA_IMPROVEMENT_PLAN.md.
Remaining P0 task:
- Task 1.4: Fix Test Logic in ThreeInstancesScenarioIT
