# Fix #4274: Cluster topology inconsistency across nodes

## Root Cause

`RaftHAServer.getReplicaAddresses()` excluded `localPeerId` (the node serving the HTTP request)
from the replica list, instead of excluding the **leader**.

On a follower node this caused two anomalies:
1. The leader appeared inside `replicaAddresses` (it was not the local peer, so it was included).
2. The follower itself was missing from `replicaAddresses` (it was the local peer, so it was excluded).

The leader node happened to produce the correct view because `localPeerId == leaderId` there.

## Fix

Changed `getReplicaAddresses()` to exclude the leader ID. When no leader is known (election in
progress), it falls back to excluding `localPeerId` to preserve prior behavior.

File: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

## Regression Test

Added `clusterTopologyIsConsistentAcrossNodes` to `RaftHTTP2ServersIT`. The test queries every
node's `?mode=cluster` endpoint and asserts:
- `leaderAddress` is identical across all responses.
- `replicaAddresses` does not contain the `leaderAddress` (leader was never a replica).
- `replicaAddresses` is identical across all responses.

## Test Results

All 6 tests in `RaftHTTP2ServersIT` pass (including the new regression test).
