# Fix #4752: bound AppendEntries catch-up batch size

## Issue Summary

On a 3-node Raft HA cluster, a follower OOMs while parsing an inbound `AppendEntriesRequestProto`
during catch-up resync. The Raft batch shipped by the leader during catch-up is not bounded
conservatively enough: during log replay from a large database, enough entries queue in the
follower's in-memory log that combined heap usage exceeds the JVM `-Xmx` limit. The follower
then dies, stops resolving on the network, and OOMs again on restart (loop).

## Root Cause Analysis

Two contributing factors:

1. `bufferElementLimit` was hardcoded at `256`. During catch-up the leader sends batches of up
   to 256 log entries per AppendEntries. These entries buffer in the follower's in-memory Raft
   log waiting for the state machine to apply them. With many large entries in flight (each up to
   several MB of compressed WAL data), the combined in-memory footprint of buffered entries can
   push the follower into OOM.

2. There was no operator knob to tune the per-batch entry count. Operators could adjust bytes
   via `HA_APPEND_BUFFER_SIZE` but not the entry count.

## Fix

Add `HA_APPEND_ELEMENT_LIMIT` (`arcadedb.ha.appendElementLimit`) to `GlobalConfiguration`:
- Default: `64` (reduced from hardcoded `256`)
- Wired into `RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, limit)`
  in `RaftPropertiesBuilder`

Reducing the default from 256 to 64 means each AppendEntries batch carries at most 64 log
entries. Combined with the existing `bufferByteLimit` (4 MB default), the per-batch in-memory
footprint is bounded on both dimensions. Operators with fast catch-up requirements can raise
`arcadedb.ha.appendElementLimit` to allow larger batches.

## Verification

Unit test: `RaftPropertiesBuilderTest`
- Default configuration produces `bufferElementLimit = 64`
- Custom configuration value is reflected
- Buffer byte limit, gRPC message size, and element limit all verify correctly

## Files Changed

- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` - new `HA_APPEND_ELEMENT_LIMIT`
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPropertiesBuilder.java` - use config
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftPropertiesBuilderTest.java` - new test

## Test Results

- `RaftPropertiesBuilderTest` (5 tests): PASS
- `RaftHAServerTest` (45 tests): PASS
- `RaftHAConfigurationIT` (5 tests): PASS
- `GlobalConfigurationTest` (10 tests): PASS
