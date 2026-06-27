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

- `RaftPropertiesBuilderTest` (8 tests): PASS
- `RaftHAServerTest` (45 tests): PASS
- `RaftHAConfigurationIT` (5 tests): PASS
- `GlobalConfigurationTest` (10 tests): PASS

## PR

https://github.com/ArcadeData/arcadedb/pull/4753

## Review cycles

### Cycle 1 - HEAD 8b18894b

- gemini-code-assist (3 inline comments, 2 HIGH/1 MEDIUM): require `appendElementLimit > 0`,
  update description, add validation test.
- Verified gemini's claim that "0 freezes replication" against Ratis source
  (`DataQueue.offer` line 101: `if (elementLimit > 0 && q.size() >= elementLimit)`) - 0 is
  actually treated as "unlimited", so the literal claim is wrong. But the underlying advice is
  sound: an operator setting 0 would silently defeat the per-batch element bound, so the value
  should be a positive integer.
- Applied: added `if (appendElementLimit < 1) throw ConfigurationException` in
  `RaftPropertiesBuilder`, updated the config description, added zero/negative regression tests.

### Cycle 2 - HEAD 21be24b9

- claude review (on the cycle-1 SHA): essentially LGTM ("clean, well-scoped ... LGTM pending
  the description fix").
- Applied (clear & low-risk): corrected the PR description test count, added a config-doc note
  that the byte limit (`appendBufferSize`) is the dominant per-batch heap bound and the element
  count is the secondary cap, added a `ConfigurationException` test for the
  `writeBuffer < appendBuffer + 8` branch.

### Cycle 3 - HEAD 1c145183

- claude review: code is correct and above bar; no new actionable code change. Amplified the
  open question on the default value (see Open decision below) and added two non-blocking notes
  (no upper bound enforced; per-issue doc phrasing). Final state: deferred to the maintainer.

## Open decision for the maintainer

### Should the default be 64 or stay at 256?

Claude raised this in two reviews and it is the one decision worth settling before merge. The
element limit only binds AFTER the 4MB byte limit (`arcadedb.ha.appendBufferSize`). For the
incident workload (entries up to several MB of compressed WAL data), the 4MB byte cap truncates a
batch to ~1-2 entries long before 64 vs 256 ever matters - so in that regime the element-limit
default flip is a no-op for peak heap, while the lower default does cost healthy clusters extra
AppendEntries round-trips for small transactions.

Two defensible options:
- Keep the new default of 64 (conservative bound) - justified if the heap dump confirms the OOM
  was driven by many small entries (element count was the governing dimension).
- Revert the default to 256 (no behavior change) and ship only the tunable knob - the strictly
  safer change with no regression for the small-transaction case; operators who hit the OOM lower
  it themselves.

This requires the #4752 heap dump, which is not available here, so it is left for the maintainer
(the issue author). The knob and its validation are correct either way.

The fix makes the full trio of catch-up bounds operator-tunable:
- entry count: `arcadedb.ha.appendElementLimit` (currently default 64) - added by this PR
- byte limit: `arcadedb.ha.appendBufferSize` (default 4MB) - pre-existing
- gRPC inbound message size: `arcadedb.ha.grpcMessageSizeMax` (default 128MB) - pre-existing

If large *individual* entries (e.g. a 50k-vertex GraphBatch exceeding the byte limit) drove the
OOM, the more effective lever is lowering `arcadedb.ha.grpcMessageSizeMax` and/or bounding
transaction size: a single oversized entry bypasses the per-batch byte limit because `DataQueue`
admits the first element even when it exceeds the byte limit (to avoid deadlock).

### Minor (non-blocking)

- No upper bound is enforced on `appendElementLimit` (an operator could set a very large value
  and re-introduce the OOM). The byte limit remains the real ceiling; an explicit upper bound was
  not added because any chosen ceiling is itself a judgment call.
- Release note: if the default stays at 64, call out the change from 256 so operators tuning
  replication throughput are not surprised.
