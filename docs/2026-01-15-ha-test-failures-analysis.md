# HA Test Failures Analysis

**Date**: 2026-01-15
**Test Run**: Full HA test suite after 2-server cluster fix
**Overall Results**: 52/62 passing (~84%), 10 failures
**Branch**: feature/2043-ha-test

## Summary of Failures

### Category 1: Cluster Formation Timeouts (4 tests)

**Pattern**: Tests timeout waiting for cluster to stabilize

#### 1. ReplicationServerQuorumMajority1ServerOutIT
- **Error**: `ConditionTimeout` - Cluster didn't stabilize within 2 minutes
- **Details**: `waitForClusterStable` Lambda condition not fulfilled
- **Impact**: High - blocks quorum majority testing
- **Likely Cause**: Test infrastructure timing issue or production bug in quorum handling

#### 2. ReplicationServerQuorumMajority2ServersOutIT
- **Error**: `QuorumNotReached` - only 1 server online (needs 2)
- **Details**: Quorum 2 not reached because only 1 server online
- **Impact**: High - blocks quorum majority testing
- **Likely Cause**: Similar to #1, quorum calculation or cluster formation issue

#### 3. ReplicationServerReplicaRestartForceDbInstallIT
- **Error**: `ConditionTimeout` - Cluster didn't stabilize within 2 minutes
- **Details**: `waitForClusterStable` Lambda condition not fulfilled after replica restart
- **Impact**: Medium - specific to replica restart scenario
- **Likely Cause**: Force DB install may have timing issues during rejoin

#### 4. ReplicationServerWriteAgainstReplicaIT
- **Error**: `RuntimeException` - Cluster failed to stabilize (expected 3 servers, only 1 connected)
- **Details**: Failed during test startup in `waitAllReplicasAreConnected`
- **Impact**: High - 3-server cluster formation issue
- **Likely Cause**: Similar to 2-server race condition we just fixed, but for 3+ servers

### Category 2: Leader Failover Issues (2 tests)

**Pattern**: Tests fail during leader changes or failover scenarios

#### 5. ReplicationServerLeaderDownIT (2 errors)
- **Error 1**: `DatabaseIsClosedException: graph`
  - Occurs in `testReplication:109`
  - Database closed during test execution

- **Error 2**: `NeedRetry` - socket closed when sending command to leader
  - Error on sending command back to leader `{ArcadeDB_0}localhost:2424`
  - Cause: socket closed

- **Impact**: High - leader failover is critical HA functionality
- **Likely Cause**: Race condition during leader shutdown/restart or database lifecycle issue

#### 6. ReplicationServerLeaderChanges3TimesIT (1 error + 1 failure)
- **Error**: `ConditionTimeout` - Cluster didn't stabilize after leadership change
  - `waitForClusterStable` Lambda condition not fulfilled within 2 minutes

- **Failure**: Assertion failed - `Expecting value to be true but was false`
  - Line: testReplication:144

- **Impact**: High - multiple leadership changes should be supported
- **Likely Cause**: Cluster doesn't stabilize properly after repeated leader elections

### Category 3: Data Replication Issues (1 test)

**Pattern**: Data not fully replicated

#### 7. IndexCompactionReplicationIT.lsmVectorReplication
- **Error**: Assertion failure
  - Expected: 1001 entries
  - Actual: 74 entries

- **Details**:
  - Test inserts 5000 vector records
  - Leader index has 1001 entries (already incomplete - why not 5000?)
  - Replica has only 74 entries (massive replication gap)

- **Impact**: Medium - specific to LSM vector index replication
- **Likely Cause**:
  - LSM vector index not indexing all records on insert (asynchronous?)
  - Vector index replication incomplete/broken
  - Possible compaction race condition

### Category 4: Expected Failures (1 test)

#### 8. ReplicationServerFixedClientConnectionIT
- **Error**: `DatabaseIsClosedException: graph`
- **Status**: Test is @Disabled (lines 69-71)
- **Reason**: Degenerate case - MAJORITY quorum with 2 servers prevents leader election
- **Impact**: None - expected behavior for this edge case
- **Action**: No fix needed - test documents edge case limitation

## Failure Pattern Analysis

### Common Themes

1. **Cluster Stabilization Timeouts** (4 tests)
   - `waitForClusterStable` Lambda not fulfilled
   - Suggests either:
     - Test timeout too short
     - Actual production issue preventing stabilization
     - Missing condition that test is waiting for

2. **Database Lifecycle Issues** (2 tests)
   - `DatabaseIsClosedException` during failover
   - Suggests database not properly reopened after leader change

3. **Cluster Formation** (1 test)
   - 3-server cluster only gets 1 connection
   - Similar to 2-server race we just fixed

### Root Cause Hypotheses

**Hypothesis 1: Test Configuration Issue**
- Our faster retry configuration (200ms/2000ms/8 attempts) might be too aggressive for complex scenarios
- Cluster might need more time to stabilize in chaos scenarios (leader changes, restarts)
- **Test**: Increase timeouts for failing tests, see if they pass

**Hypothesis 2: Cluster Formation Race (3+ servers)**
- We fixed 2-server connectToLeader race
- Similar issue may exist for 3+ server scenarios
- **Test**: Check if synchronized connectToLeader helps 3-server cases

**Hypothesis 3: Leader Fence/Database Lifecycle**
- During leader failover, database closing/reopening may have race conditions
- `DatabaseIsClosedException` suggests database closed before expected
- **Test**: Examine leader fence and database lifecycle code

**Hypothesis 4: LSM Vector Index Replication**
- Vector indexes use different replication path than standard indexes
- Asynchronous indexing may not complete before replication
- **Test**: Check if LSM vector index replication is fully implemented

## Recommended Investigation Order

### Priority 1: Cluster Formation Timeout (ReplicationServerWriteAgainstReplicaIT)
**Why**: Failed at test startup, simplest to reproduce, affects 3-server clusters

**Steps**:
1. Run test in isolation with detailed logging
2. Check if synchronized connectToLeader helps
3. Examine why only 1/3 servers connected
4. Compare to passing 3-server tests (SimpleReplicationServerIT)

### Priority 2: LSM Vector Index (IndexCompactionReplicationIT)
**Why**: Specific assertion failure with clear numbers, isolated to one feature

**Steps**:
1. Check why leader has 1001 entries instead of 5000
2. Investigate LSM vector index async indexing
3. Check vector index replication messages
4. Compare to working LSM index replication tests

### Priority 3: Quorum Tests (QuorumMajority1ServerOutIT, QuorumMajority2ServersOutIT)
**Why**: Similar timeout pattern, may share root cause

**Steps**:
1. Examine quorum calculation logic
2. Check if tests are timing out too soon
3. Verify quorum requirements match test expectations
4. Look for edge cases in quorum handling

### Priority 4: Leader Failover (LeaderDownIT, LeaderChanges3TimesIT)
**Why**: Complex scenarios, may have multiple issues

**Steps**:
1. Investigate DatabaseIsClosedException during failover
2. Check leader fence logic
3. Examine database lifecycle during leadership transitions
4. Review leader election stability after multiple changes

### Priority 5: Replica Restart (ReplicaRestartForceDbInstallIT)
**Why**: Specific to one scenario, lower impact

**Steps**:
1. Check force DB install logic
2. Verify replica rejoin after restart
3. Examine timing issues during install

## Next Actions

1. ✅ Complete Phase 1 (Root Cause Investigation) - categorization done
2. 🔄 Start with Priority 1 test (ReplicationServerWriteAgainstReplicaIT)
3. Use systematic debugging process for each failure
4. Document findings and fixes
5. Re-run full suite after each fix to check for regressions

## Success Criteria

- Understand root cause of each failure (not just symptoms)
- Fix production bugs (not just test timing)
- Increase pass rate from 84% to 95%+
- No regressions in currently passing tests
