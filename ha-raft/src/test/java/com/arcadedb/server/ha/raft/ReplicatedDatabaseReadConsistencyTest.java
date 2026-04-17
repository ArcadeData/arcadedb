/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.HAPlugin;
import com.arcadedb.server.ReadConsistencyContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the mapping from (role, consistency, bookmark) to the correct HAPlugin barrier.
 *
 * <p>The follower LINEARIZABLE-without-bookmark case is the most important: it must invoke
 * {@link HAPlugin#ensureLinearizableFollowerRead()} (a ReadIndex round-trip to the leader) and
 * NOT fall through to {@link HAPlugin#waitForLocalApply()}, which would only wait for the
 * follower's own view of the commit index and could serve stale reads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReplicatedDatabaseReadConsistencyTest {

  /** HAPlugin implementation that records which barrier methods are called. */
  private static final class RecordingHAPlugin implements HAPlugin {
    final List<String> calls = new ArrayList<>();

    @Override public void waitForAppliedIndex(final long targetIndex) { calls.add("waitForAppliedIndex:" + targetIndex); }
    @Override public void ensureLinearizableRead()         { calls.add("ensureLinearizableRead"); }
    @Override public void ensureLinearizableFollowerRead() { calls.add("ensureLinearizableFollowerRead"); }
    @Override public void waitForLocalApply()              { calls.add("waitForLocalApply"); }

    // Abstract HAPlugin methods - the barrier-mapping logic never calls these, so the test
    // just needs them present to satisfy the interface.
    @Override public boolean isLeader() { return false; }
    @Override public String getClusterToken() { return null; }
    @Override public long getCommitIndex() { return 0; }
    @Override public String getLeaderHTTPAddress() { return null; }
    @Override public String getLeaderName() { return null; }
    @Override public String getElectionStatus() { return null; }
    @Override public String getClusterName() { return null; }
    @Override public int getConfiguredServers() { return 0; }
    @Override public String getReplicaAddresses() { return null; }
    @Override public String getServerName() { return null; }
    @Override public long getLastAppliedIndex() { return 0; }
    @Override public JSONObject exportClusterStatus() { return new JSONObject(); }
    @Override public void replicateCreateDatabase(final String databaseName) { }
    @Override public void replicateDropDatabase(final String databaseName) { }
    @Override public void replicateCreateUser(final String userJson) { }
    @Override public void replicateUpdateUser(final String userJson) { }
    @Override public void replicateDropUser(final String userName) { }
    @Override public void startService() { }
  }

  private static ReadConsistencyContext ctx(final Database.READ_CONSISTENCY consistency, final long bookmark) throws Exception {
    // ReadConsistencyContext has a private constructor; use reflection so the test does not
    // have to install a ThreadLocal (the static `get()` snapshot is not what we want to assert
    // against anyway - the barrier method takes the context explicitly).
    final Constructor<ReadConsistencyContext> c = ReadConsistencyContext.class.getDeclaredConstructor(
        Database.READ_CONSISTENCY.class, long.class);
    c.setAccessible(true);
    return c.newInstance(consistency, bookmark);
  }

  // -- Leader paths --

  @Test
  void leaderLinearizableInvokesLeaderReadIndex() throws Exception {
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    ReplicatedDatabase.applyReadConsistencyBarrier(ha, ctx(Database.READ_CONSISTENCY.LINEARIZABLE, -1), true);
    assertThat(ha.calls).containsExactly("ensureLinearizableRead");
  }

  @Test
  void leaderReadYourWritesInvokesLocalApplyBarrier() throws Exception {
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    ReplicatedDatabase.applyReadConsistencyBarrier(ha, ctx(Database.READ_CONSISTENCY.READ_YOUR_WRITES, -1), true);
    assertThat(ha.calls).containsExactly("waitForLocalApply");
  }

  @Test
  void leaderEventualInvokesLocalApplyBarrier() throws Exception {
    // EVENTUAL on the leader still hits the default barrier so the leader never serves a read
    // from a state older than its own committed entries.
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    ReplicatedDatabase.applyReadConsistencyBarrier(ha, ctx(Database.READ_CONSISTENCY.EVENTUAL, -1), true);
    assertThat(ha.calls).containsExactly("waitForLocalApply");
  }

  // -- Follower paths --

  @Test
  void followerEventualDoesNothing() throws Exception {
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    ReplicatedDatabase.applyReadConsistencyBarrier(ha, ctx(Database.READ_CONSISTENCY.EVENTUAL, -1), false);
    assertThat(ha.calls).isEmpty();
  }

  @Test
  void followerWithoutContextDoesNothing() {
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    ReplicatedDatabase.applyReadConsistencyBarrier(ha, null, false);
    assertThat(ha.calls).isEmpty();
  }

  @Test
  void followerReadYourWritesWithBookmarkWaitsForIndex() throws Exception {
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    ReplicatedDatabase.applyReadConsistencyBarrier(ha, ctx(Database.READ_CONSISTENCY.READ_YOUR_WRITES, 42L), false);
    assertThat(ha.calls).containsExactly("waitForAppliedIndex:42");
  }

  @Test
  void followerReadYourWritesWithoutBookmarkFallsBackToLocalApply() throws Exception {
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    ReplicatedDatabase.applyReadConsistencyBarrier(ha, ctx(Database.READ_CONSISTENCY.READ_YOUR_WRITES, -1), false);
    assertThat(ha.calls).containsExactly("waitForLocalApply");
  }

  @Test
  void followerLinearizableWithBookmarkWaitsForIndex() throws Exception {
    // With a bookmark, LINEARIZABLE degenerates to a local apply wait because the bookmark
    // already names the minimum committed index the reader must observe.
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    ReplicatedDatabase.applyReadConsistencyBarrier(ha, ctx(Database.READ_CONSISTENCY.LINEARIZABLE, 99L), false);
    assertThat(ha.calls).containsExactly("waitForAppliedIndex:99");
  }

  @Test
  void followerLinearizableWithoutBookmarkIssuesReadIndexRpc() throws Exception {
    // Regression guard for the previously-documented limitation: without a bookmark, the
    // follower MUST issue a ReadIndex RPC to the leader, not fall back to waitForLocalApply
    // (which would only wait for the follower's own view of commitIndex).
    final RecordingHAPlugin ha = new RecordingHAPlugin();
    ReplicatedDatabase.applyReadConsistencyBarrier(ha, ctx(Database.READ_CONSISTENCY.LINEARIZABLE, -1), false);
    assertThat(ha.calls).containsExactly("ensureLinearizableFollowerRead");
  }
}
