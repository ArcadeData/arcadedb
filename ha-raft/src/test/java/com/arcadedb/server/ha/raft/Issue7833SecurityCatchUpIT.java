/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7833: a node that comes back while it is still a Raft member was never re-seeded
 * with the cluster security documents.
 * <p>
 * Issue #7531 covers the pod that is ABSENT from the committed configuration and adds itself - the leader
 * notices the configuration change and seeds. A pod that restarts while it is still a member writes no
 * configuration entry at all, so no membership hook fires. That is the normal outcome of a StatefulSet rolling
 * restart with a retained PVC, of a node drain and reschedule, and of every pod whose ordinal is inside the
 * static {@code arcadedb.ha.serverList}. If the entries it missed are still in the leader's log it converges by
 * catch-up; if they were purged and it catches up by snapshot install it does not, because
 * {@code server-users.jsonl}, {@code server-groups.json} and {@code server-api-tokens.json} live under
 * {@code <server-root>/config/} and no snapshot carries them.
 *
 * <h2>How the state under test is reached</h2>
 * Not by restarting a node: a restart in this fixture converges on its own, because the entries are still in
 * the leader's retained log and Ratis replays them - which is exactly the benign half of the issue and would
 * make this test pass whether the fix were present or not. What is driven instead is the state that half leaves
 * behind: a follower holding an OLDER security document than the leader, with nothing left in the log to bring
 * it forward. The follower then asks the leader through the mechanism this issue adds, and converges.
 * <p>
 * Tagged {@code slow}: it starts a two-node cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue7833SecurityCatchUpIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  /**
   * The regression. A follower left holding a user document from before a cluster-wide change - the state a
   * snapshot-install catch-up leaves it in - asks the leader and converges, without anybody restarting it and
   * without a second seeder running anywhere.
   */
  @Test
  void aFollowerHoldingAStaleUserDocumentIsBroughtBackInStepByTheLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected first").isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final ServerSecurity leaderSecurity = getServer(leaderIndex).getSecurity();
    final ServerSecurity followerSecurity = getServer(followerIndex).getSecurity();

    // The document as it was before the change, kept so the follower can be put back into the state a
    // snapshot-install catch-up leaves it in.
    final String beforeTheChange = followerSecurity.getUsersJsonPayload();

    leaderSecurity.createUserClusterWide(new JSONObject()
        .put("name", "issue7833")
        .put("password", "ThisIsAStrongPassword1!")
        .put("databases", new JSONObject()));

    // Both nodes converge the ordinary way first - that is the baseline this test measures against.
    waitUntil(() -> followerSecurity.getUser("issue7833") != null,
        "the replicated user must reach the follower before the stale state is staged");

    // Now the state issue #7833 describes: this follower holds the documents it had when it went down, and
    // nothing left in the log will bring it forward.
    followerSecurity.applyReplicatedUsers(beforeTheChange);
    assertThat(followerSecurity.getUser("issue7833"))
        .as("the follower must really be out of step before the catch-up is asked for")
        .isNull();

    final List<String> failed = ClusterSecuritySeedQuery.seedForCatchUp(getServer(followerIndex),
        getRaftPlugin(followerIndex), "an Issue7833 regression test");

    assertThat(failed).as("the leader must have been able to seed every document").isEmpty();
    waitUntil(() -> followerSecurity.getUser("issue7833") != null,
        "the follower must converge on the cluster's user document after asking the leader");
  }

  /**
   * And the common case writes nothing. A rolling restart of a cluster whose security state did not change
   * while the pods were down must cost one round trip per pod and no Raft entries at all, or the mechanism
   * would put three entries in the log for every pod of every restart.
   */
  @Test
  void aFollowerThatIsAlreadyInStepCostsNoRaftEntry() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final long indexBefore = leader.getLastAppliedIndex();

    assertThat(ClusterSecuritySeedQuery.seedForCatchUp(getServer(followerIndex), getRaftPlugin(followerIndex),
        "an Issue7833 regression test")).isEmpty();

    // A cluster under no other load applies nothing else here, so the index standing still is the evidence that
    // the leader answered from the fingerprint comparison rather than by submitting the three documents.
    assertThat(leader.getLastAppliedIndex())
        .as("a node already in step must not make the leader replicate anything")
        .isEqualTo(indexBefore);
  }

  /**
   * The leader's own catch-up is a no-op rather than a dial to itself: it IS the reference every other node
   * compares against.
   */
  @Test
  void theLeaderAsksNobody() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    assertThat(ClusterSecuritySeedQuery.seedForCatchUp(getServer(leaderIndex), getRaftPlugin(leaderIndex),
        "an Issue7833 regression test"))
        .as("the leader seeds locally rather than dialling its own listener")
        .isEmpty();
  }

  /** Polls a condition to a generous deadline; a hang detector, never a latency bound. */
  private static void waitUntil(final java.util.function.BooleanSupplier condition, final String what)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000L;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean())
        return;
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting: " + what);
  }
}
