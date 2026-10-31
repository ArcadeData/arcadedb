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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two-node sibling of {@link HaSecurityMutationRoutingIT}, on ONE cluster shared by every test
 * ({@link SharedClusterRaftHATest}). Merged from {@code Issue7833SecurityCatchUpIT},
 * {@code Issue8034SecurityCatchUpRearmIT} (one of two tests) and {@code Issue7549FollowerCapabilityReportingIT}, which
 * ran on the same fixture: two servers, no configuration override.
 * <ul>
 * <li><b>#7833</b>: a node that comes back while it is still a Raft member was never re-seeded with the cluster
 * security documents. A pod that restarts as a member (a StatefulSet rolling restart with a retained PVC, a drain and
 * reschedule, any ordinal inside the static {@code arcadedb.ha.serverList}) writes no configuration entry, so no
 * membership hook fires (#7531 covers the pod absent from the configuration). If it catches up by snapshot install it
 * does not converge, because {@code server-users.jsonl}, {@code server-groups.json} and {@code server-api-tokens.json}
 * live under {@code <server-root>/config/} and no snapshot carries them. The state is reached not by restarting a node
 * (which converges by log replay, the benign half, and would pass with or without the fix) but by staging what the other
 * half leaves behind: a follower holding an OLDER document than the leader with nothing left in the log; it asks the
 * leader through {@link ClusterSecuritySeedQuery} and converges.</li>
 * <li><b>#8034</b>: the security catch-up must not spend its once-per-start request on an attempt that asked nobody.
 * {@code SecurityCatchUp} takes the request on submission and then delays; leadership moving to this node in that window
 * lands the attempt on the arm that answers "I am the reference", which used to keep the request spent, so the node never
 * asked the next leader it observed. The test here is the {@code afterSnapshotInstall} trigger; the leader-change half is
 * in {@link HaSecurityMutationRoutingDisruptiveIT}. What the leader does with a request is #7833's subject.</li>
 * <li><b>#7549</b>: {@code GET /api/v1/cluster} reports each peer's advertised capabilities from EVERY node, so "may I
 * make a group change now" is answerable without locating the leader. The capability monitor used to run only on the
 * leader ({@code startLagMonitor}), and neither this endpoint nor the security routes #7511 gated on the advertisement
 * forward, so a follower answered peer rows with neither {@code capabilities} nor {@code capabilitiesUnknownReason}.</li>
 * </ul>
 * Why these share a cluster, and why no order is pinned:
 * <ul>
 * <li>#7833 does NOT depend on the once-per-start latch: {@link ClusterSecuritySeedQuery#seedForCatchUp} is the dial
 * itself, below {@code SecurityCatchUp}. Its first test stages a stale user document on the follower and ends once the
 * leader's re-seed brought it back; the re-seed replaces the whole document, so the follower ends in step. Its
 * "already in step costs no entry" test reads the leader's log index after the health check of
 * {@link SharedClusterRaftHATest} has waited for every replica to apply, so an earlier test's seed lies before it, and
 * it counts only SECURITY entries after it.</li>
 * <li>#8034's test puts the latch back to its at-start value itself ({@code rearmForTests()}) before it drives the
 * trigger, so it does not need a fresh node; it leaves the leader's latch free, which nothing here reads, and its
 * attempt on the leader asks nobody and writes nothing.</li>
 * <li>#7549 only reads the cluster endpoint of the follower.</li>
 * </ul>
 * The user #7833 creates is named through {@link #uniqueName(String)}.
 * <p>
 * The tests tagged {@code slow} were tagged so in their source class; #7549's was not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaSecurityMutationRoutingTwoNodeIT extends SharedClusterRaftHATest {

  /** #8034: a hang detector for the polls below, not a latency bound: nothing here is timed. */
  private static final long WAIT_BUDGET_8034_MS = 60_000L;

  /** #7833: a hang detector for the polls below, not a latency bound. */
  private static final long WAIT_BUDGET_7833_MS = 30_000L;

  @Override
  protected int getServerCount() {
    return 2;
  }

  // ------------------------------------------------------------------ security catch-up (#7833)

  /**
   * Issue #7833, from {@code Issue7833SecurityCatchUpIT}.
   * <p>
   * The regression. A follower left holding a user document from before a cluster-wide change - the state a
   * snapshot-install catch-up leaves it in - asks the leader and converges, without anybody restarting it and without a
   * second seeder running anywhere.
   */
  @Tag("slow")
  @Test
  void aFollowerHoldingAStaleUserDocumentIsBroughtBackInStepByTheLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected first").isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final ServerSecurity leaderSecurity = getServer(leaderIndex).getSecurity();
    final ServerSecurity followerSecurity = getServer(followerIndex).getSecurity();
    final String user = uniqueName("issue7833");

    // The document as it was before the change, kept so the follower can be put back into the state a
    // snapshot-install catch-up leaves it in.
    final String beforeTheChange = followerSecurity.getUsersJsonPayload();

    leaderSecurity.createUserClusterWide(new JSONObject()
        .put("name", user)
        .put("password", "ThisIsAStrongPassword1!")
        .put("databases", new JSONObject()));

    // Both nodes converge the ordinary way first - that is the baseline this test measures against.
    waitUntil(() -> followerSecurity.getUser(user) != null, WAIT_BUDGET_7833_MS,
        "the replicated user must reach the follower before the stale state is staged");

    // Now the state issue #7833 describes: this follower holds the documents it had when it went down, and
    // nothing left in the log will bring it forward.
    followerSecurity.applyReplicatedUsers(beforeTheChange);
    assertThat(followerSecurity.getUser(user))
        .as("the follower must really be out of step before the catch-up is asked for")
        .isNull();

    final List<String> failed = ClusterSecuritySeedQuery.seedForCatchUp(getServer(followerIndex),
        getRaftPlugin(followerIndex), "an Issue7833 regression test");

    assertThat(failed).as("the leader must have been able to seed every document").isEmpty();
    waitUntil(() -> followerSecurity.getUser(user) != null, WAIT_BUDGET_7833_MS,
        "the follower must converge on the cluster's user document after asking the leader");
  }

  /**
   * Issue #7833, from {@code Issue7833SecurityCatchUpIT}.
   * <p>
   * And the common case writes nothing. A rolling restart of a cluster whose security state did not change while the
   * pods were down must cost one round trip per pod and no Raft entries at all, or the mechanism would put three entries
   * in the log for every pod of every restart.
   */
  @Tag("slow")
  @Test
  void aFollowerThatIsAlreadyInStepCostsNoRaftEntry() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final long indexBefore = lastRaftLogIndex(leader);

    assertThat(ClusterSecuritySeedQuery.seedForCatchUp(getServer(followerIndex), getRaftPlugin(followerIndex),
        "an Issue7833 regression test")).isEmpty();

    // No security entry past the position the leader had reached is the evidence that it answered from the
    // fingerprint comparison rather than by submitting the three documents. Read by entry type, not by the raw
    // index: the cluster can commit entries of its own in the same window (issue #8455).
    assertThat(securityEntriesInLogAfter(leader, indexBefore))
        .as("a node already in step must not make the leader replicate anything")
        .isEmpty();
  }

  /**
   * Issue #7833, from {@code Issue7833SecurityCatchUpIT}.
   * <p>
   * The leader's own catch-up is a no-op rather than a dial to itself: it IS the reference every other node compares
   * against.
   */
  @Tag("slow")
  @Test
  void theLeaderAsksNobody() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    assertThat(ClusterSecuritySeedQuery.seedForCatchUp(getServer(leaderIndex), getRaftPlugin(leaderIndex),
        "an Issue7833 regression test"))
        .as("the leader seeds locally rather than dialling its own listener")
        .isEmpty();
  }

  // ------------------------------------------------------------------ catch-up request re-arm (#8034)

  /**
   * Issue #8034, from {@code Issue8034SecurityCatchUpRearmIT}.
   * <p>
   * The leader arm, reached from the snapshot-install trigger. {@code afterSnapshotInstall} sets the once-per-start
   * request up front and then submits, so a snapshot install whose follow-up request lands on a node that has since won
   * the election used to consume the request as well as its own.
   */
  @Tag("slow")
  @Test
  void aSnapshotInstallFollowUpThatLedInsteadOfAskingReleasesTheRequestToo() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected first").isGreaterThanOrEqualTo(0);

    final SecurityCatchUp catchUp = getRaftPlugin(leaderIndex).getRaftHAServer().getStateMachine().getSecurityCatchUp();
    catchUp.rearmForTests();

    catchUp.afterSnapshotInstall(getServer(leaderIndex), getRaftPlugin(leaderIndex).getRaftHAServer());

    waitUntil(() -> !catchUp.hasRequestedSinceStart(), WAIT_BUDGET_8034_MS,
        "a snapshot-install follow-up that found this node leading asked nobody, so it must not spend the "
            + "once-per-start request either");
  }

  // ------------------------------------------------------------------ capability reporting on a follower (#7549)

  /**
   * Issue #7549, from {@code Issue7549FollowerCapabilityReportingIT}.
   * <p>
   * Polled on the FOLLOWER, which is the node the endpoint used to have nothing to say about anyone else on. Every row is
   * asserted, the follower's own included: its own is rendered from what this node advertises about itself (it never
   * probes itself), and the leader's row is the one that needs a probe round to exist - which is exactly the round a
   * follower did not run.
   */
  @Test
  void aFollowerReportsTheCapabilitiesOfEveryPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    // The first refresh round runs with no initial delay, but it still has to complete a probe per peer.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      final JSONObject response = queryClusterEndpoint(followerIndex);
      assertThat(response.getBoolean("isLeader")).as("this is the follower").isFalse();

      final JSONArray peers = response.getJSONArray("peers");
      assertThat(peers.length()).isEqualTo(2);
      for (int i = 0; i < peers.length(); i++) {
        final JSONObject peer = peers.getJSONObject(i);
        assertThat(peer.has("capabilities"))
            .as("a follower knows what peer '%s' advertises, without anyone having to find the leader first",
                peer.getString("id"))
            .isTrue();
      }
    });
  }

  // ---------------------------------------------------------------------------------------------

  private JSONObject queryClusterEndpoint(final int serverIndex) throws Exception {
    final URL url = new URL(getServerHttpUrl(serverIndex, "/api/v1/cluster"));
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }

  /** Polls a condition to a generous deadline; a hang detector, never a latency bound. */
  static void waitUntil(final BooleanSupplier condition, final long budgetMs, final String what)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + budgetMs;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean())
        return;
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting: " + what);
  }
}
