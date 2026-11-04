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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Leadership handoff on a real 3-node cluster: the no-target and targeted {@code transferLeadership}, the
 * {@code POST /api/v1/cluster/leader} endpoint, and the paths that move leadership off a leader that must not keep it.
 * Merged from {@code Issue4809NoTargetTransferLeadershipIT}, {@code Issue5276TransferLeaderValidationIT},
 * {@code Issue8480NoTargetTransferIsARealHandoffIT}, {@code Issue8533TargetedTransferUnderWritesWinsIT},
 * {@code Issue8491LeaderReplacingDatabaseHandsOffIT}, {@code Issue8592LeaderLeavesClusterIT} and
 * {@code RaftTransferLeadershipIT}, which ran on the same fixture: three servers, the base configuration (health monitor
 * off, volatile Raft storage). The sources of #5276 and {@code RaftTransferLeadershipIT} carried no tag and the others
 * {@code @Tag("slow")} at class level, so the tag is per method here and every test stays in its lane. The quarantined
 * leader of #8483 runs on two servers with the health monitor on and stays in
 * {@code Issue8483QuarantinedLeaderHandsOffLeadershipIT}.
 * <ul>
 * <li><b>#4809</b>: the no-target {@code transferLeadership(timeoutMs)} reported success whenever this node simply stopped
 * being the leader, conflating a real handoff with a leaderless window or an unrelated election. It now requires leadership
 * to settle on a concrete, DIFFERENT peer, and refuses on a node that is not the leader.</li>
 * <li><b>#5276</b>: request validation and honest outcome reporting on {@code POST /api/v1/cluster/leader}: a missing body
 * is a 400 with a usage message (not a 500 from an NPE), an unrecognized field ({@code "target"} instead of
 * {@code "peerId"}) is a 400 naming it instead of a silent transfer-to-any, and a successful targeted transfer reports the
 * confirmed leader.</li>
 * <li><b>#8480</b>: the no-target transfer returned {@code true} as soon as Ratis had stepped the leader down at the same
 * term, with no leader at all, followers still naming the ex-leader for an election timeout, and the election re-electing
 * the same node 7 times out of 8. A {@code true} now means leadership is already on another node and the cluster agrees.</li>
 * <li><b>#8533</b> (with the end-to-end guard of #8487): a TARGETED transfer while every node writes must hand leadership to
 * the target, every time. The target could lose the election it was sent to win: when its acknowledgement completed the
 * majority for the leader's last data entry D, Ratis started the election with {@code lastEntry = D} and only then appended
 * the commit-index metadata entry D+1, which reached the other follower first, and both voters rejected a candidate one
 * entry short. How a failed transfer names the outcome is covered by {@link Issue8487TargetedTransferSettlesTest}.</li>
 * <li><b>#8491</b>: a node replacing a database with the leader's copy (an operator resync) can become the leader, because
 * Raft elects on the log alone. As leader it cannot download the copy from itself and refuses every request on the
 * database meanwhile. It must report the condition and hand leadership to a peer; the install then completes.</li>
 * <li><b>#8592</b>: the leader leaving goes through the shared hand-off (the ranked candidates of
 * {@link RaftHAServer#selectStepDownTargets}, no bare step-down) and the removal; the two nodes left keep a leader and a
 * configuration without it.</li>
 * <li>{@code RaftTransferLeadershipIT}: the basic targeted transfer through the HTTP endpoint, after which the target takes
 * writes that replicate.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaLeadershipTransferIT extends BaseRaftHATest {

  private static final int NO_TARGET_TRANSFER_ROUNDS = 3;
  private static final int TARGETED_TRANSFER_ROUNDS  = 12;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void clearHook() {
    SnapshotInstaller.snapshotStagedForTesting = null;
  }

  /**
   * Issue #4809, from {@code Issue4809NoTargetTransferLeadershipIT}: a genuine no-target transfer issued on the leader
   * must report success; the fix's settled-different-leader confirmation must not introduce a false negative on a real
   * handoff.
   */
  @Test
  @Tag("slow")
  void noTargetTransferOnLeaderSucceeds() {
    final int leaderIndex = waitForLeaderIndex(30_000);
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final boolean transferred = getRaftPlugin(leaderIndex).getRaftHAServer().transferLeadership(15_000);
    assertThat(transferred)
        .as("A genuine no-target transfer on the leader must report success (confirmed handoff to a different peer)")
        .isTrue();
  }

  /**
   * Issue #4809, from {@code Issue4809NoTargetTransferLeadershipIT}: the core false-positive fix. A node that is not the
   * leader has nothing to hand off, so the no-target transfer must report failure and must not trigger any handoff on the
   * real leader. Before the fix, the {@code !isLeader()} heuristic could report success for such a call.
   */
  @Test
  @Tag("slow")
  void transferOnNonLeaderReturnsFalseWithoutChangingLeader() throws Exception {
    final int leaderIndex = waitForLeaderIndex(30_000);
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex && getRaftPlugin(i) != null) {
        followerIndex = i;
        break;
      }
    }
    assertThat(followerIndex).as("A follower must exist").isGreaterThanOrEqualTo(0);

    final boolean result = getRaftPlugin(followerIndex).getRaftHAServer().transferLeadership(5_000);
    assertThat(result).as("Transfer requested on a non-leader must return false, not a false positive").isFalse();

    // The rejected request must change nothing: the original leader is still the leader.
    Thread.sleep(2_000);
    assertThat(findLeaderIndex())
        .as("Leadership must be unchanged after a rejected no-target transfer on a follower")
        .isEqualTo(leaderIndex);
  }

  /** Issue #5276, from {@code Issue5276TransferLeaderValidationIT}. */
  @Test
  void invalidRequestsReturn400WithoutTransferring() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    // 1) Missing body: must be a 400 with a usage hint, not a 500 NPE.
    final HttpResult noBody = postTransferLeader(leaderIndex, null);
    assertThat(noBody.code()).as("Response: %s", noBody.body()).isEqualTo(400);
    assertThat(noBody.body()).contains("peerId");

    // 2) Unknown field ("target" instead of "peerId"): must be rejected loudly, NOT silently
    // ignored (which executed a transfer-to-any and reported success - issue #5276).
    final String unknownField = new JSONObject().put("target", peerIdForIndex(leaderIndex == 0 ? 1 : 0)).toString();
    final HttpResult badField = postTransferLeader(leaderIndex, unknownField);
    assertThat(badField.code()).as("Response: %s", badField.body()).isEqualTo(400);
    assertThat(badField.body()).contains("target").contains("peerId");

    // Neither invalid request may have moved leadership.
    assertThat(findLeaderIndex()).isEqualTo(leaderIndex);
  }

  /** Issue #5276, from {@code Issue5276TransferLeaderValidationIT}. */
  @Test
  void targetedTransferReportsConfirmedLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int targetIndex = leaderIndex == 0 ? 1 : 0;
    final String targetPeerId = peerIdForIndex(targetIndex);

    final String body = new JSONObject().put("peerId", targetPeerId).put("timeoutMs", 10_000).toString();
    final HttpResult result = postTransferLeader(leaderIndex, body);
    assertThat(result.code()).as("Response: %s", result.body()).isEqualTo(200);

    // The response must report the confirmed leader, so callers can verify the outcome instead of
    // trusting a bare success string (issue #5276).
    final JSONObject response = new JSONObject(result.body());
    assertThat(response.getString("leaderId", "")).isEqualTo(targetPeerId);

    // And leadership must actually be on the target.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(findLeaderIndex()).isEqualTo(targetIndex));
  }

  /** Issue #8480, from {@code Issue8480NoTargetTransferIsARealHandoffIT}. */
  @Test
  @Tag("slow")
  void aNoTargetTransferHandsLeadershipToAnotherNode() {
    for (int round = 0; round < NO_TARGET_TRANSFER_ROUNDS; round++) {
      final int exLeaderIndex = findLeaderIndex();
      assertThat(exLeaderIndex).as("round %d: a Raft leader must be elected", round).isGreaterThanOrEqualTo(0);
      final RaftHAServer exLeader = getRaftPlugin(exLeaderIndex).getRaftHAServer();
      final RaftPeerId exLeaderId = exLeader.getLocalPeerId();

      assertThat(exLeader.transferLeadership(10_000))
          .as("round %d: the no-target transfer must report a handoff", round).isTrue();

      // Checked the instant the call returns. The bare step-down it used to be left this null (no leader) here.
      final RaftPeerId newLeaderId = exLeader.getLeaderId();
      assertThat(newLeaderId).as("round %d: a leader must already be in place when the transfer returns true", round)
          .isNotNull();
      assertThat(newLeaderId).as("round %d: leadership must have moved to another node", round).isNotEqualTo(exLeaderId);

      // Every node converges on that same leader: the one the transfer handed off to, not an election that may pick
      // the ex-leader again.
      final int round0 = round;
      Awaitility.await("round " + round0 + ": every node names the new leader")
          .atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS).until(() -> {
            for (int i = 0; i < getServerCount(); i++)
              if (!newLeaderId.equals(getRaftPlugin(i).getRaftHAServer().getLeaderId()))
                return false;
            return true;
          });
      assertThat(findLeaderIndex()).as("round %d: the ex-leader must not be the leader again", round)
          .isNotEqualTo(exLeaderIndex);
    }
  }

  /** Issue #8533 (and #8487), from {@code Issue8533TargetedTransferUnderWritesWinsIT}. */
  @Test
  @Tag("slow")
  void aTargetedTransferUnderConcurrentWritesHandsLeadershipToTheTarget() throws Exception {
    final int firstLeader = findLeaderIndex();
    assertThat(firstLeader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(firstLeader, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().getOrCreateVertexType("Issue8533"));
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong written = new AtomicLong();
    final List<Thread> writers = new ArrayList<>();
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      final int node = i;
      final Thread writer = new Thread(() -> {
        while (!stop.get()) {
          try {
            db.transaction(() -> db.newVertex("Issue8533").set("node", node).save());
            written.incrementAndGet();
          } catch (final Exception e) {
            // Writes are refused or retried while leadership moves: they are the load here, not the assertion.
          }
        }
      }, "issue8533-writer-" + i);
      writer.setDaemon(true);
      writer.start();
      writers.add(writer);
    }

    final List<String> failures = new ArrayList<>();
    try {
      for (int round = 0; round < TARGETED_TRANSFER_ROUNDS; round++) {
        final long before = written.get();
        Awaitility.await("writes are flowing before round " + round).atMost(30, TimeUnit.SECONDS)
            .pollInterval(20, TimeUnit.MILLISECONDS).until(() -> written.get() > before + 20);

        final int leaderIndex = findLeaderIndex();
        assertThat(leaderIndex).as("round %d: a Raft leader must be elected", round).isGreaterThanOrEqualTo(0);
        final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
        final int targetIndex = (leaderIndex + 1 + (round % 2)) % getServerCount();
        final RaftPeerId targetId = getRaftPlugin(targetIndex).getRaftHAServer().getLocalPeerId();

        RaftPeerId settledLeader = null;
        try {
          leader.transferLeadership(targetId.toString(), 10_000);
          // #8487: a transfer that returns must return with the target already the leader, not with the handoff still open
          if (targetId.equals(leader.getLeaderId()))
            settledLeader = targetId;
          else
            failures.add("round " + round + ": the transfer returned while the leader was " + leader.getLeaderId() + " instead of "
                + targetId);
        } catch (final Exception e) {
          failures.add("round " + round + ": " + e.getMessage());
        }

        // Whatever the outcome, let the cluster agree on one leader before the next round, so one lost election does not
        // cascade into the rounds after it. After a successful transfer that leader must be the target (#8487).
        final RaftPeerId expectedLeader = settledLeader;
        Awaitility.await("round " + round + ": every node names the same leader").atMost(30, TimeUnit.SECONDS)
            .pollInterval(50, TimeUnit.MILLISECONDS).until(() -> {
              final RaftPeerId first = getRaftPlugin(0).getRaftHAServer().getLeaderId();
              if (first == null || (expectedLeader != null && !expectedLeader.equals(first)))
                return false;
              for (int i = 1; i < getServerCount(); i++)
                if (!first.equals(getRaftPlugin(i).getRaftHAServer().getLeaderId()))
                  return false;
              return true;
            });
      }
    } finally {
      stop.set(true);
      for (final Thread writer : writers)
        writer.join(30_000);
    }

    assertThat(failures).as("every targeted transfer under writes must hand leadership to its target").isEmpty();
  }

  /** Issue #8491, from {@code Issue8491LeaderReplacingDatabaseHandsOffIT}. */
  @Test
  @Tag("slow")
  void aLeaderReplacingADatabaseHandsLeadershipToAPeer() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int replacingIndex = (leaderIndex + 1) % getServerCount();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Issue8491"))
        leaderDb.getSchema().createVertexType("Issue8491");
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex("Issue8491").set("value", i).save();
    });
    waitForReplicationIsCompleted(replacingIndex);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftHAServer replacingRaft = getRaftPlugin(replacingIndex).getRaftHAServer();
    final ArcadeStateMachine replacingSm = replacingRaft.getStateMachine();

    // Park the resync once its copy is downloaded, before the swap: the install is in flight from here until release.
    final CountDownLatch staged = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    SnapshotInstaller.snapshotStagedForTesting = dbName -> {
      staged.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final AtomicReference<Throwable> resyncFailure = new AtomicReference<>();
    final Thread resync = new Thread(() -> {
      try {
        replacingSm.resyncDatabaseFromLeader(getDatabaseName());
      } catch (final Throwable t) {
        resyncFailure.set(t);
      }
    }, "issue8491-resync");
    resync.start();
    try {
      assertThat(staged.await(60, TimeUnit.SECONDS)).as("the resync must reach its swap").isTrue();
      assertThat(replacingSm.getDatabasesBeingReplaced()).containsExactly(getDatabaseName());

      // Neither node has anything to hand off yet: the leader replaces nothing, the replacing node is a follower.
      assertThat(leaderRaft.getStateMachine().handOffLeadershipWhileReplacingDatabase()).isFalse();
      assertThat(replacingSm.handOffLeadershipWhileReplacingDatabase()).isFalse();
      assertThat(findLeaderIndex()).isEqualTo(leaderIndex);

      // The election of the issue: the replacing node becomes the leader, its log being as complete as anyone's.
      final RaftPeerId replacingPeerId = replacingRaft.getLocalPeerId();
      leaderRaft.transferLeadership(replacingPeerId.toString(), 10_000);
      Awaitility.await("the replacing node is the leader").atMost(30, TimeUnit.SECONDS)
          .pollInterval(50, TimeUnit.MILLISECONDS).until(replacingRaft::isLeader);

      // Every other signal reads healthy; the status document must say what is wrong.
      final JSONArray alerts = ClusterAlerts.scan(getServer(replacingIndex), replacingSm);
      assertThat(alerts.toString()).contains("leader-replacing-database");

      // The health tick's hook (RaftHAServer is the monitor's target) hands leadership to a peer.
      replacingRaft.handOffLeadershipWhileReplacingDatabase();
      Awaitility.await("leadership leaves the replacing node").atMost(30, TimeUnit.SECONDS)
          .pollInterval(50, TimeUnit.MILLISECONDS).until(() -> {
            final RaftPeerId leader = replacingRaft.getLeaderId();
            return leader != null && !leader.equals(replacingPeerId) && !replacingRaft.isLeader();
          });
      assertThat(ClusterAlerts.scan(getServer(replacingIndex), replacingSm).toString())
          .doesNotContain("leader-replacing-database");
    } finally {
      release.countDown();
      resync.join(60_000);
    }

    assertThat(resync.isAlive()).as("the resync must finish").isFalse();
    assertThat(resyncFailure.get()).as("the resync must succeed").isNull();
    assertThat(replacingSm.getDatabasesBeingReplaced()).isEmpty();

    // Writes work again, through the new leader, and reach every node.
    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).isNotEqualTo(replacingIndex).isGreaterThanOrEqualTo(0);
    final Database newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    newLeaderDb.transaction(() -> newLeaderDb.newVertex("Issue8491").set("value", 100).save());
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
    assertClusterConsistency();
  }

  /** Issue #8592, from {@code Issue8592LeaderLeavesClusterIT}. */
  @Test
  @Tag("slow")
  void theLeaderLeavingHandsOffAndIsRemoved() throws Exception {
    final int leaderIndex = waitForLeaderIndex(30_000);
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final String leavingPeerId = peerIdForIndex(leaderIndex);

    getRaftPlugin(leaderIndex).getRaftHAServer().leaveCluster(false);

    int newLeaderIndex = -1;
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      newLeaderIndex = leaderAmong(leaderIndex);
      if (newLeaderIndex >= 0 && !peerIds(newLeaderIndex).contains(leavingPeerId))
        break;
      Thread.sleep(200);
    }

    assertThat(newLeaderIndex).as("One of the two remaining nodes leads").isGreaterThanOrEqualTo(0).isNotEqualTo(leaderIndex);
    assertThat(peerIds(newLeaderIndex)).as("The leader left the configuration").doesNotContain(leavingPeerId).hasSize(2);
  }

  /** From {@code RaftTransferLeadershipIT}: a targeted transfer through the HTTP endpoint, then writes on the target. */
  @Test
  void transferLeadershipViaHttpEndpoint() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int targetIndex = leaderIndex == 0 ? 1 : 0;

    final int httpPort = getServerHttpPort(leaderIndex);
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/cluster/leader").toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

    final String body = new JSONObject().put("peerId", peerIdForIndex(targetIndex)).put("timeoutMs", 10_000).toString();
    conn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

    final int responseCode = conn.getResponseCode();
    final String responseBody;
    if (responseCode >= 400 && conn.getErrorStream() != null)
      responseBody = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    else if (conn.getInputStream() != null)
      responseBody = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    else
      responseBody = "";
    conn.disconnect();
    assertThat(responseCode).as("Response: %s", responseBody).isEqualTo(200);

    Thread.sleep(3_000);

    final Database newLeaderDb = getServerDatabase(targetIndex, getDatabaseName());
    newLeaderDb.transaction(() -> {
      if (!newLeaderDb.getSchema().existsType("TransferTest"))
        newLeaderDb.getSchema().createVertexType("TransferTest");
      newLeaderDb.newVertex("TransferTest").set("value", 1).save();
    });

    assertClusterConsistency();
  }

  /**
   * Polls {@link #findLeaderIndex()} until a leader is established, returning its index, or -1 if none settles within
   * {@code timeoutMs}. Tolerates the transient leaderless windows that occur right after cluster startup. Shared by
   * #4809 and #8592, which each had a copy (the #8592 one propagated an interrupt instead of returning -1).
   */
  private int waitForLeaderIndex(final long timeoutMs) {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      final int leaderIndex = findLeaderIndex();
      if (leaderIndex >= 0)
        return leaderIndex;
      try {
        Thread.sleep(200);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
    return findLeaderIndex();
  }

  /** The leader among the nodes other than {@code excluded}, or -1. */
  private int leaderAmong(final int excluded) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != excluded && getRaftPlugin(i) != null && getRaftPlugin(i).getRaftHAServer().isLeader())
        return i;
    return -1;
  }

  private List<String> peerIds(final int serverIndex) {
    final List<String> ids = new ArrayList<>();
    for (final RaftPeer peer : getRaftPlugin(serverIndex).getRaftHAServer().getLivePeers())
      ids.add(peer.getId().toString());
    return ids;
  }

  private HttpResult postTransferLeader(final int serverIndex, final String body) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        getServerHttpUrl(serverIndex, "/api/v1/cluster/leader")).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if (body != null)
      conn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

    final int code = conn.getResponseCode();
    final String responseBody;
    if (code >= 400 && conn.getErrorStream() != null)
      responseBody = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    else if (conn.getInputStream() != null)
      responseBody = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    else
      responseBody = "";
    conn.disconnect();
    return new HttpResult(code, responseBody);
  }

  private record HttpResult(int code, String body) {
  }
}
