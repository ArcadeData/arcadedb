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

import com.arcadedb.server.ha.raft.KubernetesAutoJoin.JitterWindow;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link KubernetesAutoJoin#tryAutoJoinWithRetry} (issue #5268): the one-shot join
 * probe reliably lost the race against the peers' allowlist DNS refresh on pod recreation and then
 * parked forever. The retry loop must keep probing with capped exponential backoff until the node
 * joins (or confirms membership), the caller's condition turns false, or the thread is interrupted.
 * <p>
 * Also holds the tests of the probe that decides membership ({@link KubernetesAutoJoin#tryAutoJoin()}), which only a LEADER answer
 * may settle (#10760), and of the per-ordinal jitter windows of its first round (#11317).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class KubernetesAutoJoinRetryTest {

  private static final RaftPeer SELF = peer("A");

  private static final RaftPeer B    = peer("B");

  private static final RaftPeer C    = peer("C");

  private static final RaftPeer D    = peer("D");

  /** The shapes a non-leader can answer in, each listing or omitting this node. */
  private static final List<Function<RaftPeer, Object>> NON_LEADER_ANSWERS = List.of(
      p -> new IOException("unreachable"),
      p -> null,
      p -> follower(null, SELF, B, C),
      p -> follower(null, B, C),
      p -> follower(D.getId(), SELF, B, C), // names a leader that is not reachable through the script
      p -> follower(D.getId(), B, C),
      p -> follower(SELF.getId(), SELF, B, C)); // names this node as the leader, which it is not

  /** The number of slots the range holds: 3000 ms of range, 500 ms per slot. */
  private static final int  SLOTS   = 6;

  private static final long SLOT_MS = 500L;

  private static final long MAX_MS  = 3_000L;

  /** Builds an auto-join whose probe outcomes are scripted and whose sleeps are recorded, not slept. */
  private static KubernetesAutoJoin scripted(final Deque<KubernetesAutoJoin.Outcome> outcomes, final List<Long> sleeps) {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId());
    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, group, RaftPeerId.valueOf("test_1"),
        new RaftProperties()) {
      @Override
      public Outcome tryAutoJoin() {
        return outcomes.pop();
      }
    };
    autoJoin.setSleeperForTesting(sleeps::add);
    return autoJoin;
  }

  @Test
  void retriesWithExponentialBackoffUntilJoined() {
    final Deque<KubernetesAutoJoin.Outcome> outcomes = new ArrayDeque<>(List.of(
        KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND,
        KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND,
        KubernetesAutoJoin.Outcome.JOINED));
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(outcomes, sleeps).tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(sleeps).containsExactly(KubernetesAutoJoin.RETRY_BACKOFF_INITIAL_MS,
        KubernetesAutoJoin.RETRY_BACKOFF_INITIAL_MS * 2);
  }

  @Test
  void alreadyMemberStopsImmediately() {
    final Deque<KubernetesAutoJoin.Outcome> outcomes =
        new ArrayDeque<>(List.of(KubernetesAutoJoin.Outcome.ALREADY_MEMBER));
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(outcomes, sleeps).tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.ALREADY_MEMBER);
    assertThat(sleeps).isEmpty();
  }

  @Test
  void stopsWithoutSleepingWhenConditionTurnsFalse() {
    // The typical exit on a cold-start deployment: the probe finds no cluster, but by the time the
    // retry decision is made a leader has been elected (the caller's condition turns false).
    final Deque<KubernetesAutoJoin.Outcome> outcomes =
        new ArrayDeque<>(List.of(KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND));
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(outcomes, sleeps).tryAutoJoinWithRetry(() -> false);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND);
    assertThat(sleeps).isEmpty();
  }

  @Test
  void addRejectedIsRetried() {
    // A peer that accepts the probe but rejects the configuration change (e.g. no settled leader
    // yet) is a transient condition and must be retried, not treated as terminal.
    final Deque<KubernetesAutoJoin.Outcome> outcomes = new ArrayDeque<>(List.of(
        KubernetesAutoJoin.Outcome.ADD_REJECTED,
        KubernetesAutoJoin.Outcome.JOINED));
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(outcomes, sleeps).tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(sleeps).hasSize(1);
  }

  @Test
  void backoffIsCappedAtMaximum() {
    final List<KubernetesAutoJoin.Outcome> script = new ArrayList<>();
    for (int i = 0; i < 8; i++)
      script.add(KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND);
    script.add(KubernetesAutoJoin.Outcome.JOINED);
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(new ArrayDeque<>(script), sleeps).tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(sleeps).containsExactly(2_000L, 4_000L, 8_000L, 16_000L, 32_000L, 60_000L, 60_000L, 60_000L);
  }

  @Test
  void interruptDuringBackoffStopsTheLoop() {
    final Deque<KubernetesAutoJoin.Outcome> outcomes = new ArrayDeque<>(List.of(
        KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND,
        KubernetesAutoJoin.Outcome.JOINED));
    final KubernetesAutoJoin autoJoin = scripted(outcomes, new ArrayList<>());
    autoJoin.setSleeperForTesting(ms -> {
      throw new InterruptedException("shutdown");
    });

    final KubernetesAutoJoin.Outcome result = autoJoin.tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.INTERRUPTED);
    // The interrupt flag must be restored for the owning thread's shutdown handling.
    assertThat(Thread.interrupted()).isTrue(); // also clears it so it does not leak into other tests
  }

  @Test
  void probeRetryPolicyIsBoundedNotUnboundedDefault() {
    // Regression for issue #5973 (see KubernetesAutoJoin.PROBE_RETRY_POLICY for the full root-cause
    // writeup): the probe client must always set a bounded retry policy, not fall back to Ratis's
    // unbounded retryForeverNoSleep() default.
    assertThat(KubernetesAutoJoin.PROBE_RETRY_POLICY).isInstanceOf(RetryPolicies.RetryLimited.class);
    assertThat(((RetryPolicies.RetryLimited) KubernetesAutoJoin.PROBE_RETRY_POLICY).getMaxAttempts()).isEqualTo(5);
    // RetryLimited/RetryForeverWithSleep expose no sleep-time getter, only toString(); pin it here too
    // so a future edit can't silently widen 500ms to something much larger while maxAttempts still
    // reads 5 and this test still passes. If this breaks after a ratis.version bump, check whether
    // Ratis changed its toString() format before assuming an actual regression.
    assertThat(KubernetesAutoJoin.PROBE_RETRY_POLICY.toString()).contains("sleepTime=500ms");
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  /**
   * Issue #10760: the Kubernetes auto-join decided "already a member" from the first peer that answered its
   * {@code GroupInfo} probe. Ratis builds that reply's group from the answering server's own configuration, so a follower
   * that is behind listed a node removed since, the probe answered {@code ALREADY_MEMBER}, and the retry loop ended for the
   * life of the process with the pod a non-member nobody dials. Only a LEADER answer may decide now.
   * <p>
   * Driven through {@link KubernetesAutoJoin#setProbeForTesting} with scripted per-peer answers: no Raft server, no mock.
   */
  @Test
  void aBehindFollowerListingThisNodeNoLongerMakesItAMember() {
    // B is behind and still lists A; C is the leader, whose configuration A was removed from
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, follower(C.getId(), SELF, B, C))
        .answer(C, leader(B, C));

    assertThat(autoJoin(probe).tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(probe.addsThrough).as("re-added through the leader").containsExactly("C");
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  @Test
  void aFollowerThatDoesNotYetListThisNodeDoesNotTriggerAnAdd() {
    // The mirror image: B predates A's addition, the leader C already holds A
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, follower(C.getId(), B, C))
        .answer(C, leader(SELF, B, C));

    assertThat(autoJoin(probe).tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.ALREADY_MEMBER);
    assertThat(probe.addsThrough).as("no Mode.ADD for a member").isEmpty();
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  @Test
  void theLeaderAFollowerNamesIsAskedNextEvenWhenItIsNotDeclared() {
    // D joined after this node's server list was written: only B's configuration knows its address
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, follower(D.getId(), SELF, B, C, D))
        .answer(C, new IOException("must not be needed"))
        .answer(D, leader(B, C, D));

    assertThat(autoJoin(probe).tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(probe.asked).as("the named leader is asked before the next declared peer").startsWith("B", "D");
    assertThat(probe.addsThrough).containsExactly("D");
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  @Test
  void aNamedLeaderWithAnIllegalAddressIsNotDialled() {
    // B's configuration holds D at an address no peer address may take: the probe does not follow it
    final RaftPeer brokenD = RaftPeer.newBuilder().setId(D.getId()).setAddress("pod-d:0").build();
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, follower(D.getId(), SELF, B, brokenD))
        .answer(C, follower(D.getId(), SELF, B, brokenD))
        .answer(D, leader(SELF, B, C, D));

    assertThat(autoJoin(probe).tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.NO_LEADER_ANSWERED);
    assertThat(probe.asked).containsExactly("B", "C");
    assertThat(probe.addsThrough).isEmpty();
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  /**
   * A leader that failed at its declared address is still asked at the address its group holds for it: a probe is keyed
   * by the peer AND the address, so the failure at one does not skip the other for every round.
   */
  @Test
  void aLeaderUnreachableAtItsDeclaredAddressIsAskedWhereItsGroupPutsIt() {
    final RaftPeer movedB = RaftPeer.newBuilder().setId(B.getId()).setAddress("pod-b-moved:2434").build();
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, new IOException("nothing listens at the declared address"))
        .answer(C, follower(B.getId(), SELF, movedB, C))
        .answerAt("pod-b-moved:2434", leader(movedB, C));

    assertThat(autoJoin(probe).tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(probe.asked).containsExactly("B", "C", "B");
    assertThat(probe.addsThrough).containsExactly("B");
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  @Test
  void aNamedLeaderMissingFromTheFollowersConfigurationIsReportedNotDialled() {
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, follower(D.getId(), SELF, B, C))
        .answer(C, new IOException("unreachable"));
    final KubernetesAutoJoin autoJoin = autoJoin(probe);

    assertThat(autoJoin.tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.NO_LEADER_ANSWERED);
    assertThat(probe.asked).containsExactly("B", "C");
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  /**
   * The guarantee, as a table: over every combination of non-leader answers from the two peers, no round yields
   * {@code ALREADY_MEMBER} or {@code JOINED}, and no {@code Mode.ADD} is sent.
   */
  @Test
  void noCombinationWithoutALeaderAnswerDecidesMembership() {
    final List<String> expectedCombinations = List.of(
        "0:0", "0:1", "0:2", "0:3", "0:4", "0:5", "0:6",
        "1:0", "1:1", "1:2", "1:3", "1:4", "1:5", "1:6",
        "2:0", "2:1", "2:2", "2:3", "2:4", "2:5", "2:6",
        "3:0", "3:1", "3:2", "3:3", "3:4", "3:5", "3:6",
        "4:0", "4:1", "4:2", "4:3", "4:4", "4:5", "4:6",
        "5:0", "5:1", "5:2", "5:3", "5:4", "5:5", "5:6",
        "6:0", "6:1", "6:2", "6:3", "6:4", "6:5", "6:6");

    final List<String> checkedCombinations = new ArrayList<>();
    for (int b = 0; b < NON_LEADER_ANSWERS.size(); b++)
      for (int c = 0; c < NON_LEADER_ANSWERS.size(); c++) {
        final Function<RaftPeer, Object> forB = NON_LEADER_ANSWERS.get(b);
        final Function<RaftPeer, Object> forC = NON_LEADER_ANSWERS.get(c);
        final ScriptedProbe probe = new ScriptedProbe().answer(B, forB.apply(B)).answer(C, forC.apply(C));

        final KubernetesAutoJoin.Outcome outcome = autoJoin(probe).tryAutoJoin();

        assertThat(outcome).as("B=%s, C=%s", probe.script.get(B.getId()), probe.script.get(C.getId()))
            .isIn(KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND, KubernetesAutoJoin.Outcome.NO_LEADER_ANSWERED);
        assertThat(probe.addsThrough).isEmpty();
        checkedCombinations.add(b + ":" + c);
      }
    assertThat(checkedCombinations).containsExactlyElementsOf(expectedCombinations);
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  @Test
  void answersFromFollowersOnlyAreRetriedUntilTheLeaderAnswers() {
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, follower(null, SELF, B, C))
        .answer(C, new IOException("electing"));
    final List<Long> sleeps = new ArrayList<>();
    final KubernetesAutoJoin autoJoin = autoJoin(probe);
    autoJoin.setSleeperForTesting(ms -> {
      sleeps.add(ms);
      // Once the first round is over the cluster has elected C, which lists this node
      if (sleeps.size() == 2)
        probe.answer(C, leader(SELF, B, C));
    });

    assertThat(autoJoin.tryAutoJoinWithRetry(() -> true)).isEqualTo(KubernetesAutoJoin.Outcome.ALREADY_MEMBER);
    // jitter, backoff, jitter: the first round ended NO_LEADER_ANSWERED and did not end the loop
    assertThat(sleeps).hasSize(3);
    assertThat(sleeps.get(1)).isEqualTo(KubernetesAutoJoin.RETRY_BACKOFF_INITIAL_MS);
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  @Test
  void aLeaderThatRejectsTheAddIsRetriable() {
    final ScriptedProbe probe = new ScriptedProbe().answer(B, leader(B, C)).answer(C, follower(B.getId(), B, C));
    probe.addRejection = "reconfiguration in progress";

    assertThat(autoJoin(probe).tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.ADD_REJECTED);
    assertThat(probe.addsThrough).containsExactly("B");
  }

  // Regression for #10760: the auto-join probe lets only a LEADER answer decide membership
  @Test
  void onlyALeaderAnswerBuildsALeaderConfiguration() {
    assertThat(KubernetesAutoJoin.LeaderConfiguration.of(B, follower(B.getId(), SELF, B))).isNull();
    assertThat(KubernetesAutoJoin.LeaderConfiguration.of(B, null)).isNull();
    assertThat(KubernetesAutoJoin.LeaderConfiguration.of(B, leader(SELF, B)).contains(SELF.getId())).isTrue();
  }

  private static KubernetesAutoJoin autoJoin(final ScriptedProbe probe) {
    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null,
        RaftGroup.valueOf(RaftGroupId.randomId(), SELF, B, C), SELF.getId(), new RaftProperties());
    autoJoin.setSleeperForTesting(ms -> {
    });
    autoJoin.setProbeForTesting(probe);
    return autoJoin;
  }

  private static KubernetesAutoJoin.GroupAnswer leader(final RaftPeer... members) {
    return new KubernetesAutoJoin.GroupAnswer(true, null, List.of(members));
  }

  private static KubernetesAutoJoin.GroupAnswer follower(final RaftPeerId leaderId, final RaftPeer... members) {
    return new KubernetesAutoJoin.GroupAnswer(false, leaderId, List.of(members));
  }

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress("pod-" + id.toLowerCase() + ":2434").build();
  }

  /** Answers each peer from a script: a {@code GroupAnswer}, {@code null} (unsuccessful) or an exception to throw. */
  private static final class ScriptedProbe implements KubernetesAutoJoin.ClusterProbe {
    private final Map<RaftPeerId, Object> script      = new HashMap<>();
    private final Map<String, Object>     byAddress   = new HashMap<>();
    private final List<String>            asked       = new ArrayList<>();
    private final List<String>            addsThrough = new ArrayList<>();
    private       String                  addRejection;

    ScriptedProbe answer(final RaftPeer peer, final Object answer) {
      script.put(peer.getId(), answer);
      return this;
    }

    /** An answer for one address only, taking precedence over the peer's answer by id. */
    ScriptedProbe answerAt(final String address, final Object answer) {
      byAddress.put(address, answer);
      return this;
    }

    @Override
    public KubernetesAutoJoin.GroupAnswer ask(final RaftPeer peer) throws Exception {
      asked.add(peer.getId().toString());
      final Object answer = byAddress.containsKey(peer.getAddress()) ? byAddress.get(peer.getAddress()) : script.get(peer.getId());
      if (answer instanceof Exception e)
        throw e;
      return (KubernetesAutoJoin.GroupAnswer) answer;
    }

    @Override
    public String addThrough(final RaftPeer leader, final RaftPeer localPeer) {
      assertThat(localPeer.getId()).isEqualTo(SELF.getId());
      addsThrough.add(leader.getId().toString());
      return addRejection;
    }
  }

  private static JitterWindow windowOf(final int ordinal) {
    return KubernetesAutoJoin.computeJitterWindow("arcadedb-" + ordinal);
  }

  private static boolean overlap(final JitterWindow a, final JitterWindow b) {
    return a.minMs() < b.maxMs() && b.minMs() < a.maxMs();
  }

  // Regression for #11317: the first-probe jitter windows of the pod ordinals do not overlap
  /**
   * Issue #11317: the jitter of the first cluster probe was documented as non-overlapping windows per pod ordinal, while only the
   * lower bound moved with the ordinal: the windows were nested, and every pod from ordinal 6 up drew from one 100 ms slot.
   */
  @Test
  void ordinalsLessThanALapApartNeverDrawTheSameInstant() {
    for (int a = 0; a < 4 * SLOTS; a++)
      for (int b = a + 1; b < a + SLOTS; b++)
        assertThat(overlap(windowOf(a), windowOf(b)))
            .as("pod-%d %s and pod-%d %s must not overlap", a, windowOf(a), b, windowOf(b)).isFalse();
  }

  // Regression for #11317: the first-probe jitter windows of the pod ordinals do not overlap
  @Test
  void aStatefulSetOfAnySizeKeepsFullWidthWindowsInsideTheRange() {
    for (final int ordinal : new int[] { 0, 1, 5, 6, 7, 8, 9, 100, 1_001, Integer.MAX_VALUE }) {
      final JitterWindow window = windowOf(ordinal);
      assertThat(window.maxMs() - window.minMs()).as("width of the window of pod-%d", ordinal).isEqualTo(SLOT_MS);
      assertThat(window.minMs()).as("start of the window of pod-%d", ordinal).isGreaterThanOrEqualTo(0L);
      assertThat(window.maxMs()).as("end of the window of pod-%d", ordinal).isLessThanOrEqualTo(MAX_MS);
    }
  }

  // Regression for #11317: the first-probe jitter windows of the pod ordinals do not overlap
  @Test
  void theHighOrdinalsNoLongerCollapseOntoOneSlot() {
    assertThat(windowOf(6)).isNotEqualTo(windowOf(7));
    assertThat(windowOf(7)).isNotEqualTo(windowOf(8));
    assertThat(windowOf(6)).as("a lap later the slot comes round again").isEqualTo(windowOf(0));
  }

  // Regression for #11317: the first-probe jitter windows of the pod ordinals do not overlap
  @Test
  void withoutAnOrdinalThePodDrawsFromTheWholeRange() {
    final JitterWindow whole = new JitterWindow(0, MAX_MS);
    assertThat(KubernetesAutoJoin.computeJitterWindow(null)).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("")).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("arcadedb")).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("arcadedb-")).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("arcadedb-primary")).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("arcadedb-99999999999")).as("an ordinal an int cannot hold").isEqualTo(whole);
  }

  // Regression for #11317: the first-probe jitter windows of the pod ordinals do not overlap
  /** The wait of a probe round is drawn from the window, whatever this machine's HOSTNAME reads. */
  @Test
  void aProbeRoundWaitsInsideTheWindowOfThisHost() {
    final RaftPeer self = RaftPeer.newBuilder().setId(RaftPeerId.valueOf("self")).setAddress("localhost:1").build();
    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, RaftGroup.valueOf(RaftGroupId.randomId(), self), self.getId(),
        new RaftProperties());

    final List<Long> waits = new ArrayList<>();
    // INTERRUPTED ENDS THE ROUND BEFORE ANY PEER IS PROBED, SO NO CLUSTER IS NEEDED TO READ THE WAIT
    autoJoin.setSleeperForTesting(ms -> {
      waits.add(ms);
      throw new InterruptedException();
    });

    final JitterWindow window = KubernetesAutoJoin.computeJitterWindow(System.getenv("HOSTNAME"));
    try {
      for (int i = 0; i < 200; i++)
        assertThat(autoJoin.tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.INTERRUPTED);
    } finally {
      // THE ROUND RESTORES THE INTERRUPT FLAG, WHICH MUST NOT LEAK INTO THE NEXT TEST ON THIS THREAD
      Thread.interrupted();
    }

    assertThat(waits).hasSize(200).allSatisfy(ms -> assertThat(ms).isBetween(window.minMs(), window.maxMs() - 1));
  }
}
