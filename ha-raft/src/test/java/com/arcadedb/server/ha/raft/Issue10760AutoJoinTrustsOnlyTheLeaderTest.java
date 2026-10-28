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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10760: the Kubernetes auto-join decided "already a member" from the first peer that answered its
 * {@code GroupInfo} probe. Ratis builds that reply's group from the answering server's own configuration, so a follower
 * that is behind listed a node removed since, the probe answered {@code ALREADY_MEMBER}, and the retry loop ended for the
 * life of the process with the pod a non-member nobody dials. Only a LEADER answer may decide now.
 * <p>
 * Driven through {@link KubernetesAutoJoin#setProbeForTesting} with scripted per-peer answers: no Raft server, no mock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10760AutoJoinTrustsOnlyTheLeaderTest {
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

  @Test
  void aBehindFollowerListingThisNodeNoLongerMakesItAMember() {
    // B is behind and still lists A; C is the leader, whose configuration A was removed from
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, follower(C.getId(), SELF, B, C))
        .answer(C, leader(B, C));

    assertThat(autoJoin(probe).tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(probe.addsThrough).as("re-added through the leader").containsExactly("C");
  }

  @Test
  void aFollowerThatDoesNotYetListThisNodeDoesNotTriggerAnAdd() {
    // The mirror image: B predates A's addition, the leader C already holds A
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, follower(C.getId(), B, C))
        .answer(C, leader(SELF, B, C));

    assertThat(autoJoin(probe).tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.ALREADY_MEMBER);
    assertThat(probe.addsThrough).as("no Mode.ADD for a member").isEmpty();
  }

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

  @Test
  void aNamedLeaderMissingFromTheFollowersConfigurationIsReportedNotDialled() {
    final ScriptedProbe probe = new ScriptedProbe()
        .answer(B, follower(D.getId(), SELF, B, C))
        .answer(C, new IOException("unreachable"));
    final KubernetesAutoJoin autoJoin = autoJoin(probe);

    assertThat(autoJoin.tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.NO_LEADER_ANSWERED);
    assertThat(probe.asked).containsExactly("B", "C");
  }

  /**
   * The guarantee, as a table: over every combination of non-leader answers from the two peers, no round yields
   * {@code ALREADY_MEMBER} or {@code JOINED}, and no {@code Mode.ADD} is sent.
   */
  @Test
  void noCombinationWithoutALeaderAnswerDecidesMembership() {
    int combinations = 0;
    for (final Function<RaftPeer, Object> forB : NON_LEADER_ANSWERS)
      for (final Function<RaftPeer, Object> forC : NON_LEADER_ANSWERS) {
        final ScriptedProbe probe = new ScriptedProbe().answer(B, forB.apply(B)).answer(C, forC.apply(C));

        final KubernetesAutoJoin.Outcome outcome = autoJoin(probe).tryAutoJoin();

        assertThat(outcome).as("B=%s, C=%s", probe.script.get(B.getId()), probe.script.get(C.getId()))
            .isIn(KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND, KubernetesAutoJoin.Outcome.NO_LEADER_ANSWERED);
        assertThat(probe.addsThrough).isEmpty();
        combinations++;
      }
    assertThat(combinations).isEqualTo(NON_LEADER_ANSWERS.size() * NON_LEADER_ANSWERS.size());
  }

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

  @Test
  void aLeaderThatRejectsTheAddIsRetriable() {
    final ScriptedProbe probe = new ScriptedProbe().answer(B, leader(B, C)).answer(C, follower(B.getId(), B, C));
    probe.addRejection = "reconfiguration in progress";

    assertThat(autoJoin(probe).tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.ADD_REJECTED);
    assertThat(probe.addsThrough).containsExactly("B");
  }

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
}
