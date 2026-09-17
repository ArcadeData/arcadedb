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

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Regression test for issue #7531: a pod that self-joins a Kubernetes StatefulSet was never seeded with the
 * cluster security documents.
 * <p>
 * {@code server-users.jsonl}, {@code server-groups.json} and {@code server-api-tokens.json} live under
 * {@code <server-root>/config/}, outside the database directory, so no Raft snapshot install carries them.
 * Issue #7521 seeds them from the <i>admitting</i> node, which covers {@code POST /api/v1/cluster/peer} and
 * {@code connect cluster}. A self-join has no admitting node - {@code KubernetesAutoJoin.tryAutoJoin} issues
 * {@code Mode.ADD} for the joining pod itself - so nothing seeded it at all, and the pod served requests
 * behind the load balancer against whatever its own config directory held.
 * <p>
 * The fix hooks the one place all three admission paths meet: the configuration entry the leader applies.
 * This pins that decision ({@link MembershipSecuritySeeder}) and its wiring into
 * {@link ArcadeStateMachine#notifyConfigurationChanged}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7531MembershipSecuritySeedTest {

  private static final long BUDGET_MS = 3_000L;

  private static RaftPeerId peer(final String id) {
    return RaftPeerId.valueOf(id);
  }

  private static List<RaftPeerId> peers(final String... ids) {
    final List<RaftPeerId> list = new ArrayList<>(ids.length);
    for (final String id : ids)
      list.add(peer(id));
    return list;
  }

  /** Runs the seed on the calling thread so every assertion below is about the decision, never about timing. */
  private static final Executor SAME_THREAD = Runnable::run;

  /** Records what the seed was asked to do, standing in for {@code ServerSecurity.seedSecurityStateClusterWide}. */
  private static class RecordingSeed implements MembershipSecuritySeeder.SecuritySeed {
    final List<Long>     budgets = new ArrayList<>();
    List<String>         failures = List.of();
    RuntimeException     blowUp;

    @Override
    public List<String> seed(final long retryBudgetMs) {
      budgets.add(retryBudgetMs);
      if (blowUp != null)
        throw blowUp;
      return failures;
    }

    int calls() {
      return budgets.size();
    }
  }

  private MembershipSecuritySeeder seeder(final RecordingSeed seed, final AtomicBoolean leader) {
    return new MembershipSecuritySeeder(leader::get, () -> BUDGET_MS, seed, SAME_THREAD);
  }

  // -------------------------------------------------------------------------------------------
  // The defect: a peer that entered the configuration on its own must still be seeded
  // -------------------------------------------------------------------------------------------

  /**
   * The reported case. A StatefulSet scale-up adds {@code arcadedb-3} to the committed configuration with no
   * ArcadeDB code running on the admitting side, so the leader noticing the configuration change is the only
   * thing left that can seed it.
   */
  @Test
  void aPeerThatJoinsTheConfigurationIsSeededByTheLeader() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true));

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"));

    assertThat(seed.calls()).as("the self-joining pod must be seeded").isEqualTo(1);
    assertThat(seed.budgets).containsExactly(BUDGET_MS);
  }

  /**
   * The joining pod applies the very same configuration entry, and its own documents are the stale ones this
   * seed exists to replace. A node that is not the leader must therefore never issue one - the gate is
   * correctness, not an optimization to avoid N redundant submits.
   */
  @Test
  void aNonLeaderNeverSeedsItsOwnStaleDocuments() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(false));

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"));

    assertThat(seed.calls()).isZero();
  }

  /**
   * A follower still tracks the membership it applies, so the node that becomes leader next has a baseline
   * rather than treating the first configuration it sees as leader as a fresh start.
   */
  @Test
  void aFollowerKeepsTheBaselineCurrentSoItCanSeedOnceItIsLeader() {
    final RecordingSeed seed = new RecordingSeed();
    final AtomicBoolean leader = new AtomicBoolean(false);
    final MembershipSecuritySeeder seeder = seeder(seed, leader);

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));
    assertThat(seed.calls()).isZero();

    leader.set(true);
    seeder.onConfigurationChanged(2, 12, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"));

    assertThat(seed.calls()).as("only the peer added while leader is seeded, not the one added before").isEqualTo(1);
    assertThat(seeder.knownPeersForTest())
        .containsExactlyInAnyOrder(peer("arcadedb-0"), peer("arcadedb-1"), peer("arcadedb-2"), peer("arcadedb-3"));
  }

  // -------------------------------------------------------------------------------------------
  // What must NOT trigger a seed
  // -------------------------------------------------------------------------------------------

  /**
   * The first configuration observed is a baseline, not an admission. On a leader that first observation is
   * Ratis's own startup configuration entry - {@code LeaderStateImpl.start()} appends one carrying the current
   * membership at the beginning of every term, and the leader rejects client requests with
   * {@code LeaderNotReadyException} until it is applied - so a membership change can never be the first
   * configuration a leader sees.
   */
  @Test
  void theFirstConfigurationObservedIsOnlyABaseline() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true));

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));

    assertThat(seed.calls()).isZero();
    assertThat(seeder.knownPeersForTest())
        .containsExactlyInAnyOrder(peer("arcadedb-0"), peer("arcadedb-1"), peer("arcadedb-2"));
  }

  /**
   * One membership change produces two configuration entries under Ratis joint consensus - the transitional
   * one and the final one - and both carry the same new peer set. Seeding on the added peers rather than on
   * every configuration entry is what keeps that one change to one seed.
   */
  @Test
  void theTransitionalAndFinalConfigurationOfOneChangeSeedOnlyOnce() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true));

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"));
    seeder.onConfigurationChanged(1, 12, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"));

    assertThat(seed.calls()).isEqualTo(1);
  }

  /** A removal admits nobody, so there is nothing to seed and no reason to put three entries in the Raft log. */
  @Test
  void aConfigurationThatOnlyRemovesAPeerSeedsNothing() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true));

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1"));

    assertThat(seed.calls()).isZero();
  }

  /**
   * A peer removed and later re-added is a new admission and is seeded again: while it was out of the cluster
   * its documents went stale, which is the #7521 failure mode with no failed seed even to log.
   */
  @Test
  void aPeerRemovedAndReAddedIsSeededAgain() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true));

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1"));
    seeder.onConfigurationChanged(1, 12, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));

    assertThat(seed.calls()).isEqualTo(1);
  }

  // -------------------------------------------------------------------------------------------
  // Nothing may reach the Ratis apply thread
  // -------------------------------------------------------------------------------------------

  /**
   * A seed that reports failing documents is logged, not thrown: the peer is already a committed member and
   * there is no caller left to fail. The same contract {@code PostAddPeerHandler} and {@code connect cluster}
   * hold, arrived at from a callback that has no caller at all.
   */
  @Test
  void aSeedThatReportsFailuresDoesNotEscape() {
    final RecordingSeed seed = new RecordingSeed();
    seed.failures = List.of("users", "API tokens");
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true));

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));

    assertThatNoException().isThrownBy(
        () -> seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2")));
    assertThat(seed.calls()).isEqualTo(1);
  }

  /**
   * And neither does a seed that blows up outright. With the production executor an escaping throwable kills
   * the single worker; with the same-thread executor here it would land on the Ratis state-machine apply
   * thread, which is the one thread in the process that must never see an exception from housekeeping.
   */
  @Test
  void aSeedThatThrowsDoesNotReachTheApplyThread() {
    final RecordingSeed seed = new RecordingSeed();
    seed.blowUp = new IllegalStateException("no security store");
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true));

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));

    assertThatNoException().isThrownBy(
        () -> seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2")));
  }

  /** An executor that refuses the task coalesces rather than propagating, for the same reason. */
  @Test
  void anExecutorThatRefusesTheTaskDoesNotFailTheApply() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> BUDGET_MS, seed,
        task -> {
          throw new RejectedExecutionException("stopping");
        });

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));

    assertThatNoException().isThrownBy(
        () -> seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2")));
    assertThat(seed.calls()).isZero();
  }

  // -------------------------------------------------------------------------------------------
  // The wiring: ArcadeStateMachine.notifyConfigurationChanged
  // -------------------------------------------------------------------------------------------

  private static RaftProtos.RaftConfigurationProto configuration(final String... peerIds) {
    final RaftProtos.RaftConfigurationProto.Builder builder = RaftProtos.RaftConfigurationProto.newBuilder();
    for (final String peerId : peerIds)
      builder.addPeers(RaftProtos.RaftPeerProto.newBuilder()
          .setId(ByteString.copyFromUtf8(peerId))
          .setAddress("localhost:2434")
          .build());
    return builder.build();
  }

  /**
   * The callback decodes the configuration proto and hands the membership to the seeder. Without this the fix
   * is a class nothing calls.
   */
  @Test
  void theStateMachineCallbackFeedsTheSeederTheNewMembership() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final RecordingSeed seed = new RecordingSeed();
      final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true));
      sm.setMembershipSecuritySeederForTesting(seeder);

      sm.notifyConfigurationChanged(1, 10, configuration("arcadedb-0", "arcadedb-1", "arcadedb-2"));
      assertThat(seeder.knownPeersForTest())
          .containsExactlyInAnyOrder(peer("arcadedb-0"), peer("arcadedb-1"), peer("arcadedb-2"));
      assertThat(seed.calls()).isZero();

      sm.notifyConfigurationChanged(1, 11, configuration("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"));
      assertThat(seed.calls()).as("the added peer reaches the seeder through the state machine").isEqualTo(1);
    } finally {
      sm.close();
    }
  }

  /**
   * And it survives a state machine with nothing wired to it. Ratis calls this on a peer whose
   * {@code ArcadeDBServer} has not been attached yet - which every peer of the {@code MiniRaftCluster} harness
   * is between {@code cluster.start()} and {@code sm.setServer(...)}, and which a peer added by
   * {@code addNewPeers} stays for its whole life - and a Ratis callback must not be the place that finds out.
   */
  @Test
  void theStateMachineCallbackIsSafeOnAnUnwiredStateMachine() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      assertThatNoException().isThrownBy(() -> {
        sm.notifyConfigurationChanged(1, 10, configuration("arcadedb-0", "arcadedb-1"));
        sm.notifyConfigurationChanged(1, 11, configuration("arcadedb-0", "arcadedb-1", "arcadedb-2"));
      });
    } finally {
      sm.close();
    }
  }

}
