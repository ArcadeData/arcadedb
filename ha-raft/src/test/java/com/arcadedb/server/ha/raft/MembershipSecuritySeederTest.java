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

import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link MembershipSecuritySeeder}: seeding on membership change, single-flight seeding and worker pool statistics.
 *
 * <p>Regression coverage for #7531:
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
 * <p>Regression coverage for #7834:
 * Regression test for issue #7834: an admission seeded the security documents twice, from two nodes, under two
 * different monitors.
 * <p>
 * After issue #7531 the leader seeds every membership change of its own accord, while
 * {@code POST /api/v1/cluster/peer} and {@code connect cluster} still ran a seed of their own on the ADMITTING
 * node - which is not required to be the leader. So an admission put up to six security entries in the Raft log
 * from two different JVMs, and {@code ServerSecurity}'s monitor - the thing that keeps a revocation committing
 * mid-seed from being undone by the whole document a seed carries (issue #7373) - is per-JVM, so it could not
 * order them. A revocation landing between the two was resurrected by whichever submit was second.
 * <p>
 * The fix makes {@link MembershipSecuritySeeder} the cluster's single seeder and gives the admitting node a way
 * to READ its outcome instead of running one, so issue #7521's operator-facing {@code failedSeeds} contract
 * survives. This pins the two halves of that: one seed per admission, and a report that describes it.
 *
 * <p>Regression coverage for #7856:
 * Issue #7856: the HA security workers - the leader-side membership seed and the rejoining node's catch-up - must
 * report the load and the coalesced count their executor rows publish, so an operator can see a scale-up storm
 * folding into one seed, or a worker that has stopped draining.
 */
class MembershipSecuritySeederTest {

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

  // Regression for #7531: a peer that joins the configuration is seeded by the leader
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

  // Regression for #7531: a non leader never seeds its own stale documents
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

  // Regression for #7531: a follower keeps the baseline current so it can seed once it is leader
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

  // Regression for #7531: the first configuration observed is only a baseline
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

  // Regression for #7531: the transitional and final configuration of one change seed only once
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

  // Regression for #7531: a configuration that only removes a peer seeds nothing
  /** A removal admits nobody, so there is nothing to seed and no reason to put three entries in the Raft log. */
  @Test
  void aConfigurationThatOnlyRemovesAPeerSeedsNothing() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true));

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1"));

    assertThat(seed.calls()).isZero();
  }

  // Regression for #7531: a peer removed and re added is seeded again
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

  // Regression for #7531: a seed that reports failures does not escape
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

  // Regression for #7531: a seed that throws does not reach the apply thread
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

  // Regression for #7531: an executor that refuses the task does not fail the apply
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

  // Regression for #7531: the state machine callback feeds the seeder the new membership
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

  // Regression for #7531: the state machine callback is safe on an unwired state machine
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

  private static final long TIMEOUT_MS = 10_000L;

  /** Stands in for {@code ServerSecurity.seedSecurityStateClusterWide}, counting how often the cluster is seeded. */
  private static class CountingSeed implements MembershipSecuritySeeder.SecuritySeed {
    final AtomicInteger  calls    = new AtomicInteger();
    List<String>         failures = List.of();
    RuntimeException     blowUp;
    CountDownLatch       started;
    CountDownLatch       release;

    @Override
    public List<String> seed(final long retryBudgetMs) {
      calls.incrementAndGet();
      if (started != null)
        started.countDown();
      if (release != null)
        try {
          release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      if (blowUp != null)
        throw blowUp;
      return failures;
    }
  }

  private static MembershipSecuritySeeder seeder(final CountingSeed seed, final Executor executor) {
    return new MembershipSecuritySeeder(() -> true, () -> BUDGET_MS, seed, executor);
  }

  // -------------------------------------------------------------------------------------------
  // The report the admitting node reads instead of running a seed of its own
  // -------------------------------------------------------------------------------------------

  // Regression for #7834: the outcome of the seed is reported to the admitting node
  /** The contract issue #7521 made operator-facing, now answered by the seeder rather than by a second seed. */
  @Test
  void theOutcomeOfTheSeedIsReportedToTheAdmittingNode() {
    final CountingSeed seed = new CountingSeed();
    seed.failures = List.of("groups", "API tokens");

    assertThat(seeder(seed, SAME_THREAD).seedNowAndReport("a test request", TIMEOUT_MS, false))
        .as("a caller that has an operator waiting must learn which documents did not commit")
        .containsExactly("groups", "API tokens");
  }

  // Regression for #7834: a seed that committed everything reports no failures
  /** And the success answer: nothing failed, which is what turns into a 200 on the admission route. */
  @Test
  void aSeedThatCommittedEverythingReportsNoFailures() {
    assertThat(seeder(new CountingSeed(), SAME_THREAD).seedNowAndReport("a test request", TIMEOUT_MS, false)).isEmpty();
  }

  // Regression for #7834: a seed that threw is not reported as having failed nothing
  /**
   * A seed that could not run at all must NOT be reported as a seed that failed no documents - the admitting
   * node would answer 200 for a peer nothing was replicated to.
   */
  @Test
  void aSeedThatThrewIsNotReportedAsHavingFailedNothing() {
    final CountingSeed seed = new CountingSeed();
    seed.blowUp = new IllegalStateException("no security store on this node");

    assertThatThrownBy(() -> seeder(seed, SAME_THREAD).seedNowAndReport("a test request", TIMEOUT_MS, false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no security store on this node");
  }

  // -------------------------------------------------------------------------------------------
  // One seed per admission
  // -------------------------------------------------------------------------------------------

  // Regression for #7834: a request arriving while a seed is running folds into it rather than adding a second
  /**
   * The defect, in the shape it takes on the leader: the membership change schedules a seed, and the admitting
   * node's request arrives while that seed is still running. The request must take THAT seed's outcome rather
   * than adding a second one - which is what makes the cluster's seed count one per admission instead of two.
   */
  @Test
  void aRequestArrivingWhileASeedIsRunningFoldsIntoItRatherThanAddingASecond() throws Exception {
    final CountingSeed seed = new CountingSeed();
    seed.started = new CountDownLatch(1);
    seed.release = new CountDownLatch(1);
    seed.failures = List.of("users");

    final ExecutorService worker = Executors.newSingleThreadExecutor();
    try (final MembershipSecuritySeeder seeder = seeder(seed, worker)) {
      // The membership change: the leader schedules the admission's seed, which parks inside the seed itself.
      seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));
      final CompletableFuture<List<String>> membershipSeed =
          seeder.scheduleForTest("the membership change that admitted arcadedb-2");
      assertThat(seed.started.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();

      // The admitting node's request, made on THIS thread through the same scheduling path seedNowAndReport
      // takes, so the fold is observed rather than raced against: what comes back is the seed already running.
      final CompletableFuture<List<String>> admissionRequest =
          seeder.scheduleForTest("a request from the node that admitted a peer");
      assertThat(admissionRequest)
          .as("the request must fold into the outstanding seed instead of scheduling a second one")
          .isSameAs(membershipSeed);

      seed.release.countDown();

      assertThat(admissionRequest.get(TIMEOUT_MS, TimeUnit.MILLISECONDS))
          .as("and it reports that seed's outcome")
          .containsExactly("users");
      assertThat(seed.calls.get())
          .as("one admission, one seed")
          .isEqualTo(1);
    } finally {
      worker.shutdownNow();
    }
  }

  // Regression for #7834: a request arriving just after a seed finished reports it rather than running another
  /**
   * The other half of the duplicate, which the fold alone does not catch: the admitting node's request is a
   * round trip behind the configuration entry, so the membership seed can FINISH before it arrives. Scheduling
   * a second seed there would put the same three Raft entries in the log again (CodeRabbit on PR #7854), so a
   * seed that has just finished is reported instead.
   */
  @Test
  void aRequestArrivingJustAfterASeedFinishedReportsItRatherThanRunningAnother() {
    final CountingSeed seed = new CountingSeed();
    seed.failures = List.of("groups");
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));
    assertThat(seed.calls.get()).as("the membership change seeded, and finished").isEqualTo(1);

    assertThat(seeder.seedNowAndReport("an admission a round trip behind it", TIMEOUT_MS, true))
        .as("the admission reports that seed's outcome")
        .containsExactly("groups");
    assertThat(seed.calls.get()).as("one admission, one seed").isEqualTo(1);
  }

  // Regression for #7834: the callers own reason is what the run is scheduled under
  /**
   * The reason a caller gives is the one the run reports under. It used to be hardcoded to an admission's
   * phrasing, which the issue #7833 catch-up - the one caller with no concurrent membership seed to fold into,
   * so the one whose string actually reaches the log - then reported itself as (code review on PR #7854).
   */
  @Test
  void theCallersOwnReasonIsWhatTheRunIsScheduledUnder() {
    final CountingSeed seed = new CountingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.seedNowAndReport("this node rejoining the cluster as an existing member", TIMEOUT_MS, false);

    assertThat(seeder.lastRunReasonForTest())
        .as("the run must be reported under what its caller asked for, not a hardcoded admission")
        .isEqualTo("this node rejoining the cluster as an existing member");
  }

  // Regression for #7834: the membership hooks reason names the peers that joined
  /** And the membership hook's own reason survives the same way, naming the peers that joined. */
  @Test
  void theMembershipHooksReasonNamesThePeersThatJoined() {
    final CountingSeed seed = new CountingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));

    assertThat(seeder.lastRunReasonForTest()).contains("arcadedb-2").contains("Raft configuration");
  }

  // Regression for #7834: a caller that has established it needs a seed is never answered by a recent one
  /**
   * The reuse is for an admission and for nothing else. A catch-up whose fingerprints did NOT match has just
   * been told this node is out of step, so answering it from an unrelated seed that happened to finish a
   * moment ago would leave it stale - which is the failure issue #7833 exists to repair (CodeRabbit on PR
   * #7854).
   */
  @Test
  void aCallerThatHasEstablishedItNeedsASeedIsNeverAnsweredByARecentOne() {
    final CountingSeed seed = new CountingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    // A seed finishes; a catch-up request arrives immediately after it, well inside the reuse window.
    seeder.seedNowAndReport("an unrelated seed", TIMEOUT_MS, true);
    assertThat(seed.calls.get()).isEqualTo(1);

    seeder.seedNowAndReport("a node whose fingerprints did not match", TIMEOUT_MS, false);

    assertThat(seed.calls.get())
        .as("a node that has been told it is out of step must actually be seeded")
        .isEqualTo(2);
  }

  // Regression for #7834: an admission in the same window is still answered by the recent seed
  /** And the admission, in the same window, still is - that is what the window is for. */
  @Test
  void anAdmissionInTheSameWindowIsStillAnsweredByTheRecentSeed() {
    final CountingSeed seed = new CountingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.seedNowAndReport("the membership change", TIMEOUT_MS, true);
    seeder.seedNowAndReport("the admission a round trip behind it", TIMEOUT_MS, true);

    assertThat(seed.calls.get()).isEqualTo(1);
  }

  // Regression for #7834: a request past the reuse window runs a fresh seed
  /**
   * And the converse, so the reuse is not mistaken for "only ever seeds once": once the window has passed, a
   * request runs a seed of its own. That is the path a node re-seeding itself takes (issue #7833), and the
   * path an operator re-POSTing a peer minutes later has to get.
   */
  @Test
  void aRequestPastTheReuseWindowRunsAFreshSeed() {
    final CountingSeed seed = new CountingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.seedNowAndReport("a first admission", TIMEOUT_MS, true);
    seeder.forgetCompletedSeedForTest();
    seeder.seedNowAndReport("an admission past the window", TIMEOUT_MS, true);

    assertThat(seed.calls.get())
        .as("a request that cannot reuse anything is a seed of its own")
        .isEqualTo(2);
  }

  // Regression for #7834: the membership hook still seeds on its own
  /**
   * A membership change is still seeded without anybody asking - the admitting node is not the only caller, and
   * {@code KubernetesAutoJoin} has none at all (issue #7531).
   */
  @Test
  void theMembershipHookStillSeedsOnItsOwn() {
    final CountingSeed seed = new CountingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));

    assertThat(seed.calls.get()).isEqualTo(1);
  }

  // Regression for #7834: closing the seeder fails a report rather than leaving it parked
  /** A node being torn down tells its reporting caller so, instead of leaving it to wait out its timeout. */
  @Test
  void closingTheSeederFailsAReportRatherThanLeavingItParked() throws Exception {
    final CountingSeed seed = new CountingSeed();
    seed.started = new CountDownLatch(1);
    seed.release = new CountDownLatch(1);

    final ExecutorService worker = Executors.newSingleThreadExecutor();
    final MembershipSecuritySeeder seeder = seeder(seed, worker);
    final ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      final var report = caller.submit(() -> seeder.seedNowAndReport("a test request", TIMEOUT_MS, false));
      assertThat(seed.started.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();

      seeder.close();

      assertThatThrownBy(() -> report.get(TIMEOUT_MS, TimeUnit.MILLISECONDS))
          .hasRootCauseInstanceOf(IllegalStateException.class);
    } finally {
      seed.release.countDown();
      caller.shutdownNow();
      worker.shutdownNow();
    }
  }

  // Regression for #7834: a follower still seeds nothing on a membership change
  /** The leader gate is unchanged: a follower applying the same configuration entry seeds nothing. */
  @Test
  void aFollowerStillSeedsNothingOnAMembershipChange() {
    final CountingSeed seed = new CountingSeed();
    final AtomicBoolean leader = new AtomicBoolean(false);
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(leader::get, () -> BUDGET_MS, seed,
        SAME_THREAD);

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));

    assertThat(seed.calls.get()).isZero();
  }

  /** A seed held on a latch, so the outstanding state is observed rather than raced. */
  private static final class BlockingSeed implements MembershipSecuritySeeder.SecuritySeed {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    @Override
    public List<String> seed(final long retryBudgetMs) {
      started.countDown();
      try {
        release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return List.of();
    }
  }

  // Regression for #7856: an idle seeder reports no thread and its one free slot
  @Test
  void anIdleSeederReportsNoThreadAndItsOneFreeSlot() {
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 1_000L, budget -> List.of());
    try {
      assertThat(seeder.getPoolStats()).isEqualTo(MembershipSecuritySeeder.EMPTY_POOL_STATS);
      assertThat(seeder.getCoalescedSeeds()).isZero();
    } finally {
      seeder.close();
    }
  }

  // Regression for #7856: every seed request without a run of its own is coalesced
  /**
   * Every way a seed request can end without a run of its own is counted - folded into the outstanding seed,
   * answered by one that just finished, refused because the node is stopping - and none of the runs is.
   */
  @Test
  void everySeedRequestWithoutARunOfItsOwnIsCoalesced() throws Exception {
    final BlockingSeed seed = new BlockingSeed();
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 1_000L, seed);
    try {
      final CompletableFuture<List<String>> first = seeder.scheduleForTest("the first request");
      assertThat(seed.started.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();

      final PoolStats busy = seeder.getPoolStats();
      assertThat(busy.poolSize()).isEqualTo(1);
      assertThat(busy.activeThreads()).as("the seed is running on the owned worker").isEqualTo(1);
      assertThat(seeder.getCoalescedSeeds()).as("a seed that runs is not coalesced").isZero();

      assertThat(seeder.scheduleForTest("a request while one is outstanding")).isSameAs(first);
      assertThat(seeder.getCoalescedSeeds()).as("the fold").isEqualTo(1);

      seed.release.countDown();
      first.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

      assertThat(seeder.scheduleReusingForTest("an admission a round trip behind")).isSameAs(first);
      assertThat(seeder.getCoalescedSeeds()).as("the reuse of a seed that just finished").isEqualTo(2);
    } finally {
      seeder.close();
    }

    assertThat(seeder.scheduleForTest("a request after the stop")).as("a stopped worker refuses the seed").isNull();
    assertThat(seeder.getCoalescedSeeds()).as("the refusal of a stopped worker").isEqualTo(3);
  }

  // Regression for #7856: a seeder on a handed in executor reports an idle row
  /** On the test seam the executor is the caller's, so its load is not this seeder's to report. */
  @Test
  void aSeederOnAHandedInExecutorReportsAnIdleRow() {
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 1_000L, budget -> List.of(),
        Runnable::run);
    seeder.scheduleForTest("a request");
    assertThat(seeder.getPoolStats()).isEqualTo(MembershipSecuritySeeder.EMPTY_POOL_STATS);
  }

  // Regression for #7856: the catch up counts every refused request
  @Test
  void theCatchUpCountsEveryRefusedRequest() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      final PoolStats idle = catchUp.getPoolStats();
      assertThat(idle.poolSize()).isZero();
      assertThat(idle.queueDepth()).isZero();
      assertThat(idle.queueCapacityRemaining()).as("the single queue slot").isEqualTo(1);
      assertThat(catchUp.getCoalescedRequests()).isZero();

      catchUp.onRejected(new SecurityCatchUp.Attempt(catchUp.tryTakeRequest(), () -> {
      }));
      catchUp.onRejected(() -> {
      });

      assertThat(catchUp.getCoalescedRequests()).isEqualTo(2);
    }
  }

  // -------------------------------------------------------------------------------------------
  // Issue #13447: a seed that could not commit is owed, and run again once the gate lets it through
  // -------------------------------------------------------------------------------------------

  /** The peers the capability gate is refusing for, as the test moves them. */
  private static final class Withholding implements Supplier<List<String>> {
    volatile List<String> peers = List.of();

    @Override
    public List<String> get() {
      return peers;
    }
  }

  private MembershipSecuritySeeder seeder(final RecordingSeed seed, final AtomicBoolean leader,
      final Withholding withholding) {
    return new MembershipSecuritySeeder(leader::get, () -> BUDGET_MS, seed, withholding, SAME_THREAD);
  }

  // Regression for #13447: a seed the capability gate refused is run again once the peer advertises
  /**
   * The reported case. A rolling restart: {@code arcadedb-1} rejoins and asks for a seed while {@code arcadedb-0} is
   * still booting, so the capability gate refuses the group and API-token entries and the seed's retry budget runs out.
   * {@code arcadedb-0} advertises its capabilities 25 s later, and nothing ran the seed again: the node kept its stale
   * groups and tokens for two days. The seed is now owed, and run again at the capability round that finds no peer
   * withholding.
   */
  @Test
  void aSeedTheCapabilityGateRefusedIsRunAgainOnceThePeerAdvertises() {
    final RecordingSeed seed = new RecordingSeed();
    final Withholding withholding = new Withholding();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true), withholding);

    withholding.peers = List.of("arcadedb-0");
    seed.failures = List.of("groups", "API tokens");
    assertThat(seeder.seedNowAndReport("arcadedb-1 rejoining the cluster as an existing member", 1_000L, false))
        .containsExactly("groups", "API tokens");
    assertThat(seed.calls()).isEqualTo(1);
    assertThat(seeder.owedSeedReasonForTest()).isEqualTo("arcadedb-1 rejoining the cluster as an existing member");

    assertThat(seeder.rerunOwedSeedIfDue()).as("arcadedb-0 is still booting: the gate would refuse again").isFalse();
    assertThat(seed.calls()).as("no seed is spent probing a peer that is still down").isEqualTo(1);
    assertThat(seeder.owedSeedReasonForTest()).as("and it stays owed").isNotNull();

    withholding.peers = List.of();
    seed.failures = List.of();
    assertThat(seeder.rerunOwedSeedIfDue()).as("every peer advertises: the owed seed is run again").isTrue();
    assertThat(seed.calls()).isEqualTo(2);
    assertThat(seeder.lastRunReasonForTest()).as("under the reason of the seed it pays")
        .isEqualTo("arcadedb-1 rejoining the cluster as an existing member");
    assertThat(seeder.owedSeedReasonForTest()).as("it committed, so nothing is owed").isNull();

    assertThat(seeder.rerunOwedSeedIfDue()).as("a paid seed is not run a third time").isFalse();
    assertThat(seed.calls()).isEqualTo(2);
  }

  // Regression for #13447: a seed that committed owes nothing
  @Test
  void aSeedThatCommittedOwesNothing() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true), new Withholding());

    assertThat(seeder.seedNowAndReport("an admission", 1_000L, false)).isEmpty();

    assertThat(seeder.owedSeedReasonForTest()).isNull();
    assertThat(seeder.rerunOwedSeedIfDue()).isFalse();
    assertThat(seed.calls()).isEqualTo(1);
  }

  // Regression for #13447: any seed that commits pays the one that was owed
  /** A seed carries the documents as they are when it runs, so the next one that commits covers the one that failed. */
  @Test
  void anySeedThatCommitsPaysTheOneThatWasOwed() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true), new Withholding());

    seed.failures = List.of("groups");
    seeder.seedNowAndReport("a catch-up", 1_000L, false);
    assertThat(seeder.owedSeedReasonForTest()).isEqualTo("a catch-up");

    seed.failures = List.of();
    seeder.seedNowAndReport("the retry of that catch-up", 1_000L, false);

    assertThat(seeder.owedSeedReasonForTest()).isNull();
    assertThat(seeder.rerunOwedSeedIfDue()).isFalse();
    assertThat(seed.calls()).isEqualTo(2);
  }

  // Regression for #13447: the re-runs of an owed seed are bounded
  /**
   * A re-run starts only when no peer withholds a capability, so one that fails again failed for another reason. It is
   * tried {@link MembershipSecuritySeeder#OWED_SEED_RERUNS} times and then left to an operator, rather than run on every
   * capability round of a cluster that cannot commit.
   */
  @Test
  void theReRunsOfAnOwedSeedAreBounded() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true), new Withholding());
    seed.failures = List.of("users", "groups", "API tokens");

    seeder.seedNowAndReport("a catch-up", 1_000L, false);
    int reruns = 0;
    while (seeder.rerunOwedSeedIfDue())
      assertThat(++reruns).as("the re-runs must end").isLessThanOrEqualTo(MembershipSecuritySeeder.OWED_SEED_RERUNS);

    assertThat(reruns).isEqualTo(MembershipSecuritySeeder.OWED_SEED_RERUNS);
    assertThat(seed.calls()).isEqualTo(1 + MembershipSecuritySeeder.OWED_SEED_RERUNS);
    assertThat(seeder.owedSeedReasonForTest()).as("dropped once the re-runs are spent").isNull();
  }

  // Regression for #13447: the re-runs of an owed seed are bounded when they throw
  /** A re-run that throws reports no documents, so it must be counted where it is started, not where it fails. */
  @Test
  void theReRunsOfAnOwedSeedAreBoundedWhenTheyThrow() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, new AtomicBoolean(true), new Withholding());
    seed.failures = List.of("groups");
    seeder.seedNowAndReport("a catch-up", 1_000L, false);

    seed.blowUp = new IllegalStateException("this node has no security store to seed the joining peer from");
    int reruns = 0;
    while (seeder.rerunOwedSeedIfDue())
      assertThat(++reruns).as("the re-runs must end").isLessThanOrEqualTo(MembershipSecuritySeeder.OWED_SEED_RERUNS);

    assertThat(reruns).isEqualTo(MembershipSecuritySeeder.OWED_SEED_RERUNS);
    assertThat(seed.calls()).isEqualTo(1 + MembershipSecuritySeeder.OWED_SEED_RERUNS);
    assertThat(seeder.owedSeedReasonForTest()).as("dropped once the re-runs are spent").isNull();
  }

  // Regression for #13447: a node that no longer leads drops the seed it owed
  /**
   * A seed publishes the leader's documents. A node that lost the leadership must not publish its own later, when it
   * may lead again: the node the seed was for asks the new leader itself.
   */
  @Test
  void aNodeThatNoLongerLeadsDropsTheSeedItOwed() {
    final RecordingSeed seed = new RecordingSeed();
    final AtomicBoolean leader = new AtomicBoolean(true);
    final MembershipSecuritySeeder seeder = seeder(seed, leader, new Withholding());
    seed.failures = List.of("groups");
    seeder.seedNowAndReport("a catch-up", 1_000L, false);

    leader.set(false);
    assertThat(seeder.rerunOwedSeedIfDue()).isFalse();
    assertThat(seeder.owedSeedReasonForTest()).isNull();

    leader.set(true);
    assertThat(seeder.rerunOwedSeedIfDue()).as("regaining the leadership does not bring it back").isFalse();
    assertThat(seed.calls()).isEqualTo(1);
  }

  // Regression for #13447: an owed seed is not run again while one is outstanding
  @Test
  void anOwedSeedIsNotRunAgainWhileOneIsOutstanding() throws Exception {
    final CountDownLatch secondStarted = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicInteger calls = new AtomicInteger();
    final MembershipSecuritySeeder.SecuritySeed seed = budget -> {
      if (calls.incrementAndGet() == 1)
        return List.of("groups");
      secondStarted.countDown();
      try {
        release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return List.of();
    };
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 1_000L, seed);
    try {
      assertThat(seeder.seedNowAndReport("a catch-up", TIMEOUT_MS, false)).containsExactly("groups");

      final CompletableFuture<List<String>> outstanding = seeder.scheduleForTest("the catch-up asking again");
      assertThat(secondStarted.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(seeder.rerunOwedSeedIfDue()).as("the seed in flight decides whether anything is still owed").isFalse();

      release.countDown();
      assertThat(outstanding.get(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isEmpty();
      assertThat(seeder.owedSeedReasonForTest()).isNull();
      assertThat(calls.get()).isEqualTo(2);
    } finally {
      release.countDown();
      seeder.close();
    }
  }

  // Regression for #13447: the failure names the peer the gate waited for, and promises only what happens
  /**
   * The line used to blame "the peer that just joined" and say the refusal "clears itself once the peer answers". The
   * peer was another one, restarting, and nothing ran the seed again.
   */
  @Test
  void theFailureNamesThePeerTheGateWaitedForAndPromisesOnlyWhatHappens() {
    assertThat(MembershipSecuritySeeder.causeOf(List.of("arcadedb-0")))
        .contains("[arcadedb-0]").contains("issue #7511").contains("need not be the node this seed was for")
        .doesNotContain("just joined").doesNotContain("clears itself");
    assertThat(MembershipSecuritySeeder.causeOf(List.of()))
        .contains("no quorum").contains("arcadedb.ha.securitySeedRetryTimeout").doesNotContain("Peer(s)");

    assertThat(MembershipSecuritySeeder.remedyOf(true)).contains("runs the seed again").contains("any other member");
    assertThat(MembershipSecuritySeeder.remedyOf(false)).contains("stops here").contains("any other member")
        .doesNotContain("runs the seed again");
  }
}
