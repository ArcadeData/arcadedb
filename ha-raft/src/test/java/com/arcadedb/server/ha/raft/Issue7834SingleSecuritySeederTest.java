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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7834SingleSecuritySeederTest {

  private static final long BUDGET_MS  = 3_000L;
  private static final long TIMEOUT_MS = 10_000L;

  private static List<RaftPeerId> peers(final String... ids) {
    final List<RaftPeerId> list = new ArrayList<>(ids.length);
    for (final String id : ids)
      list.add(RaftPeerId.valueOf(id));
    return list;
  }

  /** Runs the seed on the calling thread, so the assertions are about the decision rather than about timing. */
  private static final Executor SAME_THREAD = Runnable::run;

  /** Stands in for {@code ServerSecurity.seedSecurityStateClusterWide}, counting how often the cluster is seeded. */
  private static class RecordingSeed implements MembershipSecuritySeeder.SecuritySeed {
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

  private static MembershipSecuritySeeder seeder(final RecordingSeed seed, final Executor executor) {
    return new MembershipSecuritySeeder(() -> true, () -> BUDGET_MS, seed, executor);
  }

  // -------------------------------------------------------------------------------------------
  // The report the admitting node reads instead of running a seed of its own
  // -------------------------------------------------------------------------------------------

  /** The contract issue #7521 made operator-facing, now answered by the seeder rather than by a second seed. */
  @Test
  void theOutcomeOfTheSeedIsReportedToTheAdmittingNode() {
    final RecordingSeed seed = new RecordingSeed();
    seed.failures = List.of("groups", "API tokens");

    assertThat(seeder(seed, SAME_THREAD).seedNowAndReport("a test request", TIMEOUT_MS, false))
        .as("a caller that has an operator waiting must learn which documents did not commit")
        .containsExactly("groups", "API tokens");
  }

  /** And the success answer: nothing failed, which is what turns into a 200 on the admission route. */
  @Test
  void aSeedThatCommittedEverythingReportsNoFailures() {
    assertThat(seeder(new RecordingSeed(), SAME_THREAD).seedNowAndReport("a test request", TIMEOUT_MS, false)).isEmpty();
  }

  /**
   * A seed that could not run at all must NOT be reported as a seed that failed no documents - the admitting
   * node would answer 200 for a peer nothing was replicated to.
   */
  @Test
  void aSeedThatThrewIsNotReportedAsHavingFailedNothing() {
    final RecordingSeed seed = new RecordingSeed();
    seed.blowUp = new IllegalStateException("no security store on this node");

    assertThatThrownBy(() -> seeder(seed, SAME_THREAD).seedNowAndReport("a test request", TIMEOUT_MS, false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no security store on this node");
  }

  // -------------------------------------------------------------------------------------------
  // One seed per admission
  // -------------------------------------------------------------------------------------------

  /**
   * The defect, in the shape it takes on the leader: the membership change schedules a seed, and the admitting
   * node's request arrives while that seed is still running. The request must take THAT seed's outcome rather
   * than adding a second one - which is what makes the cluster's seed count one per admission instead of two.
   */
  @Test
  void aRequestArrivingWhileASeedIsRunningFoldsIntoItRatherThanAddingASecond() throws Exception {
    final RecordingSeed seed = new RecordingSeed();
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

  /**
   * The other half of the duplicate, which the fold alone does not catch: the admitting node's request is a
   * round trip behind the configuration entry, so the membership seed can FINISH before it arrives. Scheduling
   * a second seed there would put the same three Raft entries in the log again (CodeRabbit on PR #7854), so a
   * seed that has just finished is reported instead.
   */
  @Test
  void aRequestArrivingJustAfterASeedFinishedReportsItRatherThanRunningAnother() {
    final RecordingSeed seed = new RecordingSeed();
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

  /**
   * The reason a caller gives is the one the run reports under. It used to be hardcoded to an admission's
   * phrasing, which the issue #7833 catch-up - the one caller with no concurrent membership seed to fold into,
   * so the one whose string actually reaches the log - then reported itself as (claude-review on PR #7854).
   */
  @Test
  void theCallersOwnReasonIsWhatTheRunIsScheduledUnder() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.seedNowAndReport("this node rejoining the cluster as an existing member", TIMEOUT_MS, false);

    assertThat(seeder.lastRunReasonForTest())
        .as("the run must be reported under what its caller asked for, not a hardcoded admission")
        .isEqualTo("this node rejoining the cluster as an existing member");
  }

  /** And the membership hook's own reason survives the same way, naming the peers that joined. */
  @Test
  void theMembershipHooksReasonNamesThePeersThatJoined() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));

    assertThat(seeder.lastRunReasonForTest()).contains("arcadedb-2").contains("Raft configuration");
  }

  /**
   * The reuse is for an admission and for nothing else. A catch-up whose fingerprints did NOT match has just
   * been told this node is out of step, so answering it from an unrelated seed that happened to finish a
   * moment ago would leave it stale - which is the failure issue #7833 exists to repair (CodeRabbit on PR
   * #7854).
   */
  @Test
  void aCallerThatHasEstablishedItNeedsASeedIsNeverAnsweredByARecentOne() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    // A seed finishes; a catch-up request arrives immediately after it, well inside the reuse window.
    seeder.seedNowAndReport("an unrelated seed", TIMEOUT_MS, true);
    assertThat(seed.calls.get()).isEqualTo(1);

    seeder.seedNowAndReport("a node whose fingerprints did not match", TIMEOUT_MS, false);

    assertThat(seed.calls.get())
        .as("a node that has been told it is out of step must actually be seeded")
        .isEqualTo(2);
  }

  /** And the admission, in the same window, still is - that is what the window is for. */
  @Test
  void anAdmissionInTheSameWindowIsStillAnsweredByTheRecentSeed() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.seedNowAndReport("the membership change", TIMEOUT_MS, true);
    seeder.seedNowAndReport("the admission a round trip behind it", TIMEOUT_MS, true);

    assertThat(seed.calls.get()).isEqualTo(1);
  }

  /**
   * And the converse, so the reuse is not mistaken for "only ever seeds once": once the window has passed, a
   * request runs a seed of its own. That is the path a node re-seeding itself takes (issue #7833), and the
   * path an operator re-POSTing a peer minutes later has to get.
   */
  @Test
  void aRequestPastTheReuseWindowRunsAFreshSeed() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.seedNowAndReport("a first admission", TIMEOUT_MS, true);
    seeder.forgetCompletedSeedForTest();
    seeder.seedNowAndReport("an admission past the window", TIMEOUT_MS, true);

    assertThat(seed.calls.get())
        .as("a request that cannot reuse anything is a seed of its own")
        .isEqualTo(2);
  }

  /**
   * A membership change is still seeded without anybody asking - the admitting node is not the only caller, and
   * {@code KubernetesAutoJoin} has none at all (issue #7531).
   */
  @Test
  void theMembershipHookStillSeedsOnItsOwn() {
    final RecordingSeed seed = new RecordingSeed();
    final MembershipSecuritySeeder seeder = seeder(seed, SAME_THREAD);

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));

    assertThat(seed.calls.get()).isEqualTo(1);
  }

  /** A node being torn down tells its reporting caller so, instead of leaving it to wait out its timeout. */
  @Test
  void closingTheSeederFailsAReportRatherThanLeavingItParked() throws Exception {
    final RecordingSeed seed = new RecordingSeed();
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

  /** The leader gate is unchanged: a follower applying the same configuration entry seeds nothing. */
  @Test
  void aFollowerStillSeedsNothingOnAMembershipChange() {
    final RecordingSeed seed = new RecordingSeed();
    final AtomicBoolean leader = new AtomicBoolean(false);
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(leader::get, () -> BUDGET_MS, seed,
        SAME_THREAD);

    seeder.onConfigurationChanged(1, 10, peers("arcadedb-0", "arcadedb-1"));
    seeder.onConfigurationChanged(1, 11, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));

    assertThat(seed.calls.get()).isZero();
  }
}
