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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.FakeArcadeDBServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DatabaseReconciler#classifyReconcile} (issue #4727): the pure set-reconciliation that
 * decides which leader databases a joining node must acquire, refresh, or flag as leader-missing. Keeping this
 * logic pure makes the union/difference behavior - and the LEADER_MISSING transitions behind the cluster alert -
 * deterministically testable without a Raft cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatabaseReconcilerTest {

  @Test
  void emptyNodeAcquiresEverythingTheLeaderHolds() {
    final var plan = DatabaseReconciler.classifyReconcile(Set.of("a", "b", "c"), Set.of());
    assertThat(plan.toAcquire()).containsExactly("a", "b", "c");
    assertThat(plan.toRefresh()).isEmpty();
    assertThat(plan.leaderMissing()).isEmpty();
  }

  @Test
  void fullyReplicatedNodeRefreshesEverythingAndAcquiresNothing() {
    final var plan = DatabaseReconciler.classifyReconcile(Set.of("a", "b"), Set.of("a", "b"));
    assertThat(plan.toAcquire()).isEmpty();
    assertThat(plan.toRefresh()).containsExactly("a", "b");
    assertThat(plan.leaderMissing()).isEmpty();
  }

  @Test
  void mixedSetSplitsIntoAcquireRefreshAndLeaderMissing() {
    // leader: a, b, c ; local: b, c, d  -> acquire a ; refresh b, c ; leader-missing d
    final var plan = DatabaseReconciler.classifyReconcile(Set.of("a", "b", "c"), Set.of("b", "c", "d"));
    assertThat(plan.toAcquire()).containsExactly("a");
    assertThat(plan.toRefresh()).containsExactly("b", "c");
    assertThat(plan.leaderMissing()).containsExactly("d");
  }

  @Test
  void databaseOnlyOnThisNodeIsFlaggedLeaderMissing() {
    final var plan = DatabaseReconciler.classifyReconcile(Set.of(), Set.of("orphan"));
    assertThat(plan.toAcquire()).isEmpty();
    assertThat(plan.toRefresh()).isEmpty();
    assertThat(plan.leaderMissing()).containsExactly("orphan");
  }

  @Test
  void outputListsAreSortedForDeterminism() {
    final var plan = DatabaseReconciler.classifyReconcile(Set.of("zeta", "alpha", "mike"), Set.of("alpha"));
    assertThat(plan.toAcquire()).containsExactly("mike", "zeta");
    assertThat(plan.toRefresh()).containsExactly("alpha");
    assertThat(plan.toAcquire()).isSorted();
  }

  // ---- execution: a failing acquire must not starve refresh of healthy databases ----

  @Test
  void oneFailingAcquireDoesNotStarveRefreshOfHealthyDatabases() {
    // leader: acquire "bad" (fails validation) + "goodnew" ; refresh "healthy"
    final var plan = new DatabaseReconciler.ReconcilePlan(List.of("bad", "goodnew"), List.of("healthy"), List.of());
    final List<String> acquireCalls = new ArrayList<>();
    final List<String> refreshCalls = new ArrayList<>();

    final var outcome = DatabaseReconciler.executeReconcilePlan(plan,
        db -> {
          acquireCalls.add(db);
          if ("bad".equals(db))
            throw new IOException("corrupt snapshot");
        },
        refreshCalls::add);

    // The bad acquire failed, but the healthy database was still refreshed and the good new one acquired.
    assertThat(refreshCalls).containsExactly("healthy");
    assertThat(acquireCalls).containsExactlyInAnyOrder("bad", "goodnew");
    assertThat(outcome.refreshed()).containsExactly("healthy");
    assertThat(outcome.acquired()).containsExactly("goodnew");
    assertThat(outcome.acquireFailures()).containsKey("bad");
    assertThat(outcome.refreshFailures()).isEmpty();
  }

  @Test
  void runtimeExceptionFromOneDatabaseIsIsolatedToo() {
    // An unchecked exception (not just IOException) from one acquire must not abort the others.
    final var plan = new DatabaseReconciler.ReconcilePlan(List.of("npe", "goodnew"), List.of("healthy"), List.of());
    final List<String> refreshCalls = new ArrayList<>();
    final var outcome = DatabaseReconciler.executeReconcilePlan(plan,
        db -> {
          if ("npe".equals(db))
            throw new RuntimeException("unexpected");
        },
        refreshCalls::add);
    assertThat(outcome.acquired()).containsExactly("goodnew");
    assertThat(outcome.acquireFailures()).containsKey("npe");
    assertThat(refreshCalls).containsExactly("healthy");
    assertThat(outcome.refreshed()).containsExactly("healthy");
  }

  @Test
  void allSucceedWhenNoOperationFails() {
    final var plan = new DatabaseReconciler.ReconcilePlan(List.of("n1"), List.of("e1", "e2"), List.of("orphan"));
    final var outcome = DatabaseReconciler.executeReconcilePlan(plan, db -> {
    }, db -> {
    });
    assertThat(outcome.acquired()).containsExactly("n1");
    assertThat(outcome.refreshed()).containsExactly("e1", "e2");
    assertThat(outcome.acquireFailures()).isEmpty();
    assertThat(outcome.refreshFailures()).isEmpty();
  }

  // ---- bookkeeping: applyReconcileOutcome status transitions + give-up decision ----

  private static DatabaseReconciler.ReconcileOutcome outcome(final List<String> acquired, final List<String> acquireFailures,
      final List<String> refreshed, final List<String> refreshFailures) {
    final Map<String, String> af = new LinkedHashMap<>();
    for (final String d : acquireFailures)
      af.put(d, "boom");
    final Map<String, String> rf = new LinkedHashMap<>();
    for (final String d : refreshFailures)
      rf.put(d, "boom");
    return new DatabaseReconciler.ReconcileOutcome(acquired, af, refreshed, rf);
  }

  @Test
  void outcomeSetsAcquiredFailedAndLeaderMissingStatuses() {
    final var reconciler = new DatabaseReconciler();
    final var plan = new DatabaseReconciler.ReconcilePlan(List.of("newok", "newbad"), List.of(), List.of("orphan"));

    final var verdict = reconciler.applyReconcileOutcome(plan, outcome(List.of("newok"), List.of("newbad"), List.of(), List.of()));

    assertThat(reconciler.getAcquireStatus("newok").state()).isEqualTo(DatabaseReconciler.AcquireState.ACQUIRED);
    assertThat(reconciler.getAcquireStatus("newbad").state()).isEqualTo(DatabaseReconciler.AcquireState.FAILED);
    assertThat(reconciler.getAcquireStatus("orphan").state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
    assertThat(verdict.retryWorthwhile()).as("a fresh failure is still within the retry budget").isTrue();
    assertThat(verdict.givenUp()).as("nothing has been given up yet").isEmpty();
  }

  @Test
  void refreshClearsAStaleLeaderMissingStatus() {
    final var reconciler = new DatabaseReconciler();
    // First pass: "db" is local-only, flagged LEADER_MISSING.
    reconciler.applyReconcileOutcome(new DatabaseReconciler.ReconcilePlan(List.of(), List.of(), List.of("db")),
        outcome(List.of(), List.of(), List.of(), List.of()));
    assertThat(reconciler.getAcquireStatus("db").state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);

    // Second pass: the leader now holds it, so it is refreshed -> the stale status must clear.
    reconciler.applyReconcileOutcome(new DatabaseReconciler.ReconcilePlan(List.of(), List.of("db"), List.of()),
        outcome(List.of(), List.of(), List.of("db"), List.of()));
    assertThat(reconciler.getAcquireStatus("db")).isNull();
  }

  // ---- issue #4799: a fresh/empty follower must not ACK the snapshot index when the leader's database list
  //      could not be enumerated (auto-acquire fallback). It must fail the install so Ratis retries. ----

  @Test
  void freshEmptyFollowerFailsInstallWhenLeaderListUnavailable() {
    // No local user databases and the leader's list is unavailable: ACKing would install nothing yet advance the
    // applied index (the data gap the gap-detector cannot catch). Must fail the install so Ratis retries.
    assertThat(DatabaseReconciler.mustFailInstallWhenLeaderListUnavailable(Set.of())).isTrue();
  }

  @Test
  void populatedFollowerKeepsLegacyRefreshWhenLeaderListUnavailable() {
    // The node already holds databases: keep the best-effort legacy refresh (do not fail the install); the
    // missing databases are retried on the next reconcile.
    assertThat(DatabaseReconciler.mustFailInstallWhenLeaderListUnavailable(Set.of("existing"))).isFalse();
    assertThat(DatabaseReconciler.mustFailInstallWhenLeaderListUnavailable(Set.of("a", "b"))).isFalse();
  }

  @Test
  void persistentFailureStopsForcingRetryAfterGiveUpThreshold() {
    final var reconciler = new DatabaseReconciler();
    final var plan = new DatabaseReconciler.ReconcilePlan(List.of("bad"), List.of(), List.of());
    final var failing = outcome(List.of(), List.of("bad"), List.of(), List.of());

    // Within the budget the failure keeps forcing a retry; once the threshold is hit it stops.
    assertThat(reconciler.applyReconcileOutcome(plan, failing).retryWorthwhile()).isTrue();  // 1
    assertThat(reconciler.applyReconcileOutcome(plan, failing).retryWorthwhile()).isTrue();  // 2
    final var giveUp = reconciler.applyReconcileOutcome(plan, failing);                      // 3 -> give up
    assertThat(giveUp.retryWorthwhile()).isFalse();
    assertThat(reconciler.getAcquireStatus("bad").state()).isEqualTo(DatabaseReconciler.AcquireState.FAILED);

    // Issue #6760: giving up on the RETRY must not be reported as "installed". The database has to come back in
    // the verdict so the install does not record it at the snapshot index.
    assertThat(giveUp.givenUp()).containsExactly("bad");

    // A later success resets the counter (and would let it retry again if it failed afresh).
    reconciler.applyReconcileOutcome(new DatabaseReconciler.ReconcilePlan(List.of("bad"), List.of(), List.of()),
        outcome(List.of("bad"), List.of(), List.of(), List.of()));
    assertThat(reconciler.getAcquireStatus("bad").state()).isEqualTo(DatabaseReconciler.AcquireState.ACQUIRED);
    final var afterReset = reconciler.applyReconcileOutcome(plan, failing);
    assertThat(afterReset.retryWorthwhile()).as("counter reset after success").isTrue();
    assertThat(afterReset.givenUp()).as("back inside the budget, so nothing is given up").isEmpty();
  }

  /**
   * Issue #6760: only the databases that exhausted their budget are reported, and one healthy database failing
   * afresh alongside them still forces the retry.
   */
  @Test
  void onlyTheExhaustedDatabasesAreReportedAsGivenUp() {
    final var reconciler = new DatabaseReconciler();
    final var plan = new DatabaseReconciler.ReconcilePlan(List.of(), List.of("stubborn", "flaky"), List.of());

    reconciler.applyReconcileOutcome(plan, outcome(List.of(), List.of(), List.of(), List.of("stubborn")));
    reconciler.applyReconcileOutcome(plan, outcome(List.of(), List.of(), List.of(), List.of("stubborn")));

    final var verdict = reconciler.applyReconcileOutcome(plan,
        outcome(List.of(), List.of(), List.of(), List.of("stubborn", "flaky")));

    assertThat(verdict.givenUp()).containsExactly("stubborn");
    assertThat(verdict.retryWorthwhile()).as("'flaky' is on its first failure, so the install must still retry").isTrue();
  }

  /**
   * Regression tests for issue #8374: a leader-driven snapshot install registered an approximate boundary term
   * whenever {@link DatabaseReconciler#reconcileDatabasesFromLeader} did not get the leader's own Raft snapshot marker
   * (issue #8360) - always with {@link GlobalConfiguration#HA_AUTO_ACQUIRE_DATABASES} off, and whenever the
   * bootstrap-state call failed. A wrong term is never revisited: Ratis answers the leader's next notification at the
   * same {@code firstAvailableLogIndex} with {@code ALREADY_INSTALLED} without calling the state machine again.
   * <p>
   * The fix reads the marker on every path, with a marker-only request where the database list is not needed, and
   * FAILS the install when the leader cannot be asked, so Ratis re-drives it and the next attempt asks again.
   */
  @Nested
  class ReconcileMarkerIndependentOfAutoAcquire {

    private static final String   LEADER_HTTP   = "leader:2480";
    private static final String   LEADER_HTTPS  = null;
    private static final String   CLUSTER_TOKEN = null;
    private static final TermIndex MARKER       = TermIndex.valueOf(7L, 41L);

    // ---- auto-acquire off: the legacy refresh path must read the marker, with the cheap request ----

    // Regression for #8374: ReconcileMarkerIndependentOfAutoAcquire
    @Test
    void autoAcquireDisabledReadsTheMarkerWithTheMarkerOnlyRequest() throws Exception {
      final StubReconciler reconciler = new StubReconciler(null, marker(MARKER));
      configure(reconciler, false, Set.of());

      final DatabaseReconciler.ReconcileFromLeaderResult result =
          reconciler.reconcileDatabasesFromLeader("leader", LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN, -1L);

      assertThat(result.leaderSnapshotTermIndex())
          .as("auto-acquire off must not mean the marker is never read (issue #8374)")
          .isEqualTo(MARKER);
      assertThat(result.notInstalled()).isEmpty();
      assertThat(reconciler.fullCalls).as("the database list is not needed here, so no fingerprinting").isZero();
      assertThat(reconciler.markerCalls).isEqualTo(1);
    }

    // Regression for #8374: ReconcileMarkerIndependentOfAutoAcquire
    @Test
    void autoAcquireDisabledFailsTheInstallWhenTheMarkerCannotBeRead() {
      final StubReconciler reconciler = new StubReconciler(null, new IOException("leader unreachable"));
      configure(reconciler, false, Set.of("db"));

      assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader("leader", LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN, -1L))
          .as("a guessed term is never revisited, so the install must fail and let Ratis re-drive it")
          .isInstanceOf(IOException.class)
          .hasMessageContaining("#8374");
    }

    // Regression for #8374: ReconcileMarkerIndependentOfAutoAcquire
    @Test
    void autoAcquireDisabledPreservesTheInterruptAndFailsTheInstall() {
      final StubReconciler reconciler = new StubReconciler(null, new InterruptedException("shutting down"));
      configure(reconciler, false, Set.of("db"));

      try {
        assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader("leader", LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN, -1L))
            .isInstanceOf(IOException.class);
        assertThat(Thread.currentThread().isInterrupted()).as("the interrupt must be preserved, not swallowed").isTrue();
      } finally {
        Thread.interrupted();
      }
    }

    // Regression for #8374: ReconcileMarkerIndependentOfAutoAcquire
    @Test
    void autoAcquireDisabledCarriesNullFromALeaderThatPredatesTheField() throws Exception {
      // A leader build older than #8360 answers without snapshotTerm/snapshotIndex. Refusing would stall every follower
      // upgraded ahead of its leader, so the install proceeds and the state machine falls back to the approximation.
      final StubReconciler reconciler = new StubReconciler(null, marker(null));
      configure(reconciler, false, Set.of());

      assertThat(reconciler.reconcileDatabasesFromLeader("leader", LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN, -1L).leaderSnapshotTermIndex())
          .isNull();
    }

    // ---- auto-acquire on ----

    // Regression for #8374: ReconcileMarkerIndependentOfAutoAcquire
    @Test
    void autoAcquireEnabledCarriesTheMarkerFromTheFullListing() throws Exception {
      final StubReconciler reconciler = new StubReconciler(marker(MARKER), new AssertionError("marker-only read not expected"));
      configure(reconciler, true, Set.of());

      final DatabaseReconciler.ReconcileFromLeaderResult result =
          reconciler.reconcileDatabasesFromLeader("leader", LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN, -1L);

      assertThat(result.leaderSnapshotTermIndex()).isEqualTo(MARKER);
      assertThat(reconciler.markerCalls).isZero();
    }

    // Regression for #8374: ReconcileMarkerIndependentOfAutoAcquire
    @Test
    void autoAcquireEnabledReadsTheMarkerAloneWhenTheFullListingFails() throws Exception {
      // The full listing fingerprints every database and can time out where the marker-only read does not. A populated
      // follower keeps the legacy refresh (issue #4799), and now also the marker.
      final StubReconciler reconciler = new StubReconciler(new IOException("listing timed out"), marker(MARKER));
      configure(reconciler, true, Set.of("db"));

      final DatabaseReconciler.ReconcileFromLeaderResult result =
          reconciler.reconcileDatabasesFromLeader("leader", LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN, -1L);

      assertThat(result.leaderSnapshotTermIndex())
          .as("the listing failure used to register the approximate term (issue #8374)")
          .isEqualTo(MARKER);
      assertThat(reconciler.markerCalls).isEqualTo(1);
    }

    // Regression for #8374: ReconcileMarkerIndependentOfAutoAcquire
    @Test
    void autoAcquireEnabledFailsTheInstallWhenNeitherReadSucceeds() {
      final StubReconciler reconciler = new StubReconciler(new IOException("listing failed"), new IOException("marker failed"));
      configure(reconciler, true, Set.of("db"));

      assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader("leader", LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN, -1L))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("#8374");
    }

    // Regression for #8374: ReconcileMarkerIndependentOfAutoAcquire
    @Test
    void autoAcquireEnabledStillFailsTheInstallOnAnEmptyFollower() {
      // Issue #4799 is unchanged: an empty follower that cannot list the leader's databases must not ACK the index.
      final StubReconciler reconciler = new StubReconciler(new IOException("listing failed"), marker(MARKER));
      configure(reconciler, true, Set.of());

      assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader("leader", LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN, -1L))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("#4799");
      assertThat(reconciler.markerCalls).as("the refusal is certain, so no round trip is spent on the marker").isZero();
    }

    // Regression for #8374: ReconcileMarkerIndependentOfAutoAcquire
    @Test
    void autoAcquireEnabledFailsTheInstallWhenTheListingIsInterrupted() {
      final StubReconciler reconciler = new StubReconciler(new InterruptedException("shutting down"), marker(MARKER));
      configure(reconciler, true, Set.of("db"));

      try {
        assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader("leader", LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN, -1L))
            .isInstanceOf(IOException.class);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
      } finally {
        Thread.interrupted();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private static LeaderDatabaseQuery.BootstrapState marker(final TermIndex termIndex) {
      return new LeaderDatabaseQuery.BootstrapState(List.of(), termIndex);
    }

    /**
     * Local databases are reported by name but {@code existsDatabase} answers false on the mock, so the refresh loops
     * never reach {@code SnapshotInstaller} and the tests exercise only the marker handling.
     */
    private static void configure(final DatabaseReconciler reconciler, final boolean autoAcquire, final Set<String> localDbs) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, autoAcquire);

      final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, config);
      server.databaseNames(localDbs.toArray(new String[0]));
      reconciler.setServer(server);
    }

    /** Stands in for both network reads; each answer is a canned state or a throwable to raise. */
    private static final class StubReconciler extends DatabaseReconciler {
      private final Object full;
      private final Object markerOnly;
      private int fullCalls;
      private int markerCalls;

      private StubReconciler(final Object full, final Object markerOnly) {
        this.full = full;
        this.markerOnly = markerOnly;
      }

      @Override
      LeaderDatabaseQuery.BootstrapState fetchBootstrapState(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
          final String clusterToken) throws IOException, InterruptedException {
        fullCalls++;
        return answer(full);
      }

      @Override
      LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
          final String clusterToken) throws IOException, InterruptedException {
        markerCalls++;
        return answer(markerOnly);
      }

      private static LeaderDatabaseQuery.BootstrapState answer(final Object answer) throws IOException, InterruptedException {
        if (answer instanceof IOException e)
          throw e;
        if (answer instanceof InterruptedException e)
          throw e;
        if (answer instanceof Error e)
          throw e;
        return (LeaderDatabaseQuery.BootstrapState) answer;
      }
    }
  }
}
