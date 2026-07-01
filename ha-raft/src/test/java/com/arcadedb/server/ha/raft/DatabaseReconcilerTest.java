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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

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

    final boolean retry = reconciler.applyReconcileOutcome(plan, outcome(List.of("newok"), List.of("newbad"), List.of(), List.of()));

    assertThat(reconciler.getAcquireStatus("newok").state()).isEqualTo(DatabaseReconciler.AcquireState.ACQUIRED);
    assertThat(reconciler.getAcquireStatus("newbad").state()).isEqualTo(DatabaseReconciler.AcquireState.FAILED);
    assertThat(reconciler.getAcquireStatus("orphan").state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
    assertThat(retry).as("a fresh failure is still within the retry budget").isTrue();
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
    assertThat(reconciler.applyReconcileOutcome(plan, failing)).isTrue();  // 1
    assertThat(reconciler.applyReconcileOutcome(plan, failing)).isTrue();  // 2
    assertThat(reconciler.applyReconcileOutcome(plan, failing)).isFalse(); // 3 -> give up
    assertThat(reconciler.getAcquireStatus("bad").state()).isEqualTo(DatabaseReconciler.AcquireState.FAILED);

    // A later success resets the counter (and would let it retry again if it failed afresh).
    reconciler.applyReconcileOutcome(new DatabaseReconciler.ReconcilePlan(List.of("bad"), List.of(), List.of()),
        outcome(List.of("bad"), List.of(), List.of(), List.of()));
    assertThat(reconciler.getAcquireStatus("bad").state()).isEqualTo(DatabaseReconciler.AcquireState.ACQUIRED);
    assertThat(reconciler.applyReconcileOutcome(plan, failing)).as("counter reset after success").isTrue();
  }
}
