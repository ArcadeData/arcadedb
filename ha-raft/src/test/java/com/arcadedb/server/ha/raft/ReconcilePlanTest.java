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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ArcadeStateMachine#classifyReconcile} (issue #4727): the pure set-reconciliation that
 * decides which leader databases a joining node must acquire, refresh, or flag as leader-missing. Keeping this
 * logic pure makes the union/difference behavior - and the LEADER_MISSING transitions behind the cluster alert -
 * deterministically testable without a Raft cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReconcilePlanTest {

  @Test
  void emptyNodeAcquiresEverythingTheLeaderHolds() {
    final var plan = ArcadeStateMachine.classifyReconcile(Set.of("a", "b", "c"), Set.of());
    assertThat(plan.toAcquire()).containsExactly("a", "b", "c");
    assertThat(plan.toRefresh()).isEmpty();
    assertThat(plan.leaderMissing()).isEmpty();
  }

  @Test
  void fullyReplicatedNodeRefreshesEverythingAndAcquiresNothing() {
    final var plan = ArcadeStateMachine.classifyReconcile(Set.of("a", "b"), Set.of("a", "b"));
    assertThat(plan.toAcquire()).isEmpty();
    assertThat(plan.toRefresh()).containsExactly("a", "b");
    assertThat(plan.leaderMissing()).isEmpty();
  }

  @Test
  void mixedSetSplitsIntoAcquireRefreshAndLeaderMissing() {
    // leader: a, b, c ; local: b, c, d  -> acquire a ; refresh b, c ; leader-missing d
    final var plan = ArcadeStateMachine.classifyReconcile(Set.of("a", "b", "c"), Set.of("b", "c", "d"));
    assertThat(plan.toAcquire()).containsExactly("a");
    assertThat(plan.toRefresh()).containsExactly("b", "c");
    assertThat(plan.leaderMissing()).containsExactly("d");
  }

  @Test
  void databaseOnlyOnThisNodeIsFlaggedLeaderMissing() {
    final var plan = ArcadeStateMachine.classifyReconcile(Set.of(), Set.of("orphan"));
    assertThat(plan.toAcquire()).isEmpty();
    assertThat(plan.toRefresh()).isEmpty();
    assertThat(plan.leaderMissing()).containsExactly("orphan");
  }

  @Test
  void outputListsAreSortedForDeterminism() {
    final var plan = ArcadeStateMachine.classifyReconcile(Set.of("zeta", "alpha", "mike"), Set.of("alpha"));
    assertThat(plan.toAcquire()).containsExactly("mike", "zeta");
    assertThat(plan.toRefresh()).containsExactly("alpha");
    assertThat(plan.toAcquire()).isSorted();
  }

  // ---- execution: a failing acquire must not starve refresh of healthy databases ----

  @Test
  void oneFailingAcquireDoesNotStarveRefreshOfHealthyDatabases() {
    // leader: acquire "bad" (fails validation) + "goodnew" ; refresh "healthy"
    final var plan = new ArcadeStateMachine.ReconcilePlan(List.of("bad", "goodnew"), List.of("healthy"), List.of());
    final List<String> acquireCalls = new ArrayList<>();
    final List<String> refreshCalls = new ArrayList<>();

    final var outcome = ArcadeStateMachine.executeReconcilePlan(plan,
        db -> {
          acquireCalls.add(db);
          if (db.equals("bad"))
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
  void allSucceedWhenNoOperationFails() {
    final var plan = new ArcadeStateMachine.ReconcilePlan(List.of("n1"), List.of("e1", "e2"), List.of("orphan"));
    final var outcome = ArcadeStateMachine.executeReconcilePlan(plan, db -> {
    }, db -> {
    });
    assertThat(outcome.acquired()).containsExactly("n1");
    assertThat(outcome.refreshed()).containsExactly("e1", "e2");
    assertThat(outcome.acquireFailures()).isEmpty();
    assertThat(outcome.refreshFailures()).isEmpty();
  }
}
