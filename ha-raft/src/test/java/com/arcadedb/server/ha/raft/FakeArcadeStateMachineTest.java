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

import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9464: the defaults {@link FakeArcadeStateMachine} starts from are those of a fresh {@link ArcadeStateMachine} -
 * which is also what lets a test stubbing only those defaults use a plain one - and the values set override them.
 */
class FakeArcadeStateMachineTest {
  @Test
  void aFreshStateMachineIsIdle() {
    final ArcadeStateMachine fresh = new ArcadeStateMachine();

    assertThat(fresh.isCatchingUp()).isFalse();
    assertThat(fresh.isSnapshotDownloadPending()).isFalse();
    assertThat(fresh.isResyncInProgress()).isFalse();
    assertThat(fresh.isHaltedAfterCriticalError()).isFalse();
  }

  @Test
  void theFakeStartsIdleAndAnswersWhatWasSet() {
    final FakeArcadeStateMachine sm = new FakeArcadeStateMachine();
    assertThat(sm.isCatchingUp()).isFalse();
    assertThat(sm.getLastAppliedTermIndex()).isEqualTo(new ArcadeStateMachine().getLastAppliedTermIndex());

    sm.catchingUp(true).snapshotDownloadPending(true).resyncInProgress(true).haltedAfterCriticalError(true)
        .lastAppliedTermIndex(TermIndex.valueOf(8, 228_631));

    assertThat(sm.isCatchingUp()).isTrue();
    assertThat(sm.isSnapshotDownloadPending()).isTrue();
    assertThat(sm.isResyncInProgress()).isTrue();
    assertThat(sm.isHaltedAfterCriticalError()).isTrue();
    assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(8, 228_631));
  }

  @Test
  void unansweredQueriesAnswerForRealAndActionsOnlyRecord() {
    final FakeArcadeStateMachine sm = new FakeArcadeStateMachine();

    assertThat(sm.hasLeaderServiceGap()).as("a fresh state machine holds no gap").isFalse();
    assertThat(sm.isBootstrapInstallInFlight("db")).isFalse();
    assertThat(sm.handOffLeadershipWhileReplacingDatabase()).as("wired to no Raft server, nothing is handed off").isFalse();
    sm.resyncDatabaseFromLeader("db");

    assertThat(sm.calls("handOffLeadershipWhileReplacingDatabase")).hasSize(1);
    assertThat(sm.calls("resyncDatabaseFromLeader")).as("the one-argument form is recorded once, with no order")
        .containsExactly(Arrays.asList("db", null));
  }

  @Test
  void callsAreAnsweredAndFailuresInjected() {
    final StalledResyncOrder order = new StalledResyncOrder(3, 10, 20);
    final FakeArcadeStateMachine sm = new FakeArcadeStateMachine()
        .returns("hasLeaderServiceGap", true)
        .on("isBootstrapInstallInFlight", args -> "replaced".equals(args[0]))
        .fails("resyncDatabaseFromLeader", new StaleResyncOrderException("not behind"));

    assertThat(sm.hasLeaderServiceGap()).isTrue();
    assertThat(sm.isBootstrapInstallInFlight("replaced")).isTrue();
    assertThat(sm.isBootstrapInstallInFlight("other")).isFalse();
    assertThatThrownBy(() -> sm.resyncDatabaseFromLeader("db", order)).isInstanceOf(StaleResyncOrderException.class);
    assertThat(sm.calls("resyncDatabaseFromLeader")).containsExactly(List.of("db", order));
  }

  @Test
  void aNonBooleanAnswerIsRefusedByName() {
    final FakeArcadeStateMachine sm = new FakeArcadeStateMachine().returns("hasLeaderServiceGap", null);

    assertThatThrownBy(sm::hasLeaderServiceGap).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("hasLeaderServiceGap");
  }
}
