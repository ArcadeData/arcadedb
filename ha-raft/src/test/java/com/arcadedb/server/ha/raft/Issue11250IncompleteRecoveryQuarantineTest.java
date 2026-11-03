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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11250: a node whose own crash recovery stopped short of replaying every transaction (a WAL
 * gap after a power loss) still rejoined the cluster, won an election and answered queries from what the power loss left.
 * <p>
 * The invariant: <b>a database whose local recovery did not complete is quarantined, so the node does not serve it, leads
 * no longer with it, and re-installs it from the cluster.</b> A sole voter has no copy to re-install from, so it is alerted
 * about instead, as for #9308.
 */
class Issue11250IncompleteRecoveryQuarantineTest {
  private static final String DB_NAME = "db11250";

  private final AtomicBoolean      soleVoter = new AtomicBoolean(false);
  private       FakeRaftHAServer   raft;
  private       ArcadeStateMachine sm;

  @BeforeEach
  void setUp() {
    raft = FakeRaftHAServer.detached();
    raft.leader(true);
    raft.on("isSoleVoter", args -> soleVoter.get());
    sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);
  }

  @Test
  void anIncompleteRecoveryQuarantinesTheDatabaseAndHandsLeadershipOff() {
    assertThat(sm.quarantineAfterIncompleteRecovery(DB_NAME)).isTrue();

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
    assertThat(sm.quarantineCause(DB_NAME)).isEqualTo(DivergenceCause.LOCAL_RECOVERY_INCOMPLETE);
    assertThat(sm.isResyncInProgress()).as("the node must leave the ready set").isTrue();
    assertThat(raft.calls("handOffLeadershipToResync")).singleElement()
        .satisfies(args -> assertThat((String) args.get(0)).contains("'" + DB_NAME + "'"));
  }

  @Test
  void aSecondCallDoesNotQuarantineAgain() {
    assertThat(sm.quarantineAfterIncompleteRecovery(DB_NAME)).isTrue();
    assertThat(sm.quarantineAfterIncompleteRecovery(DB_NAME)).isFalse();
    assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
  }

  @Test
  void aSoleVoterIsNotQuarantinedBecauseNothingCouldLiftIt() {
    soleVoter.set(true);

    assertThat(sm.quarantineAfterIncompleteRecovery(DB_NAME)).isFalse();

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    assertThat(sm.isResyncInProgress()).isFalse();
    assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();
  }

  /** #11258: a unique index lost for a missing header page is quarantined like an incomplete recovery. */
  @Test
  void anUnavailableUniqueIndexQuarantinesTheDatabase() {
    assertThat(sm.quarantineAfterUnavailableUniqueIndex(DB_NAME, List.of("idx"))).isTrue();

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
    assertThat(sm.quarantineCause(DB_NAME)).isEqualTo(DivergenceCause.UNAVAILABLE_UNIQUE_INDEX);
    assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
  }

  @Test
  void aSoleVoterIsNotQuarantinedForAnUnavailableUniqueIndex() {
    soleVoter.set(true);

    assertThat(sm.quarantineAfterUnavailableUniqueIndex(DB_NAME, List.of("idx"))).isFalse();
    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
  }

  @Test
  void theScanIsANoOpWithoutAServer() {
    assertThat(sm.quarantineDatabasesWithIncompleteRecovery()).isEmpty();
  }
}
