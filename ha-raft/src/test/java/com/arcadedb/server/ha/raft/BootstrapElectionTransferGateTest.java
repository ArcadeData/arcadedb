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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the leadership-transfer branch of the first-formation gate in
 * {@link BootstrapElection} (issue #5099).
 * <p>
 * When the offline-bootstrap protocol transfers Raft leadership to the elected source, the transfer
 * and the ensuing term bump make Ratis commit internal no-op / configuration entries, so the new
 * leader reads a commit index above {@code 0} (empirically 2) even though no application entry has
 * committed. The exact-{@code 0} gate ({@link BootstrapElection#isConfirmedFirstFormation}) rejected
 * that post-transfer leader and the baseline was never committed. The widened gate accepts a positive
 * commit index when the state machine reports it has never applied an application entry, while still
 * rejecting a genuinely running cluster (issue #4800 preserved).
 */
class BootstrapElectionTransferGateTest {

  /**
   * The core #5099 fix: post-transfer leader with commit index 2 (internal entries only) and a state
   * machine that has never applied an application entry. The gate must OPEN - the protocol moves past
   * it and, with no local databases, reports {@code SKIPPED_NO_DATABASES} rather than the gate outcome.
   */
  @Test
  void postTransferLeaderWithOnlyInternalEntriesEngagesBootstrap() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);

    // No local databases: an open gate is told apart from a closed one by the outcome, SKIPPED_NO_DATABASES being the
    // one returned only after the databases are collected
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(config);

    // A fresh state machine has never applied an application entry
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true)
        .commitIndex(2L) // post-transfer: internal no-op/config entries
        .stateMachine(new ArcadeStateMachine());

    final BootstrapElection election = new BootstrapElection(raft, server);

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.SKIPPED_NO_DATABASES);
  }

  /**
   * A genuinely running cluster: commit index above {@code 0} AND the state machine has applied
   * application entries. The gate must stay CLOSED so bootstrap never overwrites live data (#4800).
   */
  @Test
  void positiveCommitIndexWithCommittedDataIsSkipped() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);

    // A local database: had the gate opened, it would be collected and the outcome would not be the gate's own
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(config).databaseNames("alpha");

    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).commitIndex(2L)
        .stateMachine(FakeRaftHAServer.stateMachineOfARunningCluster());

    final BootstrapElection election = new BootstrapElection(raft, server);

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.SKIPPED_NOT_FIRST_FORMATION);
  }

  /**
   * Defensive: a positive commit index with no reachable state machine (the query cannot be answered)
   * must skip rather than assume first formation.
   */
  @Test
  void positiveCommitIndexWithNoStateMachineIsSkipped() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(config).databaseNames("alpha");

    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).commitIndex(2L)
        .noStateMachine(); // state machine not wired yet

    final BootstrapElection election = new BootstrapElection(raft, server);

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.SKIPPED_NOT_FIRST_FORMATION);
  }
}
