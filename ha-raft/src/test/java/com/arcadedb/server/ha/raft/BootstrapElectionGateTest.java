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
 * Regression tests for the first-formation gate in {@link BootstrapElection} (issue #4800).
 * <p>
 * {@link RaftHAServer#getCommitIndex()} returns {@code -1} when the Raft division is not ready or a
 * transient {@code IOException} is thrown while reading the committed index. The old gate test was
 * {@code commitIndex > 0}, so {@code -1} ({@code -1 > 0 == false}) slipped through and the protocol
 * proceeded as if the cluster's log were empty - replicating a bootstrap fingerprint or transferring
 * leadership on an already-running cluster, repeatedly under a leader-churn storm. The gate must now
 * proceed only on a positively-confirmed commit index of exactly {@code 0}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BootstrapElectionGateTest {

  @Test
  void unknownCommitIndexIsTreatedAsSkip() {
    assertThat(BootstrapElection.isConfirmedFirstFormation(-1L)).isFalse();
    assertThat(BootstrapElection.isConfirmedFirstFormation(Long.MIN_VALUE)).isFalse();
  }

  @Test
  void onlyAnExactZeroOpensTheGate() {
    assertThat(BootstrapElection.isConfirmedFirstFormation(0L)).isTrue();
  }

  @Test
  void anyPositiveCommitIndexIsTreatedAsRunningCluster() {
    assertThat(BootstrapElection.isConfirmedFirstFormation(1L)).isFalse();
    assertThat(BootstrapElection.isConfirmedFirstFormation(Long.MAX_VALUE)).isFalse();
  }

  /**
   * The end-to-end reproduction: leader, bootstrap enabled, but {@code getCommitIndex()} returns the
   * unknown sentinel {@code -1}. The protocol must short-circuit on the gate and never reach the
   * database-collection / decide-and-act phase. Before the fix it proceeded past the gate.
   */
  @Test
  void transientUnknownCommitIndexDoesNotEngageBootstrap() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);

    // Two local databases: had the gate let this pass through, it would collect them and end in an outcome other than
    // SKIPPED_NOT_FIRST_FORMATION, which is returned before anything is collected
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(config).databaseNames("beta", "alpha");

    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(true);
    raft.commitIndex(-1L); // persistent IOException / division never ready

    final BootstrapElection election = new BootstrapElection(raft, server);
    election.commitIndexReadinessTimeoutMs = 0L; // a persistent -1 must skip without a real wait
    final BootstrapElection.Outcome outcome = election.runIfEligible();

    assertThat(outcome).isEqualTo(BootstrapElection.Outcome.SKIPPED_NOT_FIRST_FORMATION);
  }

  /**
   * The regression case: the {@code notifyLeaderChanged} callback runs the bootstrap pass before the
   * freshly-elected leader's Raft division is queryable, so {@code getCommitIndex()} returns the -1
   * UNKNOWN sentinel for a brief window before resolving to the genuine first-formation index 0. Since
   * bootstrap runs at most once per term, an instant skip on that transient -1 would leave the baseline
   * uncommitted forever. The gate must absorb the startup window and open once the index resolves to 0.
   */
  @Test
  void transientUnknownCommitIndexThatResolvesToZeroEngagesBootstrap() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(config);

    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(true);
    // -1 (division not ready) on the first read, then the real first-formation index 0.
    raft.commitIndex(-1L, -1L, 0L);

    final BootstrapElection election = new BootstrapElection(raft, server);
    election.commitIndexReadinessPollMs = 0L; // spin without sleeping
    final BootstrapElection.Outcome outcome = election.runIfEligible();

    // Past the gate once the index resolved to 0: it tried to collect databases and found none.
    assertThat(outcome).isEqualTo(BootstrapElection.Outcome.SKIPPED_NO_DATABASES);
  }

  /**
   * The wait loop's third exit condition: if this node stops being the leader while the commit index
   * is still unresolved (-1), the wait must break rather than spin out the full readiness budget, and
   * the gate must skip. Proves the {@code haServer.isLeader()} guard inside the loop works.
   */
  @Test
  void losingLeadershipWhileWaitingStopsTheWaitAndSkips() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(config).databaseNames("alpha");

    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    // Leader at the entry guard and on the first loop check, then leadership is lost.
    raft.leader(true, true, false);
    raft.commitIndex(-1L); // never resolves

    final BootstrapElection election = new BootstrapElection(raft, server);
    election.commitIndexReadinessPollMs = 0L; // spin without sleeping

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.SKIPPED_NOT_FIRST_FORMATION);
  }

  /**
   * Contrast case proving the gate still opens on a genuine first formation: commit index 0, leader,
   * enabled. The protocol moves past the gate and (with no local databases) reports
   * {@code SKIPPED_NO_DATABASES} rather than the gate outcome.
   */
  @Test
  void confirmedEmptyLogStillEngagesBootstrap() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(config);

    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(true);
    raft.commitIndex(0L);

    final BootstrapElection election = new BootstrapElection(raft, server);
    final BootstrapElection.Outcome outcome = election.runIfEligible();

    // Past the gate: it tried to collect databases and found none.
    assertThat(outcome).isEqualTo(BootstrapElection.Outcome.SKIPPED_NO_DATABASES);
  }

  /**
   * A running cluster (commit index > 0) must be skipped exactly as before - the fix tightens the
   * gate without weakening the existing "stop after first formation" guarantee.
   */
  @Test
  void runningClusterIsSkipped() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(config).databaseNames("alpha");

    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(true);
    raft.commitIndex(42L);
    // A running cluster: the state machine has applied application entries, so a positive commit index is not the
    // internal-entries-only first formation of #5099
    raft.stateMachine(FakeRaftHAServer.stateMachineOfARunningCluster());

    final BootstrapElection election = new BootstrapElection(raft, server);

    assertThat(election.runIfEligible()).isEqualTo(BootstrapElection.Outcome.SKIPPED_NOT_FIRST_FORMATION);
  }
}
