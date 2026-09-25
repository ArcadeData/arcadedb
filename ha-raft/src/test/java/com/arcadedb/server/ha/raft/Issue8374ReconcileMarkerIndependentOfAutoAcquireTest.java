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
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
class Issue8374ReconcileMarkerIndependentOfAutoAcquireTest {

  private static final String   LEADER_HTTP   = "leader:2480";
  private static final String   LEADER_HTTPS  = null;
  private static final String   CLUSTER_TOKEN = null;
  private static final TermIndex MARKER       = TermIndex.valueOf(7L, 41L);

  // ---- auto-acquire off: the legacy refresh path must read the marker, with the cheap request ----

  @Test
  void autoAcquireDisabledReadsTheMarkerWithTheMarkerOnlyRequest() throws Exception {
    final StubReconciler reconciler = new StubReconciler(null, marker(MARKER));
    configure(reconciler, false, Set.of());

    final DatabaseReconciler.ReconcileFromLeaderResult result =
        reconciler.reconcileDatabasesFromLeader(LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN);

    assertThat(result.leaderSnapshotTermIndex())
        .as("auto-acquire off must not mean the marker is never read (issue #8374)")
        .isEqualTo(MARKER);
    assertThat(result.notInstalled()).isEmpty();
    assertThat(reconciler.fullCalls).as("the database list is not needed here, so no fingerprinting").isZero();
    assertThat(reconciler.markerCalls).isEqualTo(1);
  }

  @Test
  void autoAcquireDisabledFailsTheInstallWhenTheMarkerCannotBeRead() {
    final StubReconciler reconciler = new StubReconciler(null, new IOException("leader unreachable"));
    configure(reconciler, false, Set.of("db"));

    assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader(LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN))
        .as("a guessed term is never revisited, so the install must fail and let Ratis re-drive it")
        .isInstanceOf(IOException.class)
        .hasMessageContaining("#8374");
  }

  @Test
  void autoAcquireDisabledPreservesTheInterruptAndFailsTheInstall() {
    final StubReconciler reconciler = new StubReconciler(null, new InterruptedException("shutting down"));
    configure(reconciler, false, Set.of("db"));

    try {
      assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader(LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN))
          .isInstanceOf(IOException.class);
      assertThat(Thread.currentThread().isInterrupted()).as("the interrupt must be preserved, not swallowed").isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void autoAcquireDisabledCarriesNullFromALeaderThatPredatesTheField() throws Exception {
    // A leader build older than #8360 answers without snapshotTerm/snapshotIndex. Refusing would stall every follower
    // upgraded ahead of its leader, so the install proceeds and the state machine falls back to the approximation.
    final StubReconciler reconciler = new StubReconciler(null, marker(null));
    configure(reconciler, false, Set.of());

    assertThat(reconciler.reconcileDatabasesFromLeader(LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN).leaderSnapshotTermIndex())
        .isNull();
  }

  // ---- auto-acquire on ----

  @Test
  void autoAcquireEnabledCarriesTheMarkerFromTheFullListing() throws Exception {
    final StubReconciler reconciler = new StubReconciler(marker(MARKER), new AssertionError("marker-only read not expected"));
    configure(reconciler, true, Set.of());

    final DatabaseReconciler.ReconcileFromLeaderResult result =
        reconciler.reconcileDatabasesFromLeader(LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN);

    assertThat(result.leaderSnapshotTermIndex()).isEqualTo(MARKER);
    assertThat(reconciler.markerCalls).isZero();
  }

  @Test
  void autoAcquireEnabledReadsTheMarkerAloneWhenTheFullListingFails() throws Exception {
    // The full listing fingerprints every database and can time out where the marker-only read does not. A populated
    // follower keeps the legacy refresh (issue #4799), and now also the marker.
    final StubReconciler reconciler = new StubReconciler(new IOException("listing timed out"), marker(MARKER));
    configure(reconciler, true, Set.of("db"));

    final DatabaseReconciler.ReconcileFromLeaderResult result =
        reconciler.reconcileDatabasesFromLeader(LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN);

    assertThat(result.leaderSnapshotTermIndex())
        .as("the listing failure used to register the approximate term (issue #8374)")
        .isEqualTo(MARKER);
    assertThat(reconciler.markerCalls).isEqualTo(1);
  }

  @Test
  void autoAcquireEnabledFailsTheInstallWhenNeitherReadSucceeds() {
    final StubReconciler reconciler = new StubReconciler(new IOException("listing failed"), new IOException("marker failed"));
    configure(reconciler, true, Set.of("db"));

    assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader(LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("#8374");
  }

  @Test
  void autoAcquireEnabledStillFailsTheInstallOnAnEmptyFollower() {
    // Issue #4799 is unchanged: an empty follower that cannot list the leader's databases must not ACK the index.
    final StubReconciler reconciler = new StubReconciler(new IOException("listing failed"), marker(MARKER));
    configure(reconciler, true, Set.of());

    assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader(LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("#4799");
  }

  @Test
  void autoAcquireEnabledFailsTheInstallWhenTheListingIsInterrupted() {
    final StubReconciler reconciler = new StubReconciler(new InterruptedException("shutting down"), marker(MARKER));
    configure(reconciler, true, Set.of("db"));

    try {
      assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader(LEADER_HTTP, LEADER_HTTPS, CLUSTER_TOKEN))
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

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.getDatabaseNames()).thenReturn(localDbs);
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
    LeaderDatabaseQuery.BootstrapState fetchBootstrapState(final String leaderHttpAddr, final String leaderHttpsAddr,
        final String clusterToken) throws IOException, InterruptedException {
      fullCalls++;
      return answer(full);
    }

    @Override
    LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderHttpAddr, final String leaderHttpsAddr,
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
