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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.LocalResyncState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7136.
 * <p>
 * {@code GET /api/v1/cluster} published nothing about the local node's own resync state, so on the very node
 * readiness was holding out of the Kubernetes Service the endpoint an operator polls answered {@code 200} with
 * {@code raftState: "RUNNING"} and {@code alerts: []}. None of the four components of
 * {@link ArcadeStateMachine#isResyncInProgress()} - the queued snapshot download, the running one, the WAL
 * version-gap quarantine (#4797/#4740) and the stale-snapshot read floors (#6111/#6760) - reached the document.
 * <p>
 * The invariant these tests pin: <b>anything that makes readiness return 503 must appear in the cluster status
 * document</b>. {@link ArcadeStateMachine#getLocalResyncState()} is that document's source, and readiness reads
 * the same value, so the two cannot drift.
 */
class Issue7136LocalResyncStatusTest {

  private static final String DB_A = "db-a";
  private static final String DB_B = "db-b";

  // ---------------------------------------------------------------------------------------------
  // The report is the whole of isResyncInProgress(), component by component
  // ---------------------------------------------------------------------------------------------

  @Test
  void aQuietNodeReportsNothingInFlight(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      final LocalResyncState state = sm.getLocalResyncState();

      assertThat(state.inProgress()).isFalse();
      assertThat(sm.isResyncInProgress()).isFalse();
      assertThat(state.snapshotDownloadQueued()).isFalse();
      assertThat(state.snapshotDownloadInProgress()).isFalse();
      assertThat(state.divergedDatabases()).isEmpty();
      assertThat(state.snapshotAppliedFloor()).isEqualTo(-1L);
      assertThat(state.databaseAppliedFloors()).isEmpty();
    } finally {
      sm.close();
    }
  }

  @Test
  void aQueuedSnapshotDownloadIsBothReportedAndUnready(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      flag(sm, "needsSnapshotDownload", true);

      assertThat(sm.getLocalResyncState().snapshotDownloadQueued()).isTrue();
      assertThat(sm.getLocalResyncState().inProgress()).isTrue();
      assertThat(sm.isResyncInProgress()).as("readiness and the status document must agree").isTrue();
    } finally {
      sm.close();
    }
  }

  @Test
  void aRunningSnapshotDownloadIsBothReportedAndUnready(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      flag(sm, "snapshotDownloadInProgress", true);

      assertThat(sm.getLocalResyncState().snapshotDownloadInProgress()).isTrue();
      assertThat(sm.getLocalResyncState().inProgress()).isTrue();
      assertThat(sm.isResyncInProgress()).isTrue();
    } finally {
      sm.close();
    }
  }

  /** The WAL version-gap quarantine (#4797/#4740): the condition the issue was reported against. */
  @Test
  void aWalGapQuarantineNamesTheDatabase(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.markStateDiverged(DB_B);
      sm.markStateDiverged(DB_A);

      final LocalResyncState state = sm.getLocalResyncState();
      assertThat(state.divergedDatabases()).as("sorted for a stable poll payload").containsExactly(DB_A, DB_B);
      assertThat(state.inProgress()).isTrue();
      assertThat(sm.isResyncInProgress()).isTrue();
    } finally {
      sm.close();
    }
  }

  /** The node-wide stale-snapshot read floor (#6111): set with neither download flag raised. */
  @Test
  void anOutstandingReadFloorIsBothReportedAndUnready(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      floor(sm, 100L);

      assertThat(sm.getLocalResyncState().snapshotAppliedFloor()).isEqualTo(100L);
      assertThat(sm.getLocalResyncState().inProgress()).isTrue();
      assertThat(sm.isResyncInProgress()).isTrue();
    } finally {
      sm.close();
    }
  }

  /** The per-database read floor (#6760). */
  @Test
  void aPerDatabaseReadFloorIsBothReportedAndUnready(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      databaseFloor(sm, DB_A, 42L);

      final LocalResyncState state = sm.getLocalResyncState();
      assertThat(state.databaseAppliedFloors()).containsExactly(Map.entry(DB_A, 42L));
      assertThat(state.inProgress()).isTrue();
      assertThat(sm.isResyncInProgress()).isTrue();
    } finally {
      sm.close();
    }
  }

  /**
   * The report is a snapshot, not a live view: a later change to the state machine must not mutate a document
   * already handed to a caller (two concurrent polls would otherwise see each other's state mid-render).
   */
  @Test
  void theReportIsAnImmutableSnapshot(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.markStateDiverged(DB_A);
      final LocalResyncState state = sm.getLocalResyncState();

      sm.markStateDiverged(DB_B);

      assertThat(state.divergedDatabases()).containsExactly(DB_A);
      assertThat(sm.getLocalResyncState().divergedDatabases()).containsExactly(DB_A, DB_B);
    } finally {
      sm.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The alert a monitoring rule keyed on alerts[] can fire on
  // ---------------------------------------------------------------------------------------------

  @Test
  void noAlertWhileNothingIsResyncing() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(clean(), null, alerts);
    ClusterAlerts.addLocalResyncAlert(null, null, alerts);

    assertThat(alerts.length()).isZero();
  }

  @Test
  void aRunningDownloadRaisesAWarningNamingWhatIsInFlight() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(
        new LocalResyncState(false, true, List.of(), -1, Map.of()), null, alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("local-resync-in-progress");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
    final JSONObject details = alert.getJSONObject("details");
    assertThat(details.getBoolean("snapshotDownloadInProgress")).isTrue();
    assertThat(details.getBoolean("snapshotDownloadQueued")).isFalse();
  }

  @Test
  void aQuarantinedDatabaseEscalatesToCriticalAndIsNamed() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(
        new LocalResyncState(false, false, List.of(DB_A), -1, Map.of()), null, alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alert.getJSONObject("details").getJSONArray("divergedDatabases").toList()).containsExactly(DB_A);
  }

  @Test
  void anOutstandingReadFloorEscalatesToCritical() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(
        new LocalResyncState(false, false, List.of(), 100L, Map.of()), null, alerts);

    assertThat(alerts.getJSONObject(0).getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alerts.getJSONObject(0).getJSONObject("details").getLong("snapshotAppliedFloor")).isEqualTo(100L);
  }

  /**
   * The alert names databases, so a caller scoped to one database must not learn another one's name - the same
   * rule the other database-scoped alerts follow. The node-level condition itself is NOT hidden: the alert still
   * fires, because whether this node serves traffic is not a per-tenant fact.
   */
  @Test
  void databaseNamesAreScopedToTheCallerButTheAlertStillFires() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(
        new LocalResyncState(false, false, List.of(DB_A, DB_B), -1, Map.of(DB_B, 7L)), Set.of(DB_A), alerts);

    assertThat(alerts.length()).as("a resyncing node is node-level news, not per-tenant news").isEqualTo(1);
    final JSONObject details = alerts.getJSONObject(0).getJSONObject("details");
    assertThat(details.getJSONArray("divergedDatabases").toList()).containsExactly(DB_A);
    assertThat(details.getJSONObject("databaseAppliedFloors").keySet()).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // The localResync object the status document publishes
  // ---------------------------------------------------------------------------------------------

  @Test
  void theStatusDocumentCarriesEveryComponentOnAQuietNode() {
    final JSONObject json = GetClusterHandler.buildLocalResync(clean(), null);

    assertThat(json.getBoolean("inProgress")).isFalse();
    assertThat(json.getBoolean("snapshotDownloadQueued")).isFalse();
    assertThat(json.getBoolean("snapshotDownloadInProgress")).isFalse();
    assertThat(json.getJSONArray("divergedDatabases").toList()).isEmpty();
    assertThat(json.getLong("snapshotAppliedFloor")).isEqualTo(-1L);
    assertThat(json.getJSONObject("databaseAppliedFloors").keySet()).isEmpty();
  }

  /** The reported scenario: a follower quarantined on a WAL version gap must no longer read as green. */
  @Test
  void aQuarantinedFollowerIsVisibleInTheStatusDocument() {
    final JSONObject json = GetClusterHandler.buildLocalResync(
        new LocalResyncState(true, false, List.of(DB_A), -1, Map.of(DB_A, 42L)), null);

    assertThat(json.getBoolean("inProgress")).isTrue();
    assertThat(json.getBoolean("snapshotDownloadQueued")).isTrue();
    assertThat(json.getJSONArray("divergedDatabases").toList()).containsExactly(DB_A);
    assertThat(json.getJSONObject("databaseAppliedFloors").getLong(DB_A)).isEqualTo(42L);
  }

  /**
   * {@code inProgress} is the node's answer and survives scoping; only the database names are reduced. A
   * tenant that may see nothing still learns the node is not serving.
   */
  @Test
  void theStatusDocumentScopesDatabaseNamesButNotTheNodeLevelAnswer() {
    final JSONObject json = GetClusterHandler.buildLocalResync(
        new LocalResyncState(false, false, List.of(DB_A, DB_B), -1, Map.of(DB_B, 7L)), Set.of(DB_A));

    assertThat(json.getBoolean("inProgress")).isTrue();
    assertThat(json.getJSONArray("divergedDatabases").toList()).containsExactly(DB_A);
    assertThat(json.getJSONObject("databaseAppliedFloors").keySet()).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // The status-exporter leftover from #7040: a removed peer must stop reading as "not yet converged"
  // ---------------------------------------------------------------------------------------------

  @Test
  void aDeclaredPeerThatHasNeverJoinedStillCountsAsPending() {
    // Bootstrap window (issue #5304): arcadedb-3 is declared, has never been committed, and the table must
    // keep saying the membership is not converged yet.
    final int expected = RaftClusterStatusExporter.expectedMemberCount(
        List.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of("arcadedb-1", "arcadedb-2"),
        Set.of("arcadedb-1", "arcadedb-2"), true);

    assertThat(expected).isEqualTo(3);
  }

  @Test
  void aPeerRemovedFromTheConfigurationStopsCountingAsPending() {
    // arcadedb-3 was a committed member and then left (DELETE /api/v1/cluster/peer/{id}). Convergence is
    // complete: the note must clear rather than blame a peer that is never coming back.
    final int expected = RaftClusterStatusExporter.expectedMemberCount(
        List.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of("arcadedb-1", "arcadedb-2"),
        Set.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), true);

    assertThat(expected).isEqualTo(2);
  }

  @Test
  void aRuntimeJoinNotInTheDeclaredListDoesNotInflateTheCount() {
    // A peer added with POST /api/v1/cluster/peer is a committed member the server list never declared.
    final int expected = RaftClusterStatusExporter.expectedMemberCount(
        List.of("arcadedb-1"), List.of("arcadedb-1", "arcadedb-2"), Set.of("arcadedb-1", "arcadedb-2"), true);

    assertThat(expected).isEqualTo(2);
  }

  @Test
  void aTickThatCouldNotReadTheConfigurationFallsBackToTheDeclaredCount() {
    // The division was unreadable (issue #5271): getCommittedPeersOrNull() answered null, so no committed id
    // was observed this tick. Every declared peer HAS been committed before, so the pending scan would find
    // none and return 0 - a denominator that says the cluster expects no members at all. Nothing was measured,
    // so the honest answer is the declared list.
    final int expected = RaftClusterStatusExporter.expectedMemberCount(
        List.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(),
        Set.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), false);

    assertThat(expected).as("an unmeasured tick must not invent a membership size").isEqualTo(3);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static LocalResyncState clean() {
    return new LocalResyncState(false, false, List.of(), -1, Map.of());
  }

  private static ArcadeStateMachine newStateMachine(final Path tempDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    return sm;
  }

  private static void flag(final ArcadeStateMachine sm, final String fieldName, final boolean value)
      throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    ((AtomicBoolean) f.get(sm)).set(value);
  }

  private static void floor(final ArcadeStateMachine sm, final long value) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(value);
  }

  @SuppressWarnings("unchecked")
  private static void databaseFloor(final ArcadeStateMachine sm, final String dbName, final long value)
      throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("staleDatabaseAppliedFloors");
    f.setAccessible(true);
    ((ConcurrentHashMap<String, Long>) f.get(sm)).put(dbName, value);
  }
}
