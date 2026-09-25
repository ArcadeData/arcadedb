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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.http.handler.openapi.OpenApiContributor;
import com.arcadedb.server.http.handler.openapi.PluginApiSpec;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8076, which groups the two halves of what the #7519 bootstrap readiness gate told an
 * operator.
 * <ul>
 *   <li><b>#8044</b> - while a bootstrap install was in flight, {@code /api/v1/ready} answered 503 and pointed the
 *       reader at {@code GET /api/v1/cluster}, which published nothing about it: no alert, no member,
 *       {@code localResync.inProgress} false. The #7136 invariant - anything that makes readiness answer 503 is
 *       visible in the status document - was broken by the gate that cited it.</li>
 *   <li><b>#8045</b> - the gate read the bootstrap-unreconciled set raw, so a node merely MISSING a database (the
 *       #7298 half) was taken out of the Service for good, under the text written for a node that KEPT a fresher
 *       copy, told to "discard the local copy" it does not have - while the SEVERE log line and the
 *       {@code bootstrap-database-missing} alert both said the node was still serving everything else.</li>
 * </ul>
 * The state machine is real; the server is a stub over a {@link TempDir}, with one real {@link LocalDatabase} for
 * the database whose copy is replaced, and zero install retries so the download fails immediately with no leader.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8076BootstrapWindowStatusTest {

  private static final String REPLACED_DB = "replaced-from-the-leader";
  private static final String MISSING_DB  = "wiped-and-not-back";
  private static final String KEPT_DB     = "fresher-than-the-cluster";

  @TempDir
  private Path serverDir;

  private       LocalDatabase             localDb;
  private final List<ArcadeStateMachine> stateMachines = new ArrayList<>();

  @BeforeEach
  void setUp() {
    localDb = (LocalDatabase) new DatabaseFactory(serverDir.resolve(REPLACED_DB).toString()).create();
    localDb.getSchema().createDocumentType("Seed");
    localDb.transaction(() -> localDb.newDocument("Seed").set("k", 1).save());
  }

  @AfterEach
  void tearDown() {
    for (final ArcadeStateMachine sm : stateMachines)
      try {
        sm.close();
      } catch (final IOException e) {
        // Teardown of a unit-test fixture: a close that fails must not replace the test's own verdict.
      }
    stateMachines.clear();
    if (localDb != null && localDb.isOpen())
      localDb.close();
  }

  private ArcadeDBServer stubbedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(REPLACED_DB)).thenReturn(true);
    when(server.getDatabase(REPLACED_DB)).thenReturn(new ServerDatabase(null, localDb));
    when(server.existsDatabase(MISSING_DB)).thenReturn(false);
    when(server.existsDatabase(KEPT_DB)).thenReturn(false);
    // The single-bucket check walks the registry; nothing in it is what these tests are about.
    when(server.getDatabaseNames()).thenReturn(Set.of());
    return server;
  }

  private ArcadeStateMachine stateMachineOn(final ArcadeDBServer server) {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    stateMachines.add(sm);
    return sm;
  }

  /** A baseline this node's copy never matches, so the mismatch arm reinstalls from the leader. */
  private static RaftLogEntryCodec.DecodedEntry baselineOfAnotherCopy(final String dbName) {
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(dbName, "0".repeat(64), Long.MAX_VALUE);
    return RaftLogEntryCodec.decode(encoded);
  }

  /**
   * A failed install hands its readiness holder to a retry on the state machine's lifecycle executor, which writes
   * under {@code .raft} before it releases it. Waiting for the release keeps that write from racing the
   * {@link TempDir} cleanup - the teardown flake {@code Issue7298BootstrapReplaySkipMissingDatabaseTest} documents.
   */
  private static void awaitInstallsSettled(final ArcadeStateMachine sm) {
    await().atMost(Duration.ofSeconds(30)).until(() -> sm.getBootstrapInstallsInFlight().isEmpty());
  }

  private static JSONObject alertWithId(final JSONArray alerts, final String id) {
    for (int i = 0; i < alerts.length(); i++) {
      final JSONObject alert = alerts.getJSONObject(i);
      if (id.equals(alert.getString("id", null)))
        return alert;
    }
    return null;
  }

  /** What {@code GetClusterHandler} renders, sampled at the same instant, the way the handler samples it. */
  private record StatusSample(String readiness, JSONArray alerts, JSONObject bootstrapInstalls) {
  }

  private static StatusSample sample(final ArcadeDBServer server, final ArcadeStateMachine sm,
      final Set<String> visibleDatabases) {
    final ClusterAlerts.NodeStatus nodeStatus = ClusterAlerts.NodeStatus.of(sm);
    return new StatusSample(sm.bootstrapWindowReason(),
        ClusterAlerts.scan(server, sm, List.of(), visibleDatabases, null, null, sm.getLocalResyncState(), nodeStatus,
            false),
        GetClusterHandler.buildBootstrapInstalls(nodeStatus.bootstrapInstalls(), visibleDatabases));
  }

  // ------------------------------------------------------------------------------------------------------------
  // #8044: the install window is published in the document the readiness body points at
  // ------------------------------------------------------------------------------------------------------------

  /**
   * The defect, observed from inside the install ({@code SnapshotInstaller.install} asks for the backup coordinator
   * before it downloads anything): readiness is 503 and says the document names the database, so the document has
   * to - as an alert a monitoring rule keyed on {@code alerts} sees, and as a member a client reads by name.
   */
  @Test
  void anInstallInFlightIsPublishedInTheStatusDocumentTheReadinessBodyPointsAt() {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachineOn(server);
    final AtomicReference<StatusSample> inside = new AtomicReference<>();
    when(server.getBackupCoordinator()).thenAnswer(invocation -> {
      inside.set(sample(server, sm, null));
      return null;
    });

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(baselineOfAnotherCopy(REPLACED_DB), 7L));
    awaitInstallsSettled(sm);

    final StatusSample status = inside.get();
    assertThat(status).as("the install was reached").isNotNull();
    assertThat(status.readiness())
        .as("the #7519 gate still takes the node out of the Service while a held copy is being replaced")
        .isNotNull()
        .contains("replacing 1 database(s)")
        .endsWith("GET /api/v1/cluster names them.");

    final JSONObject alert = alertWithId(status.alerts(), "bootstrap-install-in-progress");
    assertThat(alert).as("before #8044 the alert list was empty for the whole download").isNotNull();
    assertThat(alert.getJSONObject("details").getInt("count")).isEqualTo(1);
    assertThat(alert.getJSONObject("details").getJSONArray("databases").getString(0)).isEqualTo(REPLACED_DB);

    assertThat(status.bootstrapInstalls().getBoolean("inProgress")).isTrue();
    assertThat(status.bootstrapInstalls().getInt("count")).isEqualTo(1);
    assertThat(status.bootstrapInstalls().getJSONArray("databases").getString(0)).isEqualTo(REPLACED_DB);
  }

  /**
   * Whether this node is out of the Service is a node-level fact, so a caller authorized on no database still sees
   * the alert and the count; only the NAME is scoped, exactly as {@code localResync} and
   * {@code bootstrap-database-missing} do it.
   */
  @Test
  void aScopedCallerSeesTheInstallButNotTheName() {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachineOn(server);
    final AtomicReference<StatusSample> inside = new AtomicReference<>();
    when(server.getBackupCoordinator()).thenAnswer(invocation -> {
      inside.set(sample(server, sm, Set.of()));
      return null;
    });

    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(baselineOfAnotherCopy(REPLACED_DB), 7L));
    awaitInstallsSettled(sm);

    final JSONObject alert = alertWithId(inside.get().alerts(), "bootstrap-install-in-progress");
    assertThat(alert).isNotNull();
    assertThat(alert.getJSONObject("details").getInt("count")).isEqualTo(1);
    assertThat(alert.getJSONObject("details").getJSONArray("databases")).isEmpty();
    assertThat(inside.get().bootstrapInstalls().getBoolean("inProgress")).isTrue();
    assertThat(inside.get().bootstrapInstalls().getInt("count")).isEqualTo(1);
    assertThat(inside.get().bootstrapInstalls().getJSONArray("databases")).isEmpty();
  }

  /** The quiet case: the member says "nothing" rather than going absent, and no alert is raised. */
  @Test
  void aNodeInstallingNothingSaysSoExplicitly() {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachineOn(server);

    final StatusSample status = sample(server, sm, null);

    assertThat(status.readiness()).isNull();
    assertThat(alertWithId(status.alerts(), "bootstrap-install-in-progress")).isNull();
    assertThat(status.bootstrapInstalls().getBoolean("inProgress")).isFalse();
    assertThat(status.bootstrapInstalls().getInt("count")).isZero();
    assertThat(status.bootstrapInstalls().getJSONArray("databases")).isEmpty();
  }

  /** The member reaches the published contract too, with exactly the keys the handler writes. */
  @Test
  void theApiSpecDeclaresTheMemberTheHandlerWrites() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    final OpenApiContributor contributor = new PluginApiSpec();
    contributor.contribute(openAPI);
    final Schema<?> status = openAPI.getComponents().getSchemas().get("ClusterStatus");

    assertThat(status.getRequired()).as("written on every answer").contains("bootstrapInstalls");
    final Schema<?> declared = (Schema<?>) status.getProperties().get("bootstrapInstalls");
    assertThat(declared).isNotNull();
    final JSONObject emitted = GetClusterHandler.buildBootstrapInstalls(List.of("db"), null);
    assertThat(declared.getProperties().keySet()).containsExactlyInAnyOrderElementsOf(emitted.keySet());
    assertThat(declared.getRequired()).containsExactlyInAnyOrderElementsOf(emitted.keySet());
  }

  // ------------------------------------------------------------------------------------------------------------
  // #8045: a MISSING database is not a copy the cluster decided against
  // ------------------------------------------------------------------------------------------------------------

  /**
   * The defect: a #7298 mark on a database this node does not hold used to pin readiness at 503 for good, across
   * restarts, under the kept-copy remedy. The node holds no copy the cluster did not adopt, so it stays in the
   * Service - which is what the {@code bootstrap-database-missing} alert and the SEVERE line already promise - and
   * the alert remains the place the condition is reported.
   */
  @Test
  void aMissingDatabaseDoesNotTakeTheNodeOutOfTheService() {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.markBootstrapUnreconciled(MISSING_DB);

    assertThat(sm.getBootstrapUnreconciled(null).missingLocally())
        .as("the mark is there and classified as missing: this is the #7298 half")
        .containsExactly(MISSING_DB);

    final StatusSample status = sample(server, sm, null);
    assertThat(status.readiness())
        .as("no copy is being served that the cluster decided against, so readiness is not held")
        .isNull();
    assertThat(alertWithId(status.alerts(), "bootstrap-database-missing"))
        .as("and the condition is still reported, where it belongs")
        .isNotNull();
  }

  /**
   * Both halves at once: only the KEPT copy holds readiness, and the count and the remedy in the body are the
   * kept-copy ones. Before the fix this read "2 database(s) on this node hold a copy", one of which did not exist.
   */
  @Test
  void aKeptCopyNextToAMissingDatabaseIsCountedAlone() throws Exception {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.markBootstrapUnreconciled(MISSING_DB);
    sm.markBootstrapUnreconciled(KEPT_DB);
    // A closed database, files and all: an EMPTY directory is what a failed install leaves behind, and holds no
    // copy of anything (issue #8045).
    Files.writeString(Files.createDirectories(serverDir.resolve(KEPT_DB)).resolve(LocalSchema.SCHEMA_FILE_NAME), "{}");

    final String reason = sm.bootstrapWindowReason();
    assertThat(reason).isNotNull();
    assertThat(reason).contains("1 database(s) on this node hold a copy");
    assertThat(reason).doesNotContain("2 database(s)");
    assertThat(reason).contains("/api/v1/cluster/resync/");
    assertThat(reason).doesNotContain(KEPT_DB).doesNotContain(MISSING_DB);
  }

  /**
   * The install half of the same distinction. Reinstalling a database this node does not hold (the #7298 replay-skip
   * arm) serves nothing the cluster decided against - there is nothing on disk to serve - so it does not hold
   * readiness either. It is still published as an install in flight, because it is one.
   * <p>
   * Sampled on EVERY install attempt, the retry included. The first attempt fails (no leader in a unit test) and
   * leaves behind the empty directory the installer staged its download in, and the retry is where that used to
   * turn the missing database into a "kept copy" - see the next test.
   */
  @Test
  void reinstallingAMissingDatabaseIsPublishedButDoesNotHoldReadiness() {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachineOn(server);
    // A previous session applied this very entry, so the replay-skip arm finds the database gone and reinstalls.
    sm.writePersistedAppliedIndex(50L, MISSING_DB);
    final List<StatusSample> samples = new CopyOnWriteArrayList<>();
    when(server.getBackupCoordinator()).thenAnswer(invocation -> {
      samples.add(sample(server, sm, null));
      return null;
    });

    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(MISSING_DB, "0".repeat(64), 7L);
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encoded), 50L));
    awaitInstallsSettled(sm);

    assertThat(samples).as("the install and its retry were both reached").hasSizeGreaterThanOrEqualTo(2);
    for (final StatusSample status : samples) {
      assertThat(status.readiness()).as("nothing on disk is being served in the cluster's stead").isNull();
      assertThat(alertWithId(status.alerts(), "bootstrap-install-in-progress")).isNotNull();
      assertThat(status.bootstrapInstalls().getInt("count")).isEqualTo(1);
    }
  }

  /**
   * What a failed reinstall of a missing database leaves behind. {@code SnapshotInstaller.install} creates the
   * database directory to stage its download in and leaves it, empty, when the download fails. Counted as a copy,
   * that one failure made the database "kept" for good: the readiness gate held the node out of the Service under
   * the kept-copy text this issue is about, and the {@code bootstrap-diverged-databases} alert told the operator to
   * copy this node's directory - empty - to every peer.
   */
  @Test
  void aFailedReinstallLeavesTheDatabaseMissingNotKept() {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.writePersistedAppliedIndex(50L, MISSING_DB);

    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(MISSING_DB, "0".repeat(64), 7L);
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(encoded), 50L));
    awaitInstallsSettled(sm);

    assertThat(serverDir.resolve(MISSING_DB))
        .as("the precondition: the failed install left its (empty) staging directory behind")
        .isEmptyDirectory();
    final ArcadeStateMachine.BootstrapUnreconciled unreconciled = sm.getBootstrapUnreconciled(null);
    assertThat(unreconciled.missingLocally()).containsExactly(MISSING_DB);
    assertThat(unreconciled.keptLocalCopy()).as("an empty directory holds no copy to keep").isEmpty();
    assertThat(sm.bootstrapWindowReason()).isNull();

    final JSONArray alerts = ClusterAlerts.scan(server, sm, List.of(), null, null, null, sm.getLocalResyncState(),
        ClusterAlerts.NodeStatus.of(sm), false);
    assertThat(alertWithId(alerts, "bootstrap-database-missing")).isNotNull();
    assertThat(alertWithId(alerts, "bootstrap-diverged-databases"))
        .as("the remedy that copies this node's directory to every peer must not be offered for an empty one")
        .isNull();
  }

  /**
   * And the periodic #7298 retry still fires over that empty directory. It runs only for a marked database whose
   * directory is gone, so counting an empty one as present stopped the one automatic recovery the missing half has.
   */
  @Test
  void thePeriodicRetryStillRunsOverAnEmptyLeftoverDirectory() throws Exception {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.markBootstrapUnreconciled(MISSING_DB);
    Files.createDirectories(serverDir.resolve(MISSING_DB));

    sm.reconcileBootstrapDivergence(Map.of(MISSING_DB, new ArcadeStateMachine.BootstrapBaseline("0".repeat(64), 7L)));

    verify(server, atLeastOnce()).getBackupCoordinator();
    awaitInstallsSettled(sm);
  }
}
