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
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7902: the durable mark the #7298 fix hangs its whole guarantee on was never
 * published, because the alert builder filtered it through the databases this node still HAS.
 * <p>
 * {@code ClusterAlerts.checkBootstrapDivergedDatabases} passed the marked set through {@code visible(...)}, and
 * {@code GetClusterHandler} builds that filter from {@code ArcadeDBServer.getDatabaseNames()} - the in-memory
 * database registry. A database that is missing from this node is, by definition, not in it. So the filter
 * removed precisely the databases the #7298 mark exists for, the builder received an empty list and emitted
 * nothing, and {@code GET /api/v1/cluster} answered {@code alerts: []} on a node knowingly running without a
 * database the cluster has. The filter is right for what it was written for - a tenant must not learn another
 * tenant's database name from a status poll - and the #6124 case it was written against always has the database
 * registered, so it never removed anything that mattered until now.
 * <p>
 * The second defect was behind the same call. If the database ever WAS registered again while the mark stood,
 * the alert that finally fired described the opposite condition: "Database(s) kept a local copy the cluster never
 * adopted", "the copies are otherwise intact - nothing has been lost", and a recommendation to copy this node's
 * database directory to every peer. Following that for a database this node is missing would overwrite every good
 * copy in the cluster.
 * <p>
 * Both are fixed by splitting the marked set on where the database is NOW, not on why it was marked: a mark is
 * durable and the condition under it is not, so an operator who restores a directory by hand moves the database
 * from one half to the other with nothing re-running the branch that marked it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7902BootstrapMissingDatabaseAlertTest {

  private static final String MISSING_DB = "wiped-and-not-back";
  private static final String KEPT_DB    = "fresher-than-the-cluster";

  @TempDir
  private Path serverDir;

  private final List<ArcadeStateMachine> stateMachines = new ArrayList<>();

  @AfterEach
  void closeStateMachines() {
    for (final ArcadeStateMachine sm : stateMachines)
      try {
        sm.close();
      } catch (final IOException e) {
        // Teardown of a unit-test fixture: a close that fails must not replace the test's own verdict.
      }
    stateMachines.clear();
  }

  /**
   * A server that has NO database registered, which is the state a node is in for a database it lost: the
   * authorization filter {@code GetClusterHandler} builds from the registry is therefore empty, exactly as it is
   * in production on this path.
   */
  private ArcadeStateMachine stateMachineOnAnEmptyRegistry() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(MISSING_DB)).thenReturn(false);
    when(server.existsDatabase(KEPT_DB)).thenReturn(false);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    stateMachines.add(sm);
    return sm;
  }

  private static JSONObject alertWithId(final JSONArray alerts, final String id) {
    for (int i = 0; i < alerts.length(); i++) {
      final JSONObject alert = alerts.getJSONObject(i);
      if (id.equals(alert.getString("id", null)))
        return alert;
    }
    return null;
  }

  /**
   * The defect, at the seam it actually failed at: the caller's visible set is the registry, the registry does not
   * contain a database this node is missing, and the alert has to fire anyway. Whether this node is serving a
   * database the cluster has is a node-level fact, like {@code localResync.inProgress}, and the filter reduces its
   * NAMES rather than deciding whether it is reported.
   */
  @Test
  void aMissingDatabaseIsReportedAlthoughTheRegistryFilterCannotContainIt() {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(MISSING_DB);

    final JSONArray alerts = new JSONArray();
    // Set.of() is precisely what filterAuthorizedDatabases(user, server.getDatabaseNames()) answers here.
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, Set.of());

    final JSONObject missing = alertWithId(alerts, "bootstrap-database-missing");
    assertThat(missing)
        .as("the one case the registry-derived filter silently swallowed")
        .isNotNull();
    assertThat(missing.getJSONObject("details").getInt("count"))
        .as("the count is the raw figure, so a caller who may be told no name still learns the node is short")
        .isEqualTo(1);
    assertThat(missing.getJSONObject("details").getJSONArray("databases"))
        .as("but the name itself stays scoped")
        .isEmpty();
  }

  /**
   * And it is not reported under the alert whose text says the opposite. The kept-copy alert promises the data is
   * intact and tells an operator to copy this node's directory to every peer, which for a database this node does
   * not have would destroy every good copy in the cluster.
   */
  @Test
  void aMissingDatabaseIsNotReportedAsAKeptLocalCopy() {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(MISSING_DB);

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, null);

    assertThat(alertWithId(alerts, "bootstrap-diverged-databases"))
        .as("a node with no copy has kept nothing")
        .isNull();
    final JSONObject missing = alertWithId(alerts, "bootstrap-database-missing");
    assertThat(missing.getJSONObject("details").getJSONArray("databases").getString(0)).isEqualTo(MISSING_DB);
    assertThat(missing.getString("recommendation"))
        .as("the remedy must not be the one that overwrites every good copy in the cluster")
        .doesNotContain("copy this node's database directory to every peer");
  }

  /**
   * The #6124 case is untouched: a marked database whose directory is on disk is a copy this node KEPT - merely
   * closed, not gone - and it keeps the alert whose text was written for it, filter and all.
   */
  @Test
  void aKeptLocalCopyStillGetsTheDivergenceAlert() throws Exception {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(KEPT_DB);
    // A closed database, files and all: an EMPTY directory is what a failed install leaves behind, and holds no
    // copy of anything (issue #8045).
    Files.writeString(Files.createDirectories(serverDir.resolve(KEPT_DB)).resolve(LocalSchema.SCHEMA_FILE_NAME), "{}");

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, null);

    final JSONObject diverged = alertWithId(alerts, "bootstrap-diverged-databases");
    assertThat(diverged).isNotNull();
    assertThat(diverged.getJSONObject("details").getJSONArray("databases").getString(0)).isEqualTo(KEPT_DB);
    assertThat(alertWithId(alerts, "bootstrap-database-missing"))
        .as("nothing is missing here")
        .isNull();
  }

  /**
   * Both conditions at once, which is the state a node reaches by losing one database and refusing to overwrite
   * another. Each database must appear in exactly one of the two alerts, or an operator reading either one acts
   * on the wrong database.
   */
  @Test
  void theTwoConditionsAreReportedSeparatelyWhenBothHold() throws Exception {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(MISSING_DB);
    sm.markBootstrapUnreconciled(KEPT_DB);
    // A closed database, files and all: an EMPTY directory is what a failed install leaves behind, and holds no
    // copy of anything (issue #8045).
    Files.writeString(Files.createDirectories(serverDir.resolve(KEPT_DB)).resolve(LocalSchema.SCHEMA_FILE_NAME), "{}");

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, null);

    assertThat(alertWithId(alerts, "bootstrap-diverged-databases").getJSONObject("details").getJSONArray("databases"))
        .map(Object::toString).containsExactly(KEPT_DB);
    assertThat(alertWithId(alerts, "bootstrap-database-missing").getJSONObject("details").getJSONArray("databases"))
        .map(Object::toString).containsExactly(MISSING_DB);
  }

  /**
   * The kept-copy alert keeps its old scoping, and that is deliberate rather than an oversight: it is a statement
   * about specific databases whose remedy is per database, so a caller authorized on none of them has nothing to
   * read and nothing to do. Only the missing half is node-scoped.
   */
  @Test
  void theKeptCopyAlertIsStillSuppressedForACallerThatMayNotSeeIt() throws Exception {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();
    sm.markBootstrapUnreconciled(KEPT_DB);
    // A closed database, files and all: an EMPTY directory is what a failed install leaves behind, and holds no
    // copy of anything (issue #8045).
    Files.writeString(Files.createDirectories(serverDir.resolve(KEPT_DB)).resolve(LocalSchema.SCHEMA_FILE_NAME), "{}");

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, Set.of("some-other-tenants-database"));

    assertThat(alerts).isEmpty();
  }

  /**
   * An empty marked set costs nothing, which matters because this runs on every Studio status poll: no allocation
   * and, in particular, no directory stat per database.
   */
  @Test
  void anUnmarkedNodeEmitsNothingAndTouchesNoDisk() {
    final ArcadeStateMachine sm = stateMachineOnAnEmptyRegistry();

    final ArcadeStateMachine.BootstrapUnreconciled unreconciled = sm.getBootstrapUnreconciled(null);

    assertThat(unreconciled.keptLocalCopy()).isEmpty();
    assertThat(unreconciled.missingLocally()).isEmpty();
    assertThat(unreconciled.missingCount()).isZero();

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.checkBootstrapDivergedDatabases(sm, alerts, null);
    assertThat(alerts).isEmpty();
  }
}
