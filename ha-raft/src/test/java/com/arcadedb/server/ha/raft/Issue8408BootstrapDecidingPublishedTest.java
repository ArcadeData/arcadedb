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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.SubclassMocks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8408: the third arm of {@code ArcadeStateMachine.bootstrapWindowReason()} - a first-formation
 * bootstrap pass still deciding which copy of a database the cluster keeps - was the one arm of the window that
 * {@code GET /api/v1/cluster} did not publish, so the readiness body deliberately left out its "GET /api/v1/cluster names
 * them" sentence. It is now a {@code bootstrap-deciding-databases} alert and a {@code bootstrapDeciding} member, sampled
 * from the same {@link ClusterAlerts.NodeStatus} as the installs, and the sentence is said for it too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8408BootstrapDecidingPublishedTest {

  private static final String DB_DIR  = "./target/databases";
  private static final String DB_NAME = "test-8408-bootstrap-deciding";
  private static final String DB_PATH = DB_DIR + "/" + DB_NAME;
  private static final long   HOLD_MS = 60_000L;

  private LocalDatabase localDb;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
    localDb = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    localDb.getSchema().createDocumentType("Seed");
    localDb.transaction(() -> localDb.newDocument("Seed").set("k", 1).save());
  }

  @AfterEach
  void tearDown() {
    ProtocolContext.clear();
    if (localDb != null && localDb.isOpen()) {
      if (localDb.isTransactionActive())
        localDb.rollbackAllNested();
      localDb.close();
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
  }

  @Test
  void aPassDecidingOnADatabaseIsPublishedInTheStatusDocument() {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachine(server);
    assertThat(sm.getBootstrapPassesDeciding()).as("nothing is running yet").isEmpty();

    receiveAnnounce(sm, "pass-1");

    assertThat(sm.getBootstrapPassesDeciding()).containsExactly(DB_NAME);
    assertThat(sm.bootstrapWindowReason())
        .as("the readiness body points at the document for this arm too, now that it names it")
        .contains("deciding which copy of 1 database(s)")
        .endsWith(" GET /api/v1/cluster names them.")
        .doesNotContain(DB_NAME);

    final ClusterAlerts.NodeStatus nodeStatus = ClusterAlerts.NodeStatus.of(sm);
    final JSONArray alerts = ClusterAlerts.scan(server, sm, List.of(), null, null, null, sm.getLocalResyncState(), nodeStatus,
        false);
    final JSONObject alert = alertWithId(alerts, "bootstrap-deciding-databases");
    assertThat(alert).as("before #8408 the alert list was empty while the readiness probe answered 503").isNotNull();
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_INFO);
    assertThat(alert.getJSONObject("details").getInt("count")).isEqualTo(1);
    assertThat(alert.getJSONObject("details").getJSONArray("databases").getString(0)).isEqualTo(DB_NAME);

    final JSONObject member = GetClusterHandler.buildBootstrapDeciding(nodeStatus.bootstrapDeciding(), null);
    assertThat(member.getBoolean("inProgress")).isTrue();
    assertThat(member.getInt("count")).isEqualTo(1);
    assertThat(member.getJSONArray("databases").getString(0)).isEqualTo(DB_NAME);
    assertThat(member.keySet()).containsExactlyInAnyOrder("inProgress", "count", "databases");
  }

  /**
   * Whether this node is out of the Service is a node-level fact: a caller authorized on no database still sees the alert
   * and the count, and only the name is scoped.
   */
  @Test
  void aCallerWhoMayNotSeeTheDatabaseLearnsTheCountAndNotTheName() {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachine(server);
    receiveAnnounce(sm, "pass-1");

    final ClusterAlerts.NodeStatus nodeStatus = ClusterAlerts.NodeStatus.of(sm);
    final JSONArray alerts = ClusterAlerts.scan(server, sm, List.of(), Set.of(), null, null, sm.getLocalResyncState(),
        nodeStatus, false);
    final JSONObject alert = alertWithId(alerts, "bootstrap-deciding-databases");
    assertThat(alert).isNotNull();
    assertThat(alert.getJSONObject("details").getInt("count")).isEqualTo(1);
    assertThat(alert.getJSONObject("details").getJSONArray("databases")).isEmpty();

    final JSONObject member = GetClusterHandler.buildBootstrapDeciding(nodeStatus.bootstrapDeciding(), Set.of());
    assertThat(member.getBoolean("inProgress")).isTrue();
    assertThat(member.getInt("count")).isEqualTo(1);
    assertThat(member.getJSONArray("databases")).isEmpty();
  }

  @Test
  void theDocumentIsCleanOnceThePassConcludes() {
    final ArcadeDBServer server = stubbedServer();
    final ArcadeStateMachine sm = stateMachine(server);
    receiveAnnounce(sm, "pass-1");
    PostBootstrapStateHandler.applyPassMarker(new JSONObject(BootstrapElection.concludePassBody("pass-1", List.of())), sm,
        HOLD_MS);

    final ClusterAlerts.NodeStatus nodeStatus = ClusterAlerts.NodeStatus.of(sm);
    assertThat(nodeStatus.bootstrapDeciding()).isEmpty();
    assertThat(alertWithId(ClusterAlerts.scan(server, sm, List.of(), null, null, null, sm.getLocalResyncState(), nodeStatus,
        false), "bootstrap-deciding-databases")).isNull();
    assertThat(GetClusterHandler.buildBootstrapDeciding(nodeStatus.bootstrapDeciding(), null).getBoolean("inProgress"))
        .isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // ------------------------------------------------------------------------------------------------------------

  private ArcadeDBServer stubbedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DB_DIR);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);
    final ArcadeDBServer server = SubclassMocks.mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(DB_NAME)).thenReturn(true);
    when(server.getDatabase(DB_NAME)).thenReturn(new ServerDatabase(null, localDb));
    // The single-bucket check walks the registry; nothing in it is what this test is about.
    when(server.getDatabaseNames()).thenReturn(Set.of());
    return server;
  }

  private static ArcadeStateMachine stateMachine(final ArcadeDBServer server) {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  private static void receiveAnnounce(final ArcadeStateMachine sm, final String passId) {
    PostBootstrapStateHandler.applyPassMarker(new JSONObject(BootstrapElection.announcePassBody(passId, List.of(DB_NAME))), sm,
        HOLD_MS);
  }

  private static JSONObject alertWithId(final JSONArray alerts, final String id) {
    for (int i = 0; i < alerts.length(); i++) {
      final JSONObject alert = alerts.getJSONObject(i);
      if (id.equals(alert.getString("id", null)))
        return alert;
    }
    return null;
  }
}
