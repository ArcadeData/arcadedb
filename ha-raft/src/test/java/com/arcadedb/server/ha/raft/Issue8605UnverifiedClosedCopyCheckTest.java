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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.ha.raft.UnverifiedClosedCopyCheck.CopyState;
import com.arcadedb.server.security.ServerSecurityUser;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8605, HA side: before a leader reopens a closed copy marked unverified by #8589 - which
 * makes it the cluster's copy - it asks every peer about its own copy, over {@code POST /api/v1/cluster/bootstrap-state}
 * with {@code copyOf}, and reopens only when every peer answered and none holds a copy with a higher applied Raft index.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Timeout(120)
class Issue8605UnverifiedClosedCopyCheckTest {

  private static final String     DB_NAME  = "db8605";
  private static final String     PASSWORD = "DefaultPasswordForTests";
  private static final RaftPeerId PEER_1   = RaftPeerId.valueOf("peer-1");
  private static final RaftPeerId PEER_2   = RaftPeerId.valueOf("peer-2");

  @TempDir
  Path root;

  private ArcadeDBServer     server;
  private ArcadeStateMachine sm;
  private RaftHAServer       raft;

  @BeforeEach
  void setUp() throws IOException {
    server = startServer();
    raft = mock(RaftHAServer.class);
    when(raft.getLocalPeerId()).thenReturn(RaftPeerId.valueOf("local"));
    sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.setRaftHAServer(raft);
    when(raft.getStateMachine()).thenReturn(sm);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (sm != null)
      sm.close();
    if (server != null) {
      try {
        if (server.existsDatabase(DB_NAME))
          ((DatabaseInternal) server.getDatabase(DB_NAME)).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort cleanup; the @TempDir is removed regardless
      }
      server.stop();
    }
  }

  // ---------------------------------------------------------------------------------------------- the rule

  /** The issue as reported: the previous leader holds the database closed at a later index than this copy. */
  @Test
  void aPeerHoldingANewerCopyRefusesTheReopen() {
    final String refusal = UnverifiedClosedCopyCheck.verdict(new CopyState(true, 10L),
        Map.of("peer-1", new CopyState(true, 42L), "peer-2", new CopyState(false, -1L)), List.of());

    assertThat(refusal).contains("peer-1").contains("42").contains("10");
  }

  @Test
  void peersHoldingNoCopyOrAnOlderOrEqualOneAllowTheReopen() {
    assertThat(UnverifiedClosedCopyCheck.verdict(new CopyState(true, 42L),
        Map.of("peer-1", new CopyState(true, 42L), "peer-2", new CopyState(true, 7L), "peer-3",
            new CopyState(false, -1L)), List.of())).isNull();
  }

  /** The previous leader down is the likeliest case of all: its copy may be the newer one. */
  @Test
  void aPeerThatCouldNotBeAskedRefusesTheReopen() {
    final String refusal = UnverifiedClosedCopyCheck.verdict(new CopyState(true, 42L),
        Map.of("peer-1", new CopyState(false, -1L)), List.of("peer-2 (connection refused)"));

    assertThat(refusal).contains("peer-2").contains("connection refused");
  }

  @Test
  void aCopyWithNoRecordedIndexOnEitherSideCannotBeOrderedAndRefuses() {
    assertThat(UnverifiedClosedCopyCheck.verdict(new CopyState(true, 42L), Map.of("peer-1", new CopyState(true, -1L)),
        List.of())).contains("cannot be ordered");
    assertThat(UnverifiedClosedCopyCheck.verdict(new CopyState(true, -1L), Map.of("peer-1", new CopyState(true, 3L)),
        List.of())).contains("cannot be ordered");
    assertThat(UnverifiedClosedCopyCheck.verdict(new CopyState(true, -1L), Map.of("peer-1", new CopyState(false, -1L)),
        List.of())).as("no other copy anywhere: nothing to be behind").isNull();
  }

  // ---------------------------------------------------------------------------------------------- the fan-out

  @Test
  void everyPeerIsAskedAndAPeerWithNoAddressOrAFailedCallRefuses() {
    final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
    final Map<RaftPeerId, String> urls = new LinkedHashMap<>();
    urls.put(PEER_1, "http://peer-1/api/v1/cluster/bootstrap-state");
    urls.put(PEER_2, null);

    assertThat(check.check(DB_NAME, new CopyState(true, 5L), urls, (url, name) -> new CopyState(false, -1L)))
        .contains("peer-2").contains("no HTTP address");

    urls.put(PEER_2, "http://peer-2/api/v1/cluster/bootstrap-state");
    assertThat(check.check(DB_NAME, new CopyState(true, 5L), urls, (url, name) -> {
      if (url.contains("peer-2"))
        throw new IOException("HTTP 503");
      return new CopyState(false, -1L);
    })).contains("peer-2").contains("HTTP 503");

    assertThat(check.check(DB_NAME, new CopyState(true, 5L), urls, (url, name) -> {
      assertThat(name).isEqualTo(DB_NAME);
      return new CopyState(true, 5L);
    })).isNull();
  }

  /** A refusal stands in the cluster alert while the mark does, and goes with a verified reopen or with the mark. */
  @Test
  void aRefusalIsReportedWhileItsMarkStands() throws IOException {
    createClosedCopy(true);
    final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
    final Map<RaftPeerId, String> urls = Map.of(PEER_1, "http://peer-1/x");

    check.check(DB_NAME, new CopyState(true, 5L), urls, (url, name) -> new CopyState(true, 9L));
    assertThat(check.getRefusals()).containsOnlyKeys(DB_NAME);

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addUnverifiedClosedCopyRefusedAlert(check.getRefusals(), null, true, alerts);
    assertThat(alerts.length()).isEqualTo(1);
    assertThat(alerts.getJSONObject(0).getString("id")).isEqualTo("unverified-closed-copy-refused");
    assertThat(alerts.getJSONObject(0).getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);

    final JSONArray hidden = new JSONArray();
    ClusterAlerts.addUnverifiedClosedCopyRefusedAlert(check.getRefusals(), Set.of("other"), true, hidden);
    assertThat(hidden.length()).as("a database the caller cannot see raises nothing").isZero();

    check.check(DB_NAME, new CopyState(true, 9L), urls, (url, name) -> new CopyState(true, 9L));
    assertThat(check.getRefusals()).as("verified: the refusal no longer stands").isEmpty();

    check.check(DB_NAME, new CopyState(true, 5L), urls, (url, name) -> new CopyState(true, 9L));
    Files.delete(marker());
    assertThat(check.getRefusals()).as("the operator removed the mark: the refusal no longer stands").isEmpty();
  }

  // ---------------------------------------------------------------------------------------------- the wire

  @Test
  void anAnswerWithoutTheCopyIsNotAnAnswer() throws IOException {
    assertThatThrownBy(() -> UnverifiedClosedCopyCheck.parseAnswer(200,
        new JSONObject().put("databases", new JSONArray()).put("peerId", "peer-1").toString()))
        .isInstanceOf(IOException.class).hasMessageContaining("older version");
    assertThatThrownBy(() -> UnverifiedClosedCopyCheck.parseAnswer(403, "{}")).isInstanceOf(IOException.class);

    final CopyState parsed = UnverifiedClosedCopyCheck.parseAnswer(200, new JSONObject()
        .put(UnverifiedClosedCopyCheck.COPY, new CopyState(true, 77L).toJSON(DB_NAME)).toString());
    assertThat(parsed).isEqualTo(new CopyState(true, 77L));
  }

  /** The peer's side: a closed copy is reported from disk and the persisted index, and is not opened to answer. */
  @Test
  void aPeerAnswersAboutItsClosedCopyWithoutOpeningIt() throws Exception {
    createClosedCopy(false);
    sm.writePersistedAppliedIndex(31L, DB_NAME);

    final JSONObject answer = askHandler(DB_NAME);

    assertThat(UnverifiedClosedCopyCheck.CopyState.fromJSON(answer.getJSONObject(UnverifiedClosedCopyCheck.COPY)))
        .isEqualTo(new CopyState(true, 31L));
    assertThat(answer.has("databases")).as("not the full fingerprint listing").isFalse();
    assertThat(server.existsDatabase(DB_NAME)).as("answering did not reopen the closed copy").isFalse();
  }

  @Test
  void aPeerWithoutTheDatabaseOrWithItQuarantinedSaysSo() throws Exception {
    assertThat(CopyState.fromJSON(askHandler(DB_NAME).getJSONObject(UnverifiedClosedCopyCheck.COPY)))
        .isEqualTo(new CopyState(false, -1L));

    createClosedCopy(false);
    sm.writePersistedAppliedIndex(31L, DB_NAME);
    sm.quarantineDatabase(DB_NAME, DivergenceCause.WAL_VERSION_GAP);
    assertThat(CopyState.fromJSON(askHandler(DB_NAME).getJSONObject(UnverifiedClosedCopyCheck.COPY)))
        .as("a quarantined copy's index may be overstated").isEqualTo(new CopyState(true, -1L));
  }

  @Test
  void anOpenCopyIsReportedAtItsAppliedIndex() {
    server.getOrCreateDatabase(DB_NAME);
    sm.writePersistedAppliedIndex(12L, DB_NAME);

    assertThat(UnverifiedClosedCopyCheck.localCopyState(server, sm, DB_NAME)).isEqualTo(new CopyState(true, 12L));
  }

  @Test
  void theHandlerRejectsAPathInTheName() {
    assertThatThrownBy(() -> askHandler("../etc")).isInstanceOf(IllegalArgumentException.class);
  }

  // ------------------------------------------------------------------------------------------------------------

  private JSONObject askHandler(final String name) throws Exception {
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    final RaftHAPlugin plugin = mock(RaftHAPlugin.class);
    when(plugin.getRaftHAServer()).thenReturn(raft);
    final ServerSecurityUser root = mock(ServerSecurityUser.class);
    when(root.getName()).thenReturn("root");

    final var response = new PostBootstrapStateHandler(httpServer, plugin).execute(null, root,
        new JSONObject().put(UnverifiedClosedCopyCheck.COPY_OF, name));
    assertThat(response.getCode()).isEqualTo(200);
    return new JSONObject(response.getResponse());
  }

  private void createClosedCopy(final boolean marked) throws IOException {
    final ServerDatabase db = server.getOrCreateDatabase(DB_NAME);
    db.transaction(() -> db.getSchema().createVertexType("Node"));
    db.getEmbedded().close();
    server.removeDatabase(DB_NAME);
    if (marked)
      Files.writeString(marker(), "");
  }

  private Path marker() {
    return root.resolve("databases").resolve(DB_NAME).resolve(ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE);
  }

  private ArcadeDBServer startServer() throws IOException {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8605");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT,
        String.valueOf(StaticBaseServerTest.allocateFreePorts(1)[0]));
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final ArcadeDBServer started = new ArcadeDBServer(config);
    started.start();
    return started;
  }
}
