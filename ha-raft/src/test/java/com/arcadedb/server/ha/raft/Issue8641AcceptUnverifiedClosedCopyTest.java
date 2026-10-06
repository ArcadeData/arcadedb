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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.ha.raft.UnverifiedClosedCopyCheck.CopyState;
import com.arcadedb.server.security.ServerSecurityException;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8641: before it, the only way to reopen a leader's closed copy that a peer can never
 * confirm - a peer permanently gone but still in the configuration, or a copy that cannot be ordered - was deleting the
 * {@code .ha-unverified-closed-copy} marker by hand, with no trace of who accepted what. {@code POST
 * /api/v1/cluster/accept-copy/{database}} is the audited override.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Timeout(120)
class Issue8641AcceptUnverifiedClosedCopyTest {

  private static final String     DB_NAME  = "db8641";
  private static final String     PASSWORD = "DefaultPasswordForTests";
  private static final RaftPeerId DEAD     = RaftPeerId.valueOf("dead-peer");

  @TempDir
  Path root;

  private ArcadeDBServer            server;
  private ArcadeStateMachine        sm;
  private RaftHAServer              raft;
  private UnverifiedClosedCopyCheck check;

  @BeforeEach
  void setUp() throws IOException {
    server = startServer();
    raft = mock(RaftHAServer.class);
    when(raft.getLocalPeerId()).thenReturn(RaftPeerId.valueOf("local"));
    when(raft.isLeader()).thenReturn(true);
    sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.setRaftHAServer(raft);
    when(raft.getStateMachine()).thenReturn(sm);
    check = new UnverifiedClosedCopyCheck(raft, server);
    check.refusalReuseMs = 0L;
    when(raft.getUnverifiedClosedCopyCheck()).thenReturn(check);
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

  /**
   * The issue as reported: a peer that can never answer keeps refusing the reopen, round after round. Accepting the copy
   * removes the marker, drops the standing refusal from the cluster alert, and the next round has nothing to verify.
   */
  @Test
  void acceptingTheCopyLiftsARefusalADeadPeerWouldKeepForever() throws IOException {
    createClosedCopy();
    sm.writePersistedAppliedIndex(17L, DB_NAME);
    final Map<RaftPeerId, String> urls = Map.of(DEAD, "http://dead-peer/x");
    final UnverifiedClosedCopyCheck.PeerQuestion deadPeer = (peer, url, name) -> CompletableFuture.failedFuture(
        new IOException("connection refused"));

    final String refusal = check.check(DB_NAME, new CopyState(true, 17L), urls, deadPeer);
    assertThat(refusal).contains("dead-peer").contains("connection refused");
    assertThat(check.check(DB_NAME, new CopyState(true, 17L), urls, deadPeer))
        .as("the dead peer keeps the database closed on every attempt").isNotNull();
    assertThat(alertCount()).isEqualTo(1);

    final UnverifiedClosedCopyCheck.Acceptance acceptance = check.accept(DB_NAME, "user 'root'");

    assertThat(acceptance).isNotNull();
    assertThat(acceptance.appliedIndex()).isEqualTo(17L);
    assertThat(acceptance.standingRefusal()).isEqualTo(refusal);
    assertThat(Files.exists(marker())).as("the marker is gone").isFalse();
    assertThat(check.getRefusals()).isEmpty();
    assertThat(alertCount()).as("the critical alert no longer stands").isZero();
    assertThat(check.check(DB_NAME, new CopyState(true, 17L), urls, deadPeer))
        .as("nothing left to verify: the copy may be reopened").isNull();
  }

  /** The reopen itself: once accepted, a request that names the database on the leader opens it. */
  @Test
  void anAcceptedCopyIsReopenedByTheNextRequest() throws IOException {
    createClosedCopy();
    assertThat(server.existsDatabase(DB_NAME)).isFalse();

    final ExecutionResponse response = handler().accept(raft, DB_NAME, "user 'root'");
    assertThat(response.getCode()).isEqualTo(200);

    final ServerDatabase reopened = server.getDatabase(DB_NAME);
    assertThat(reopened.isOpen()).isTrue();
    assertThat(reopened.getSchema().existsType("Node")).as("the copy accepted is the one reopened").isTrue();
  }

  /** The audit trail the hand deletion never left: who, which database, at which index, over which refusal. */
  @Test
  void theAcceptanceIsLoggedWithWhoAcceptedWhatOverWhichRefusal() throws IOException {
    createClosedCopy();
    sm.writePersistedAppliedIndex(23L, DB_NAME);
    check.check(DB_NAME, new CopyState(true, 23L), Map.of(DEAD, "http://dead-peer/x"),
        (peer, url, name) -> CompletableFuture.failedFuture(new IOException("connection refused")));

    final List<String> warnings = new CopyOnWriteArrayList<>();
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        if (level == Level.WARNING && requester instanceof UnverifiedClosedCopyCheck)
          warnings.add(args == null || args.length == 0 ? message : String.format(message, args));
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        log(requester, level, message, exception, context,
            new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
                arg15, arg16, arg17 });
      }

      @Override
      public void flush() {
      }
    });
    try {
      check.accept(DB_NAME, "user 'root' (from 10.0.0.7)");
    } finally {
      LogManager.instance().setLogger(previous);
    }

    assertThat(warnings).hasSize(1);
    assertThat(warnings.get(0)).contains(DB_NAME).contains("user 'root' (from 10.0.0.7)").contains("23")
        .contains("dead-peer").contains("connection refused").contains("#8641");
  }

  /** On a follower the marker keeps a copy the leader does not hold from reopening (#8589): never removed there. */
  @Test
  void aFollowerRefusesAndKeepsItsMarker() throws IOException {
    createClosedCopy();
    when(raft.isLeader()).thenReturn(false);

    final ExecutionResponse response = handler().accept(raft, DB_NAME, "user 'root'");
    assertThat(response.getCode()).isEqualTo(400);
    assertThat(response.getResponse()).contains("not the leader");
    assertThat(Files.exists(marker())).isTrue();

    // The role is checked again under the round lock, right before the marker goes.
    assertThatThrownBy(() -> check.accept(DB_NAME, "user 'root'")).isInstanceOf(IllegalStateException.class);
    assertThat(Files.exists(marker())).isTrue();
  }

  /** Nothing to accept: no marked copy, or none at all. The open database is not touched. */
  @Test
  void aDatabaseWithNoMarkedCopyIsNotFound() throws IOException {
    assertThat(handler().accept(raft, DB_NAME, "user 'root'").getCode()).isEqualTo(404);

    server.getOrCreateDatabase(DB_NAME);
    assertThat(handler().accept(raft, DB_NAME, "user 'root'").getCode()).isEqualTo(404);
    assertThat(server.getDatabase(DB_NAME).isOpen()).isTrue();
  }

  /** Root only, a valid name only, and only where Raft runs. */
  @Test
  void theHandlerGuardsItsCaller() throws IOException {
    createClosedCopy();
    final ServerSecurityUser notRoot = mock(ServerSecurityUser.class);
    when(notRoot.getName()).thenReturn("alice");
    assertThatThrownBy(() -> handler().execute(null, notRoot, new JSONObject()))
        .isInstanceOf(ServerSecurityException.class);
    assertThat(Files.exists(marker())).isTrue();

    final ServerSecurityUser root = mock(ServerSecurityUser.class);
    when(root.getName()).thenReturn("root");
    assertThat(handler().execute(null, root, new JSONObject()).getCode()).as("no database in the path").isEqualTo(400);

    final RaftHAPlugin noRaft = mock(RaftHAPlugin.class);
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    assertThat(new PostAcceptCopyHandler(httpServer, noRaft).execute(null, root, new JSONObject()).getCode())
        .isEqualTo(400);
    assertThat(Files.exists(marker())).isTrue();
  }

  // ------------------------------------------------------------------------------------------------------------

  private int alertCount() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addUnverifiedClosedCopyRefusedAlert(check.getRefusals(), null, true, alerts);
    if (alerts.length() > 0)
      assertThat(alerts.getJSONObject(0).getString("recommendation")).contains(PostAcceptCopyHandler.ROUTE);
    return alerts.length();
  }

  private PostAcceptCopyHandler handler() {
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    final RaftHAPlugin plugin = mock(RaftHAPlugin.class);
    when(plugin.getRaftHAServer()).thenReturn(raft);
    return new PostAcceptCopyHandler(httpServer, plugin);
  }

  private void createClosedCopy() throws IOException {
    final ServerDatabase db = server.getOrCreateDatabase(DB_NAME);
    db.transaction(() -> db.getSchema().createVertexType("Node"));
    db.getEmbedded().close();
    server.removeDatabase(DB_NAME);
    Files.writeString(marker(), "");
  }

  private Path marker() {
    return root.resolve("databases").resolve(DB_NAME).resolve(ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE);
  }

  private ArcadeDBServer startServer() throws IOException {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8641");
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
