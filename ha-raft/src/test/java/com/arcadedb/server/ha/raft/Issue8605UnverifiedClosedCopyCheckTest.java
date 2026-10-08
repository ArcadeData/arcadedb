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
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.ha.raft.UnverifiedClosedCopyCheck.CopyState;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #8605, HA side: before a leader reopens a closed copy marked unverified by #8589 - which
 * makes it the cluster's copy - it asks every peer about its own copy, over {@code POST /api/v1/cluster/bootstrap-state}
 * with {@code copyOf}, and reopens only when every peer answered and none holds a copy with a higher applied Raft index.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Timeout(120)
class Issue8605UnverifiedClosedCopyCheckTest {
  @RegisterExtension
  static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();


  private static final String     DB_NAME  = "db8605";
  private static final String     PASSWORD = "DefaultPasswordForTests";
  private static final RaftPeerId PEER_1   = RaftPeerId.valueOf("peer-1");
  private static final RaftPeerId PEER_2   = RaftPeerId.valueOf("peer-2");

  @TempDir
  Path root;

  private ArcadeDBServer     server;
  private ArcadeStateMachine sm;
  private FakeRaftHAServer       raft;

  @BeforeEach
  void setUp() throws IOException {
    server = startServer();
    raft = FakeRaftHAServer.detached().localPeerId(RaftPeerId.valueOf("local"));
    sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.setRaftHAServer(raft);
    raft.stateMachine(sm);
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
  void everyPeerIsAskedAndAPeerWithNoAddressOrAFailedCallRefuses() throws IOException {
    createClosedCopy(true);
    final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
    final Map<RaftPeerId, String> urls = new LinkedHashMap<>();
    urls.put(PEER_1, "http://peer-1/api/v1/cluster/bootstrap-state");
    urls.put(PEER_2, null);

    assertThat(check.check(DB_NAME, new CopyState(true, 5L), urls, (peer, url, name) -> done(new CopyState(false, -1L))))
        .contains("peer-2").contains("no HTTP address");

    urls.put(PEER_2, "http://peer-2/api/v1/cluster/bootstrap-state");
    check.refusalReuseMs = 0L;
    assertThat(check.check(DB_NAME, new CopyState(true, 5L), urls, (peer, url, name) -> url.contains("peer-2") ?
        CompletableFuture.failedFuture(new IOException("HTTP 503")) :
        done(new CopyState(false, -1L)))).contains("peer-2").contains("HTTP 503");

    assertThat(check.check(DB_NAME, new CopyState(true, 5L), urls, (peer, url, name) -> {
      assertThat(name).isEqualTo(DB_NAME);
      return done(new CopyState(true, 5L));
    })).isNull();
  }

  /** A refusal stands in the cluster alert while the mark does, and goes with a verified reopen or with the mark. */
  @Test
  void aRefusalIsReportedWhileItsMarkStands() throws IOException {
    createClosedCopy(true);
    final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
    check.refusalReuseMs = 0L;
    final Map<RaftPeerId, String> urls = Map.of(PEER_1, "http://peer-1/x");

    check.check(DB_NAME, new CopyState(true, 5L), urls, (peer, url, name) -> done(new CopyState(true, 9L)));
    assertThat(check.getRefusals()).containsOnlyKeys(DB_NAME);

    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addUnverifiedClosedCopyRefusedAlert(check.getRefusals(), null, true, alerts);
    assertThat(alerts.length()).isEqualTo(1);
    assertThat(alerts.getJSONObject(0).getString("id")).isEqualTo("unverified-closed-copy-refused");
    assertThat(alerts.getJSONObject(0).getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);

    final JSONArray hidden = new JSONArray();
    ClusterAlerts.addUnverifiedClosedCopyRefusedAlert(check.getRefusals(), Set.of("other"), true, hidden);
    assertThat(hidden.length()).as("a database the caller cannot see raises nothing").isZero();

    check.check(DB_NAME, new CopyState(true, 9L), urls, (peer, url, name) -> done(new CopyState(true, 9L)));
    assertThat(check.getRefusals()).as("verified: the refusal no longer stands").isEmpty();

    check.check(DB_NAME, new CopyState(true, 5L), urls, (peer, url, name) -> done(new CopyState(true, 9L)));
    Files.delete(marker());
    assertThat(check.getRefusals()).as("the operator removed the mark: the refusal no longer stands").isEmpty();
  }

  /** A client retrying, or a dashboard polling the database, does not run one round per request. */
  @Test
  void aRecentRefusalIsHandedBackWithoutAskingAgain() throws IOException {
    createClosedCopy(true);
    final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
    final Map<RaftPeerId, String> urls = Map.of(PEER_1, "http://peer-1/x");
    final AtomicInteger asked = new AtomicInteger();

    final String first = check.check(DB_NAME, new CopyState(true, 5L), urls, (peer, url, name) -> {
      asked.incrementAndGet();
      return done(new CopyState(true, 9L));
    });
    final String second = check.check(DB_NAME, new CopyState(true, 5L), urls, (peer, url, name) -> {
      asked.incrementAndGet();
      return done(new CopyState(true, 5L));
    });

    assertThat(second).isEqualTo(first).isNotNull();
    assertThat(asked.get()).isEqualTo(1);
  }

  /**
   * A request that waited behind a round which passed finds the mark gone - its caller reopened the copy - and has
   * nothing left to verify: no second round.
   */
  @Test
  void aCopyWhoseMarkIsGoneIsNotAskedAbout() {
    final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
    final AtomicInteger asked = new AtomicInteger();

    assertThat(check.check(DB_NAME, new CopyState(true, 5L), Map.of(PEER_1, "http://peer-1/x"), (peer, url, name) -> {
      asked.incrementAndGet();
      return done(new CopyState(true, 9L));
    })).isNull();
    assertThat(asked.get()).isZero();
  }

  /** Every peer is asked at once; one that never answers is unanswered once the round's deadline passes. */
  @Test
  void peersThatNeverAnswerAreUnansweredAtTheRoundsDeadline() throws IOException {
    createClosedCopy(true);
    final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
    check.roundTimeoutMs = 50L;
    final Map<RaftPeerId, String> urls = new LinkedHashMap<>();
    urls.put(PEER_1, "http://peer-1/x");
    urls.put(PEER_2, "http://peer-2/x");
    final AtomicInteger asked = new AtomicInteger();

    final String refusal = check.check(DB_NAME, new CopyState(true, 5L), urls, (peer, url, name) -> {
      asked.incrementAndGet();
      return new CompletableFuture<>();
    });

    assertThat(asked.get()).as("both asked before either is waited on").isEqualTo(2);
    assertThat(refusal).contains("peer-1").contains("peer-2").contains("no answer within");
  }

  // ---------------------------------------------------------------------------------------------- the wire

  /** The real HTTP path: the request a peer receives, its answer, and an older peer's answer that is not one. */
  @Test
  void peersAreAskedOverHttpAndAnOlderPeersListingIsNotAnAnswer() throws IOException {
    createClosedCopy(true);
    final AtomicReference<String> body = new AtomicReference<>();
    final AtomicReference<String> forwardedUser = new AtomicReference<>();
    final com.sun.net.httpserver.HttpServer current = peer(exchange -> {
      body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      forwardedUser.set(exchange.getRequestHeaders().getFirst("X-ArcadeDB-Forwarded-User"));
      return new JSONObject().put("peerId", "peer-1")
          .put(UnverifiedClosedCopyCheck.COPY, new CopyState(true, 42L).toJSON(DB_NAME)).toString();
    });
    final com.sun.net.httpserver.HttpServer older = peer(
        exchange -> new JSONObject().put("peerId", "peer-2").put("databases", new JSONArray()).toString());
    try {
      final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
      check.refusalReuseMs = 0L;
      final UnverifiedClosedCopyCheck.PeerQuestion http = (peer, url, name) -> UnverifiedClosedCopyCheck.askOverHttp(
          BootstrapElection.HTTP, peer, url, name, null);

      final String newer = check.check(DB_NAME, new CopyState(true, 10L), Map.of(PEER_1, urlOf(current)), http);
      assertThat(newer).contains("peer-1").contains("42");
      assertThat(new JSONObject(body.get()).getString(UnverifiedClosedCopyCheck.COPY_OF)).isEqualTo(DB_NAME);
      assertThat(forwardedUser.get()).isEqualTo("root");

      assertThat(check.check(DB_NAME, new CopyState(true, 42L), Map.of(PEER_1, urlOf(current)), http)).isNull();

      assertThat(check.check(DB_NAME, new CopyState(true, 42L), Map.of(PEER_2, urlOf(older)), http))
          .contains("peer-2").contains("older version");
    } finally {
      current.stop(0);
      older.stop(0);
    }
  }


  @Test
  void anAnswerWithoutTheCopyIsNotAnAnswer() throws IOException {
    assertThatThrownBy(() -> UnverifiedClosedCopyCheck.parseAnswer(200,
        new JSONObject().put("databases", new JSONArray()).put("peerId", "peer-1").toString(), "peer-1", "http://x"))
        .isInstanceOf(IOException.class).hasMessageContaining("older version");
    assertThatThrownBy(() -> UnverifiedClosedCopyCheck.parseAnswer(403, "{}", "peer-1", "http://x"))
        .isInstanceOf(IOException.class);

    final CopyState parsed = UnverifiedClosedCopyCheck.parseAnswer(200, new JSONObject().put("peerId", "peer-1")
        .put(UnverifiedClosedCopyCheck.COPY, new CopyState(true, 77L).toJSON(DB_NAME)).toString(), "peer-1", "http://x");
    assertThat(parsed).isEqualTo(new CopyState(true, 77L));
  }

  // ---------------------------------------------------------------------------------------------- who answered (#8703)

  /**
   * Issue #8703: an answer counts as the peer's only when the peer wrote it - not another node behind its address, not
   * the leader itself, not a node that names nobody. The same check every other reader of the endpoint makes.
   */
  @Test
  void anAnswerWrittenByAnotherNodeIsNotThePeersAnswer() {
    final String copy = new CopyState(false, -1L).toJSON(DB_NAME).toString();

    assertThatThrownBy(() -> UnverifiedClosedCopyCheck.parseAnswer(200,
        "{\"peerId\":\"peer-3\",\"copy\":" + copy + "}", "peer-1", "http://peer-1/x"))
        .isInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class)
        .hasMessageContaining("peer-1").hasMessageContaining("peer-3").hasMessageContaining("http://peer-1/x");
    assertThatThrownBy(() -> UnverifiedClosedCopyCheck.parseAnswer(200,
        "{\"peerId\":\"local\",\"copy\":" + copy + "}", "peer-1", "http://peer-1/x"))
        .as("the leader answering its own question").isInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class);
    assertThatThrownBy(() -> UnverifiedClosedCopyCheck.parseAnswer(200, "{\"copy\":" + copy + "}", "peer-1",
        "http://peer-1/x")).as("an answer that names nobody")
        .isInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class).hasMessageContaining("names no peer");
  }

  /** The round hands every question the peer it is meant for, so the answer can be checked against it. */
  @Test
  void eachPeerIsAskedUnderItsOwnId() throws IOException {
    createClosedCopy(true);
    final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
    final Map<RaftPeerId, String> urls = new LinkedHashMap<>();
    urls.put(PEER_1, "http://peer-1/x");
    urls.put(PEER_2, "http://peer-2/x");
    final Map<String, String> askedFor = new LinkedHashMap<>();

    assertThat(check.check(DB_NAME, new CopyState(true, 5L), urls, (peer, url, name) -> {
      askedFor.put(url, peer);
      return done(new CopyState(false, -1L));
    })).isNull();
    assertThat(askedFor).containsEntry("http://peer-1/x", "peer-1").containsEntry("http://peer-2/x", "peer-2");
  }

  /**
   * The issue's two consequences end to end over HTTP: peer-1's address answered by peer-3, which does not hold the
   * database, and by the leader itself, at the leader's own index. Either used to count as peer-1's answer and let the
   * leader reopen a copy peer-1 may hold a newer version of; now the peer counts as unanswered and the reopen is refused.
   */
  @Test
  void aStrangerOrTheLeaderItselfAnsweringForAPeerRefusesTheReopen() throws IOException {
    createClosedCopy(true);
    final com.sun.net.httpserver.HttpServer stranger = peer(exchange -> new JSONObject().put("peerId", "peer-3")
        .put(UnverifiedClosedCopyCheck.COPY, new CopyState(false, -1L).toJSON(DB_NAME)).toString());
    final com.sun.net.httpserver.HttpServer itself = peer(exchange -> new JSONObject().put("peerId", "local")
        .put(UnverifiedClosedCopyCheck.COPY, new CopyState(true, 42L).toJSON(DB_NAME)).toString());
    try {
      final UnverifiedClosedCopyCheck check = new UnverifiedClosedCopyCheck(raft, server);
      check.refusalReuseMs = 0L;
      final UnverifiedClosedCopyCheck.PeerQuestion http = (peer, url, name) -> UnverifiedClosedCopyCheck.askOverHttp(
          BootstrapElection.HTTP, peer, url, name, null);

      assertThat(check.check(DB_NAME, new CopyState(true, 42L), Map.of(PEER_1, urlOf(stranger)), http))
          .contains("could not all be asked").contains("peer-1").contains("peer-3");
      assertThat(check.check(DB_NAME, new CopyState(true, 42L), Map.of(PEER_1, urlOf(itself)), http))
          .contains("could not all be asked").contains("peer-1").contains("'local'");
      assertThat(check.getRefusals()).containsOnlyKeys(DB_NAME);
    } finally {
      stranger.stop(0);
      itself.stop(0);
    }
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
  void theHandlerRejectsAPathInTheName() throws Exception {
    assertThat(handlerResponse("../etc").getCode()).as("a client error, not a 500").isEqualTo(400);
  }

  // ------------------------------------------------------------------------------------------------------------

  private static CompletableFuture<CopyState> done(final CopyState state) {
    return CompletableFuture.completedFuture(state);
  }

  // Fully qualified: the JDK's HttpServer clashes with com.arcadedb.server.http.HttpServer, imported above.
  private static com.sun.net.httpserver.HttpServer peer(final Answer answer) throws IOException {
    final com.sun.net.httpserver.HttpServer peer = com.sun.net.httpserver.HttpServer.create(
        new InetSocketAddress("localhost", 0), 0);
    peer.createContext(BootstrapElection.BOOTSTRAP_STATE_ROUTE, exchange -> {
      final byte[] bytes = answer.answer(exchange).getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, bytes.length);
      try (final OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    });
    peer.start();
    return peer;
  }

  private static String urlOf(final com.sun.net.httpserver.HttpServer peer) {
    return BootstrapElection.chooseUrl("localhost:" + peer.getAddress().getPort(), null, false);
  }

  @FunctionalInterface
  private interface Answer {
    String answer(HttpExchange exchange) throws IOException;
  }

  private JSONObject askHandler(final String name) throws Exception {
    final ExecutionResponse response = handlerResponse(name);
    assertThat(response.getCode()).isEqualTo(200);
    return new JSONObject(response.getResponse());
  }

  private ExecutionResponse handlerResponse(final String name) throws Exception {
    final HttpServer httpServer = HTTP_SERVERS.of(server);
    final RaftHAPlugin plugin = raft.plugin();
    final ServerSecurityUser root = TestServerHelper.securityUser("root");

    return new PostBootstrapStateHandler(httpServer, plugin).execute(null, root,
        new JSONObject().put(UnverifiedClosedCopyCheck.COPY_OF, name));
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
