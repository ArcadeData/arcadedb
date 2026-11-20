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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
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
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.sun.net.httpserver.HttpExchange;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unverified closed copies: peer check before reopening, local re-check, and accepting a copy no peer can confirm.
 */
class UnverifiedClosedCopyTest {

  /**
   * Regression tests for issue #8605, HA side: before a leader reopens a closed copy marked unverified by #8589 - which
   * makes it the cluster's copy - it asks every peer about its own copy, over {@code POST /api/v1/cluster/bootstrap-state}
   * with {@code copyOf}, and reopens only when every peer answered and none holds a copy with a higher applied Raft index.
   */
  @Nested
  @Timeout(120)
  class PeerCheck {

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
            ((DatabaseInternal) server.getDatabaseForOperator(DB_NAME)).getEmbedded().drop();
        } catch (final Exception ignore) {
          // best-effort cleanup; the @TempDir is removed regardless
        }
        server.stop();
      }
    }

    // ---------------------------------------------------------------------------------------------- the rule

    /** The issue as reported: the previous leader holds the database closed at a later index than this copy. */
    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
    @Test
    void aPeerHoldingANewerCopyRefusesTheReopen() {
      final String refusal = UnverifiedClosedCopyCheck.verdict(new CopyState(true, 10L),
          Map.of("peer-1", new CopyState(true, 42L), "peer-2", new CopyState(false, -1L)), List.of());

      assertThat(refusal).contains("peer-1").contains("42").contains("10");
    }

    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
    @Test
    void peersHoldingNoCopyOrAnOlderOrEqualOneAllowTheReopen() {
      assertThat(UnverifiedClosedCopyCheck.verdict(new CopyState(true, 42L),
          Map.of("peer-1", new CopyState(true, 42L), "peer-2", new CopyState(true, 7L), "peer-3",
              new CopyState(false, -1L)), List.of())).isNull();
    }

    /** The previous leader down is the likeliest case of all: its copy may be the newer one. */
    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
    @Test
    void aPeerThatCouldNotBeAskedRefusesTheReopen() {
      final String refusal = UnverifiedClosedCopyCheck.verdict(new CopyState(true, 42L),
          Map.of("peer-1", new CopyState(false, -1L)), List.of("peer-2 (connection refused)"));

      assertThat(refusal).contains("peer-2").contains("connection refused");
    }

    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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

    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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
    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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
    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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

    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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
    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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
    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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
            BootstrapElection.HTTP, peer, url, false, name, "s3cr3t");

        final String newer = check.check(DB_NAME, new CopyState(true, 10L), Map.of(PEER_1, urlOf(current)), http);
        assertThat(newer).contains("peer-1").contains("42");
        assertThat(new JSONObject(body.get()).getString(UnverifiedClosedCopyCheck.COPY_OF)).isEqualTo(DB_NAME);
        // The principal travels with the token that proves it, never without one (issue #9999)
        assertThat(forwardedUser.get()).isEqualTo("root");

        assertThat(check.check(DB_NAME, new CopyState(true, 42L), Map.of(PEER_1, urlOf(current)), http)).isNull();

        assertThat(check.check(DB_NAME, new CopyState(true, 42L), Map.of(PEER_2, urlOf(older)), http))
            .contains("peer-2").contains("older version");
      } finally {
        current.stop(0);
        older.stop(0);
      }
    }


    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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

    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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
    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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

    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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
            BootstrapElection.HTTP, peer, url, false, name, null);

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
    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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

    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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

    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
    @Test
    void anOpenCopyIsReportedAtItsAppliedIndex() {
      server.getOrCreateDatabase(DB_NAME);
      sm.writePersistedAppliedIndex(12L, DB_NAME);

      assertThat(UnverifiedClosedCopyCheck.localCopyState(server, sm, DB_NAME)).isEqualTo(new CopyState(true, 12L));
    }

    // Regression for #8605: a leader asks every peer before reopening an unverified closed copy
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

  /**
   * Regression tests for issue #8641: before it, the only way to reopen a leader's closed copy that a peer can never
   * confirm - a peer permanently gone but still in the configuration, or a copy that cannot be ordered - was deleting the
   * {@code .ha-unverified-closed-copy} marker by hand, with no trace of who accepted what. {@code POST
   * /api/v1/cluster/accept-copy/{database}} is the audited override.
   */
  @Nested
  @Timeout(120)
  class AcceptUnverifiedClosedCopy {

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();


    private static final String     DB_NAME  = "db8641";
    private static final String     PASSWORD = "DefaultPasswordForTests";
    private static final RaftPeerId DEAD     = RaftPeerId.valueOf("dead-peer");

    @TempDir
    Path root;

    private ArcadeDBServer            server;
    private ArcadeStateMachine        sm;
    private FakeRaftHAServer              raft;
    private UnverifiedClosedCopyCheck check;

    @BeforeEach
    void setUp() throws IOException {
      server = startServer();
      raft = FakeRaftHAServer.detached().localPeerId(RaftPeerId.valueOf("local")).leader(true);
      sm = new ArcadeStateMachine();
      sm.setServer(server);
      sm.setRaftHAServer(raft);
      raft.stateMachine(sm);
      check = new UnverifiedClosedCopyCheck(raft, server);
      check.refusalReuseMs = 0L;
      raft.unverifiedClosedCopyCheck(check);
    }

    @AfterEach
    void tearDown() throws IOException {
      if (sm != null)
        sm.close();
      if (server != null) {
        try {
          if (server.existsDatabase(DB_NAME))
            ((DatabaseInternal) server.getDatabaseForOperator(DB_NAME)).getEmbedded().drop();
        } catch (final Exception ignore) {
          // best-effort cleanup; the @TempDir is removed regardless
        }
        server.stop();
      }
    }

    // Regression for #8641: an operator can accept an unverified closed copy no peer can confirm
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
    // Regression for #8641: an operator can accept an unverified closed copy no peer can confirm
    @Test
    void anAcceptedCopyIsReopenedByTheNextRequest() throws IOException {
      createClosedCopy();
      assertThat(server.existsDatabase(DB_NAME)).isFalse();

      final ExecutionResponse response = handler().accept(raft, DB_NAME, "user 'root'");
      assertThat(response.getCode()).isEqualTo(200);

      final ServerDatabase reopened = server.getDatabaseForOperator(DB_NAME);
      assertThat(reopened.isOpen()).isTrue();
      assertThat(reopened.getSchema().existsType("Node")).as("the copy accepted is the one reopened").isTrue();
    }

    /** The audit trail the hand deletion never left: who, which database, at which index, over which refusal. */
    // Regression for #8641: an operator can accept an unverified closed copy no peer can confirm
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
      assertThat(warnings.getFirst()).contains(DB_NAME).contains("user 'root' (from 10.0.0.7)").contains("23")
          .contains("dead-peer").contains("connection refused").contains("#8641");
    }

    /** On a follower the marker keeps a copy the leader does not hold from reopening (#8589): never removed there. */
    // Regression for #8641: an operator can accept an unverified closed copy no peer can confirm
    @Test
    void aFollowerRefusesAndKeepsItsMarker() throws IOException {
      createClosedCopy();
      // setUp() made this node the leader, where accepting is allowed; this test is about a follower
      raft.leader(false);

      final ExecutionResponse response = handler().accept(raft, DB_NAME, "user 'root'");
      assertThat(response.getCode()).isEqualTo(400);
      assertThat(response.getResponse()).contains("not the leader");
      assertThat(Files.exists(marker())).isTrue();

      // The role is checked again under the round lock, right before the marker goes.
      assertThatThrownBy(() -> check.accept(DB_NAME, "user 'root'")).isInstanceOf(IllegalStateException.class);
      assertThat(Files.exists(marker())).isTrue();
    }

    /** Nothing to accept: no marked copy, or none at all. The open database is not touched. */
    // Regression for #8641: an operator can accept an unverified closed copy no peer can confirm
    @Test
    void aDatabaseWithNoMarkedCopyIsNotFound() throws IOException {
      assertThat(handler().accept(raft, DB_NAME, "user 'root'").getCode()).isEqualTo(404);

      server.getOrCreateDatabase(DB_NAME);
      assertThat(handler().accept(raft, DB_NAME, "user 'root'").getCode()).isEqualTo(404);
      assertThat(server.getDatabaseForOperator(DB_NAME).isOpen()).isTrue();
    }

    /** Root only, a valid name only, and only where Raft runs. */
    // Regression for #8641: an operator can accept an unverified closed copy no peer can confirm
    @Test
    void theHandlerGuardsItsCaller() throws IOException {
      createClosedCopy();
      final ServerSecurityUser notRoot = TestServerHelper.securityUser("alice");
      assertThatThrownBy(() -> handler().execute(null, notRoot, new JSONObject()))
          .isInstanceOf(ServerSecurityException.class);
      assertThat(Files.exists(marker())).isTrue();

      final ServerSecurityUser root = TestServerHelper.securityUser("root");
      assertThat(handler().execute(null, root, new JSONObject()).getCode()).as("no database in the path").isEqualTo(400);

      final RaftHAPlugin noRaft = new RaftHAPlugin();
      final HttpServer httpServer = HTTP_SERVERS.of(server);
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
      final HttpServer httpServer = HTTP_SERVERS.of(server);
      final RaftHAPlugin plugin = raft.plugin();
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

  /**
   * Regression tests for issue #11518: the #11264 and #11461 quarantines (a unique index with no header page, a dictionary a
   * power loss emptied) were decided only by the Raft startup scan. A copy found damaged by a later schema reload or a later open
   * was served and stayed electable.
   * <p>
   * The invariant: <b>whenever a running HA node reads its copy's index and dictionary files again - a replicated schema entry's
   * reload, an open after the startup scan - a damaged copy is quarantined the way the startup scan quarantines it</b>. The
   * snapshot-install half (an installed copy found damaged is rolled back) is {@link InstalledCopyDamageRollbackTest}.
   * Each test drives a state machine that never ran the startup scan, so only the path under test can raise the quarantine.
   */
  @Nested
  class LocalCopyRecheck {

    private static final String DB   = "db11518";
    private static final String TYPE = "Account";

    @TempDir
    Path root;

    private final AtomicBoolean       soleVoter = new AtomicBoolean(false);
    private       FakeRaftHAServer    raft;
    private       LocalDatabase       database;
    private       CapturingTestLogger logger;

    @BeforeEach
    void setUp() {
      raft = FakeRaftHAServer.detached();
      raft.on("isSoleVoter", args -> soleVoter.get());
      database = (LocalDatabase) new DatabaseFactory(root.resolve(DB).toString()).create();
    }

    @AfterEach
    void tearDown() {
      if (logger != null)
        logger.uninstall();
      GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.reset();
      if (database != null && database.isOpen())
        database.drop();
    }

    // ---- the schema reload of a replicated schema entry, on the apply thread ----

    // Regression for #11518: a copy found damaged after startup is re-checked and quarantined
    @Test
    void aSchemaEntryOnACopyWithAnEmptiedDictionaryQuarantinesIt() throws IOException {
      reopenWithAnEmptiedDictionary();
      final DatabaseBoundStateMachine sm = stateMachine();
      assertThat(sm.isDatabaseDiverged(DB)).as("nothing has rechecked the copy yet").isFalse();

      sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L, false);

      assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.EMPTY_DICTIONARY);
      assertThat(sm.isResyncInProgress()).as("the node leaves the ready set").isTrue();
    }

    // Regression for #11518: a copy found damaged after startup is re-checked and quarantined
    @Test
    void aSchemaEntryTakingAFullReloadStillSeesTheEmptiedDictionary() throws IOException {
      // The full load builds a new dictionary instance over the page the open wrote: it must not read the copy as healthy
      GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(false);
      reopenWithAnEmptiedDictionary();
      final DatabaseBoundStateMachine sm = stateMachine();

      sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L, false);

      assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.EMPTY_DICTIONARY);
    }

    // Regression for #11518: a copy found damaged after startup is re-checked and quarantined
    @Test
    void aSchemaEntryOnACopyWithAHeaderlessUniqueIndexQuarantinesIt() throws Exception {
      GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(false);
      final String compactedName = reopenWithAHeaderlessUniqueIndex();
      assertThat(database.getSchema().getEmbedded().getUnavailableUniqueIndexNames()).containsExactly(compactedName);
      final DatabaseBoundStateMachine sm = stateMachine();

      sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L, false);

      assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.UNAVAILABLE_UNIQUE_INDEX);
    }

    // Regression for #11518: a copy found damaged after startup is re-checked and quarantined
    @Test
    void aSchemaEntryOnAHealthyCopyQuarantinesNothing() {
      createTypeWithAProperty();
      final DatabaseBoundStateMachine sm = stateMachine();

      sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L, false);

      assertThat(sm.isDatabaseDiverged(DB)).isFalse();
      assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();
    }

    // Regression for #11518: a copy found damaged after startup is re-checked and quarantined
    @Test
    void aSoleVoterReportsTheDamageOnceInsteadOfOnEveryReload() throws IOException {
      soleVoter.set(true);
      reopenWithAnEmptiedDictionary();
      final DatabaseBoundStateMachine sm = stateMachine();
      logger = CapturingTestLogger.install();

      for (int i = 0; i < 3; i++)
        sm.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry()), 7L + i, false);

      assertThat(sm.isDatabaseDiverged(DB)).as("nothing could lift a quarantine on a sole voter").isFalse();
      assertThat(logger.formattedAt(Level.SEVERE)).filteredOn(line -> line.contains("only voter")).hasSize(1);
    }

    // ---- an open after the startup scan ----

    // Regression for #11518: a copy found damaged after startup is re-checked and quarantined
    @Test
    void anOpenAfterTheStartupScanQuarantinesADamagedCopy() throws IOException {
      reopenWithAnEmptiedDictionary();
      final DatabaseBoundStateMachine sm = stateMachine();

      assertThat(sm.recheckOpenedDatabase(DB, database)).isTrue();

      assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.EMPTY_DICTIONARY);
    }

    // Regression for #11518: a copy found damaged after startup is re-checked and quarantined
    @Test
    void anOpenOfAHealthyCopyQuarantinesNothing() {
      createTypeWithAProperty();
      final DatabaseBoundStateMachine sm = stateMachine();

      assertThat(sm.recheckOpenedDatabase(DB, database)).isFalse();
      assertThat(sm.isDatabaseDiverged(DB)).isFalse();
    }

    // Regression for #11518: a copy found damaged after startup is re-checked and quarantined
    @Test
    void theRaftServerRechecksNothingBeforeItIsBuilt() throws IOException {
      // A detached server has no Ratis server: the open of a database before the start is the startup scan's to judge
      reopenWithAnEmptiedDictionary();
      final DatabaseBoundStateMachine sm = stateMachine();
      raft.stateMachine(sm);

      raft.recheckOpenedDatabase(DB, database);

      assertThat(sm.isDatabaseDiverged(DB)).isFalse();
    }

    // Regression for #11518: a copy found damaged after startup is re-checked and quarantined
    @Test
    void anOpenDuringASnapshotInstallOfTheSameDatabaseIsLeftToTheInstall() throws IOException {
      // Every install path lifts the quarantine on success, which would erase one raised by the install's own reopen; the
      // install validates the copy it reopens instead
      reopenWithAnEmptiedDictionary();
      final DatabaseBoundStateMachine sm = stateMachine();
      final AtomicReference<Boolean> duringInstall = new AtomicReference<>();

      sm.runUnderInstallGate(DB, () -> duringInstall.set(sm.recheckOpenedDatabase(DB, database)));

      assertThat(duringInstall.get()).isFalse();
      assertThat(sm.isDatabaseDiverged(DB)).isFalse();
      // Once the install is over the same open is judged
      assertThat(sm.recheckOpenedDatabase(DB, database)).isTrue();
    }

    // ---- fixtures ----

    private DatabaseBoundStateMachine stateMachine() {
      final DatabaseBoundStateMachine sm = new DatabaseBoundStateMachine(database);
      sm.setRaftHAServer(raft);
      return sm;
    }

    private void createTypeWithAProperty() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE).createProperty("acctId", Type.LONG));
    }

    private void reopenWithAnEmptiedDictionary() throws IOException {
      createTypeWithAProperty();
      database.close();

      final File[] dictionaryFiles = root.resolve(DB).toFile().listFiles((dir, name) -> name.endsWith("." + Dictionary.DICT_EXT));
      assertThat(dictionaryFiles).as("the fixture has a dictionary file").hasSize(1);
      try (final RandomAccessFile raf = new RandomAccessFile(dictionaryFiles[0], "rw")) {
        raf.setLength(0);
      }

      database = (LocalDatabase) new DatabaseFactory(root.resolve(DB).toString()).open();
      assertThat(database.getSchema().getEmbedded().isDictionaryLostForExistingTypes()).isTrue();
    }

    /** As {@code UnavailableUniqueIndexTest}: the unique index's mutable file swapped for an empty one. */
    private String reopenWithAHeaderlessUniqueIndex() throws Exception {
      database.transaction(() -> {
        database.getSchema().createVertexType(TYPE).createProperty("id", Type.INTEGER).createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
        for (int i = 0; i < 5; i++)
          database.newVertex(TYPE).set("id", i).save();
      });
      final IndexInternal bucketIndex = database.getSchema().getType(TYPE).getAllIndexes(false).iterator().next()
          .getIndexesOnBuckets()[0];
      final LSMTreeIndexMutable mutable = ((LSMTreeIndex) bucketIndex).getMutableIndex();
      final String mutableName = mutable.getName();
      final File mutableFile = new File(database.getFileManager().getFile(mutable.getFileId()).getFilePath());
      final String compactedName = LSMTreeIndexAbstract.compactionBaseName(mutableName) + "_" + System.nanoTime();
      database.close();

      final File schemaFile = root.resolve(DB).resolve(LocalSchema.SCHEMA_FILE_NAME).toFile();
      final String schemaJson = Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8);
      Files.writeString(schemaFile.toPath(), schemaJson.replace(mutableName, compactedName), StandardCharsets.UTF_8);
      Files.deleteIfExists(root.resolve(DB).resolve(LocalSchema.SCHEMA_PREV_FILE_NAME));
      final File headerless = new File(mutableFile.getParentFile(), mutableFile.getName().replace(mutableName, compactedName));
      Files.delete(mutableFile.toPath());
      assertThat(headerless.createNewFile()).isTrue();

      database = (LocalDatabase) new DatabaseFactory(root.resolve(DB).toString()).open();
      return compactedName;
    }

    private ByteString schemaEntry() {
      return RaftLogEntryCodec.encodeSchemaEntry(DB, database.getSchema().getEmbedded().toJSON().toString(),
          Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
          Collections.emptyList(), false, Collections.emptyList(), null, Collections.emptyMap());
    }

    private static class DatabaseBoundStateMachine extends ArcadeStateMachine {
      private final DatabaseInternal database;

      DatabaseBoundStateMachine(final DatabaseInternal database) {
        this.database = database;
      }

      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return database;
      }
    }
  }
}
