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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.http.IdempotencyCache;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * What a follower sends along when it forwards a write to the leader: the client's request id / idempotency key, the whole-request body and the command budget.
 */
class ForwardedWriteRequestPropagationTest {
  // ---- #8323 ----
  // Regression test for issue #8323: the SQL write a follower forwards to the leader
  // ({@code RaftReplicatedDatabase.forwardCommandToLeaderViaRaft}) did not carry the client's {@code X-Request-Id}, so
  // the leader executed it outside its idempotency cache and a retry after a lost answer ran the write a second time.
  // The leader here is a plain HTTP server that records the header of every forward it receives; the forward is driven
  // through the private method the same way {@code ForwardedWriteDeadlineTest} does.

  @AfterEach
  void clearContext() {
    ForwardedRequestIdContext.clear();
  }

  // Regression for #8323: the forward carries the clients request id
  @Test
  void theForwardCarriesTheClientsRequestId() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8323");

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly("client-8323");
      assertThat(leader.ordinals()).as("the first forward sends no ordinal").containsExactly((String) null);
    }
  }

  // Regression for #8323: two forwards in one request never share A leader cache key
  /**
   * Two forwards of the same statement under one request: sent under the bare id, the leader would answer the second
   * from the first one's cache entry, and the second write would silently never run. The id stays the client's, and
   * the ordinal travels in its own header - never folded into the id, where a client could send the same value.
   */
  @Test
  void twoForwardsInOneRequestNeverShareALeaderCacheKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8323");

      forward(db, "INSERT INTO V SET id = 1");
      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly("client-8323", "client-8323");
      assertThat(leader.ordinals()).containsExactly((String) null, "2");
    }
  }

  // Regression for #8323: without A cluster token A later forward relays no id
  /**
   * Without a cluster token the leader does not honor the ordinal, so a second forward sent with the bare id would
   * share the first one's key there: it relays no id at all instead.
   */
  @Test
  void withoutAClusterTokenALaterForwardRelaysNoId() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, false, null);
      ForwardedRequestIdContext.set("client-8323");

      forward(db, "INSERT INTO V SET id = 1");
      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly("client-8323", null);
      assertThat(leader.ordinals()).containsExactly((String) null, (String) null);
    }
  }

  // Regression for #8323: a request without an id forwards without one
  /** No id published - no client id, a session-scoped request, an embedded caller - means no id invented. */
  @Test
  void aRequestWithoutAnIdForwardsWithoutOne() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly((String) null);
    }
  }

  // Regression for #8323: a forward to itself as the new leader relays no id
  /**
   * This node became the leader while the forward waited for one, so the POST goes to itself: its cache is the
   * leader's, the request already holds its reservation there, and relaying the id would only make a forward whose
   * body matches the client's wait out the in-flight timeout on that pending reservation.
   */
  @Test
  void aForwardToItselfAsTheNewLeaderRelaysNoId() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, true);
      ForwardedRequestIdContext.set("client-8323");

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly((String) null);
    }
  }

  static void forward(final RaftReplicatedDatabase db, final String command) throws Exception {
    final Method m = RaftReplicatedDatabase.class.getDeclaredMethod("forwardCommandToLeaderViaRaft",
        String.class, String.class, Map.class, Object[].class);
    m.setAccessible(true);
    m.invoke(db, "sql", command, null, new Object[0]);
  }

  static ContextConfiguration config() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 30_000L);
    return cfg;
  }

  static RaftReplicatedDatabase database(final RecordingLeader leader) {
    return database(leader, false);
  }

  static RaftReplicatedDatabase database(final RecordingLeader leader, final boolean localIsLeader) {
    return database(leader, localIsLeader, "test-token");
  }

  static RaftReplicatedDatabase database(final RecordingLeader leader, final boolean localIsLeader,
      final String clusterToken) {
    return database(leader, localIsLeader, clusterToken, mock(LocalDatabase.class));
  }

  static RaftReplicatedDatabase database(final RecordingLeader leader, final boolean localIsLeader,
      final String clusterToken, final LocalDatabase proxied) {
    final ContextConfiguration cfg = config();
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leaderHttpAddress(leader.address()).clusterToken(clusterToken)
        .leader(localIsLeader);
    // A node that became the leader resolves the leader's address to its own: the real isOwnHttpAddress compares it
    // with this node's address.
    if (localIsLeader)
      raft.localHttpAddress(leader.address());
    return new RaftReplicatedDatabase(server, proxied, raft);
  }

  /**
   * A leader that answers every command with one empty-result success and records its request id, ordinal and client
   * key (issue #8347).
   */
  static final class RecordingLeader implements AutoCloseable {
    private final HttpServer   server;
    private final List<String> requestIds = new CopyOnWriteArrayList<>();
    private final List<String> ordinals   = new CopyOnWriteArrayList<>();
    private final List<String> clientKeys = new CopyOnWriteArrayList<>();
    private final List<String> bodies     = new CopyOnWriteArrayList<>();
    private final String       answer;

    RecordingLeader() throws IOException {
      this(new JSONObject().put("result", new JSONArray()).toString());
    }

    /** A leader that answers every command with the given body (issue #8359). */
    RecordingLeader(final String answer) throws IOException {
      this.answer = answer;
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
      server.createContext("/", exchange -> {
        requestIds.add(exchange.getRequestHeaders().getFirst(IdempotencyCache.HEADER_REQUEST_ID));
        ordinals.add(exchange.getRequestHeaders().getFirst(ForwardedRequestIdContext.FORWARD_ORDINAL_HEADER));
        clientKeys.add(exchange.getRequestHeaders().getFirst(ForwardedRequestIdContext.CLIENT_KEY_HEADER));
        try (final InputStream in = exchange.getRequestBody()) {
          bodies.add(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        final byte[] bytes = this.answer.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (final OutputStream out = exchange.getResponseBody()) {
          out.write(bytes);
        }
      });
      server.start();
    }

    List<String> requestIds() {
      return requestIds;
    }

    List<String> ordinals() {
      return ordinals;
    }

    List<String> clientKeys() {
      return clientKeys;
    }

    List<String> bodies() {
      return bodies;
    }

    String address() {
      return server.getAddress().getAddress().getHostAddress() + ":" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
  // ---- #8347 ----
  // Issue #8347, the forwarding side: the SQL write a follower forwards to the leader carries a body rebuilt from the
  // statement, so the leader could not match it to a retry the client sends it directly with its own body. The forward
  // now relays, beside the client's id, the key the client's request has on the follower - but only when the forward IS
  // that whole request. A write issued by a command the follower executes itself, or any forward after the first, is a
  // part of the request: settling the client's key with its answer would replay that answer to a direct retry of the
  // whole request.

  private static final String KEY  = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
  private static final String BODY = "{\"language\":\"sql\",\"command\":\"INSERT INTO V SET id = 1\"}";

  // Regression for #8347: the forward of A one command request relays the client key
  @Test
  void theForwardOfAOneCommandRequestRelaysTheClientKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly("client-8347");
      assertThat(leader.clientKeys()).containsExactly(KEY);
    }
  }

  // Regression for #8347: a second forward in the same request relays no key
  @Test
  void aSecondForwardInTheSameRequestRelaysNoKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");
      forward(db, "INSERT INTO V SET id = 2");

      assertThat(leader.clientKeys()).containsExactly(KEY, null);
    }
  }

  // Regression for #8347: a route that is not one command relays no key
  /** A route whose body is not one command (the handler did not opt in) never relays its key on a command forward. */
  @Test
  void aRouteThatIsNotOneCommandRelaysNoKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8347", KEY, false);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly("client-8347");
      assertThat(leader.clientKeys()).containsExactly((String) null);
    }
  }

  // Regression for #8347: without A cluster token no key is sent
  /** Without a cluster token the leader would not honor it, so it is not sent. */
  @Test
  void withoutAClusterTokenNoKeyIsSent() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, false, null);
      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.clientKeys()).containsExactly((String) null);
    }
  }

  // Regression for #8347: a forward to itself as the new leader relays no key
  /** The POST goes to this node itself, which holds the client's reservation already: nothing is relayed. */
  @Test
  void aForwardToItselfAsTheNewLeaderRelaysNoKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, true);
      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.clientKeys()).containsExactly((String) null);
    }
  }

  // Regression for #8347: a write issued by A command executed locally relays no key
  /**
   * Through the real command() entry point: a write command is forwarded whole and relays the key; a read-only command
   * is executed on the follower, and the write it issues from inside - forwarded on its own - is a part of the request
   * and relays none.
   */
  @Test
  void aWriteIssuedByACommandExecutedLocallyRelaysNoKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final LocalDatabase proxied = mock(LocalDatabase.class);
      final QueryEngineManager engines = mock(QueryEngineManager.class);
      final QueryEngine engine = mock(QueryEngine.class);
      when(proxied.getQueryEngineManager()).thenReturn(engines);
      when(engines.getEngine(eq("sql"), any())).thenReturn(engine);
      when(engine.analyze(anyString())).thenAnswer(inv -> analyzed(((String) inv.getArgument(0)).startsWith("SELECT")));

      final RaftReplicatedDatabase db = database(leader, false, "test-token", proxied);
      // The local execution of the read-only command issues one write through the same database.
      when(proxied.command(eq("sql"), eq("SELECT nested()"), any(), any(Object[].class))).thenAnswer(inv -> {
        db.command("sql", "INSERT INTO V SET id = 3");
        return new InternalResultSet();
      });

      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);
      db.command("sql", "INSERT INTO V SET id = 1");
      assertThat(leader.clientKeys()).as("the write command is the whole request").containsExactly(KEY);

      ForwardedRequestIdContext.set("client-8347-b", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "SELECT nested()", BODY);
      db.command("sql", "SELECT nested()");
      assertThat(leader.requestIds()).containsExactly("client-8347", "client-8347-b");
      assertThat(leader.clientKeys()).as("the nested write is a part of the request").containsExactly(KEY, null);
    }
  }

  private static QueryEngine.AnalyzedQuery analyzed(final boolean idempotent) {
    return new QueryEngine.AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return idempotent;
      }

      @Override
      public boolean isDDL() {
        return false;
      }
    };
  }
  // ---- #8359 ----
  // Issue #8359, the forwarding side: the forward that relays the client's key (issue #8347) used to post a body rebuilt
  // from the statement, so the leader answered it - and settled the client's key with - its default rendering, not the
  // 'serializer', 'limit' or 'typeHints' the client's body names. That forward now posts the client's own body, and the
  // leader's answer is handed back to the handler to be sent as it is. A forward that is only a part of the request keeps
  // the rebuilt body and hands nothing back.

  private static final String KEY8359     = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  private static final String COMMAND = "INSERT INTO V SET id = 1";
  private static final String BODY8359    =
      "{ \"serializer\": \"studio\", \"limit\": 7, \"typeHints\": true, \"language\": \"sql\", \"command\": \"" + COMMAND
          + "\" }";
  // What a leader renders for the 'studio' serializer: not a row array, so it cannot be parsed back into rows.
  private static final String STUDIO_ANSWER =
      "{\"user\":\"root\",\"result\":{\"vertices\":[],\"edges\":[],\"records\":[{\"id\":1}]},\"limit\":7}";

  // Regression for #8359: the whole request forward posts the clients body and hands back the leaders answer
  @Test
  void theWholeRequestForwardPostsTheClientsBodyAndHandsBackTheLeadersAnswer() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader(STUDIO_ANSWER)) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8359", KEY8359, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", COMMAND, BODY8359);

      forward(db, COMMAND);

      assertThat(leader.bodies()).as("the client's own body, byte for byte").containsExactly(BODY8359);
      assertThat(leader.clientKeys()).containsExactly(KEY8359);
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer())
          .as("the leader's answer in the client's rendering, as it is").isEqualTo(STUDIO_ANSWER);
    }
  }

  // Regression for #8359: a second forward posts the rebuilt body
  /** A second forward in the same request is a part of it: the body is rebuilt and nothing is handed back. */
  @Test
  void aSecondForwardPostsTheRebuiltBody() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader(STUDIO_ANSWER)) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8359", KEY8359, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", COMMAND, BODY8359);

      forward(db, COMMAND);
      ForwardedRequestIdContext.takeWholeRequestAnswer();
      forward(db, COMMAND);

      assertThat(leader.bodies()).hasSize(2);
      assertRebuilt(leader.bodies().get(1));
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();
    }
  }

  // Regression for #8359: a forward of another statement posts the rebuilt body
  /** A write the declared statement issues from inside - another statement - is a part of the request. */
  @Test
  void aForwardOfAnotherStatementPostsTheRebuiltBody() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8359", KEY8359, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "SELECT writes()", BODY8359);

      forward(db, COMMAND);

      assertRebuilt(leader.bodies().get(0));
      assertThat(leader.clientKeys()).containsExactly((String) null);
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();
    }
  }

  // Regression for #8359: without A cluster token the body is rebuilt
  /** Without the cluster token the key is not relayed, and neither is the body: the forward is what it was before. */
  @Test
  void withoutAClusterTokenTheBodyIsRebuilt() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, false, null);
      ForwardedRequestIdContext.set("client-8359", KEY8359, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", COMMAND, BODY8359);

      forward(db, COMMAND);

      assertRebuilt(leader.bodies().get(0));
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();
    }
  }

  // Regression for #8359: a forward to itself posts the rebuilt body
  /** A POST to this node itself, the new leader, runs the request here: nothing to substitute or hand back. */
  @Test
  void aForwardToItselfPostsTheRebuiltBody() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, true);
      ForwardedRequestIdContext.set("client-8359", KEY8359, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", COMMAND, BODY8359);

      forward(db, COMMAND);

      assertRebuilt(leader.bodies().get(0));
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();
    }
  }

  private static void assertRebuilt(final String body) {
    final JSONObject json = new JSONObject(body);
    assertThat(json.getString("command")).isEqualTo(COMMAND);
    assertThat(json.has("serializer")).as("a rebuilt body carries none of the client's rendering fields").isFalse();
    assertThat(json.has("limit")).isFalse();
  }
  /**
   * Issue #8313: a follower sized the deadline of a forwarded write from the {@code arcadedb.command.timeout} in the
   * configuration its caller passed - which the convenience {@code command(...)} overloads and the HTTP handler fill with
   * the SERVER configuration, and which no command context reads - and the leader enforced its own database setting. The
   * forward now resolves the budget from this database's configuration, the rule every command context applies, and
   * sends it to the leader so both sides enforce the same number. The leader's half is
   * {@code LeaderForwardTimeoutTest.ForwardedCommandTimeoutBudget} in the server module.
   */
  @Nested
  class CommandBudget {
    private HeaderRecordingLeader leader;

    @BeforeEach
    void startLeader() throws IOException {
      leader = new HeaderRecordingLeader();
    }

    @AfterEach
    void stopLeader() {
      leader.close();
    }

    // Regression for #8313: the databases budget travels with the forward
    @Test
    void theDatabasesBudgetTravelsWithTheForward() throws Exception {
      final ContextConfiguration databaseConfig = new ContextConfiguration();
      databaseConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 5_000L);

      forward(database(serverConfig(), databaseConfig, "test-token"));

      assertThat(leader.budgets()).containsExactly(Optional.of("5000"));
    }

    // Regression for #8313: the server configuration is not the budget
    /**
     * The reported second case: the server configuration the convenience overloads pass is not the database's, and a
     * budget set on the database was not seen by the follower either. The server's value plays no part now.
     */
    @Test
    void theServerConfigurationIsNotTheBudget() throws Exception {
      final ContextConfiguration serverConfig = serverConfig();
      serverConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 99_000L);
      final ContextConfiguration databaseConfig = new ContextConfiguration();
      databaseConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 7_000L);

      forward(database(serverConfig, databaseConfig, "test-token"));

      assertThat(leader.budgets()).containsExactly(Optional.of("7000"));
    }

    // Regression for #8313: an unbounded command sends no budget
    @Test
    void anUnboundedCommandSendsNoBudget() throws Exception {
      final ContextConfiguration databaseConfig = new ContextConfiguration();
      databaseConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);

      forward(database(serverConfig(), databaseConfig, "test-token"));

      assertThat(leader.budgets()).as("the leader then applies its own setting").containsExactly(Optional.empty());
    }

    // Regression for #8313: without A cluster token no budget is sent
    @Test
    void withoutAClusterTokenNoBudgetIsSent() throws Exception {
      final ContextConfiguration databaseConfig = new ContextConfiguration();
      databaseConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 5_000L);

      forward(database(serverConfig(), databaseConfig, null));

      assertThat(leader.budgets()).as("the leader honours it under the token only, so it is not put on the wire without one")
          .containsExactly(Optional.empty());
    }

    private static void forward(final RaftReplicatedDatabase db) throws Exception {
      final Method m = RaftReplicatedDatabase.class.getDeclaredMethod("forwardCommandToLeaderViaRaft",
          String.class, String.class, Map.class, Object[].class);
      m.setAccessible(true);
      m.invoke(db, "sql", "INSERT INTO V SET id = 1", null, new Object[0]);
    }

    private static ContextConfiguration serverConfig() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 30_000L);
      return cfg;
    }

    private RaftReplicatedDatabase database(final ContextConfiguration serverConfig, final ContextConfiguration databaseConfig,
        final String clusterToken) {
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, serverConfig);
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leaderHttpAddress(leader.address());
      raft.clusterToken(clusterToken);
      final LocalDatabase local = mock(LocalDatabase.class);
      when(local.getConfiguration()).thenReturn(databaseConfig);
      return new RaftReplicatedDatabase(server, local, raft);
    }

    /** A leader that answers every command with an empty result and records the budget header it was sent. */
    private static final class HeaderRecordingLeader implements AutoCloseable {
      private final HttpServer                   server;
      private final List<Optional<String>>       budgets = new CopyOnWriteArrayList<>();

      HeaderRecordingLeader() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
        server.createContext("/", exchange -> {
          budgets.add(Optional.ofNullable(
              exchange.getRequestHeaders().getFirst(LeaderForwardContext.FORWARDED_COMMAND_TIMEOUT_HEADER)));
          final byte[] bytes = "{\"result\":[]}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, bytes.length);
          try (final OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        });
        server.start();
      }

      String address() {
        return server.getAddress().getAddress().getHostAddress() + ":" + server.getAddress().getPort();
      }

      List<Optional<String>> budgets() {
        return budgets;
      }

      @Override
      public void close() {
        server.stop(0);
      }
    }

  }
}
