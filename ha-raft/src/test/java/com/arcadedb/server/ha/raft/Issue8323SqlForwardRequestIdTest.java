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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.http.IdempotencyCache;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8323: the SQL write a follower forwards to the leader
 * ({@code RaftReplicatedDatabase.forwardCommandToLeaderViaRaft}) did not carry the client's {@code X-Request-Id}, so
 * the leader executed it outside its idempotency cache and a retry after a lost answer ran the write a second time.
 * The leader here is a plain HTTP server that records the header of every forward it receives; the forward is driven
 * through the private method the same way {@code Issue7737ForwardDeadlineHeadroomTest} does.
 */
class Issue8323SqlForwardRequestIdTest {

  @AfterEach
  void clearContext() {
    ForwardedRequestIdContext.clear();
  }

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

  /** No id published - no client id, a session-scoped request, an embedded caller - means no id invented. */
  @Test
  void aRequestWithoutAnIdForwardsWithoutOne() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly((String) null);
    }
  }

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
        String.class, String.class, Map.class, Object[].class, ContextConfiguration.class);
    m.setAccessible(true);
    m.invoke(db, "sql", command, null, new Object[0], config());
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
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(cfg);
    when(server.getHA()).thenReturn(null); // plain HTTP forward, no HTTPS dial to resolve
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getLeaderHttpAddress()).thenReturn(leader.address());
    when(raft.getClusterToken()).thenReturn(clusterToken);
    when(raft.isLeader()).thenReturn(localIsLeader);
    // A node that became the leader resolves the leader's address to its own.
    when(raft.isOwnHttpAddress(leader.address())).thenReturn(localIsLeader);
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
}
