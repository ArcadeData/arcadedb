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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11739: a follower that forwards {@code create database} or {@code drop database} to the leader relays the leader's 200
 * only once it has applied what the leader had committed ({@link HAServerPlugin#awaitLeaderCommitApplied()}), because it installs
 * or drops the database only when it applies the entry. The 3-node case, with the follower's apply held back, is
 * {@code RaftCreateDatabaseOnFollowerIT}; this test pins the decisions of {@link PostServerCommandHandler} around that wait: which
 * forwarded commands wait, that the wait starts only after the leader answered, and that a wait that cannot confirm does not
 * change the answer, since the leader made the change either way.
 * <p>
 * The leader is a real HTTP listener on the loopback interface; the HA plugin is a recording stand-in for the Raft one.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue11739ForwardedCreateDropWaitTest {
  private static final String LOCAL_PEER  = "peer-follower";
  private static final String LEADER_PEER = "peer-leader";

  @RegisterExtension
  static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

  private final AtomicInteger waits                    = new AtomicInteger();
  private final AtomicInteger leaderRequestsWhenWaited = new AtomicInteger(-1);
  private volatile boolean    confirms                 = true;
  private StubLeader          leader;

  @BeforeEach
  void startLeader() throws IOException {
    leader = new StubLeader();
  }

  @AfterEach
  void stopLeader() {
    leader.close();
    LeaderForwardContext.clear();
  }

  @Test
  void aForwardedCreateDatabaseIsRelayedOnlyOnceThisServerConfirmedItApplied() throws Exception {
    leader.answer(200, "{\"result\":\"ok\"}");

    final ExecutionResponse response = forward("create database Issue11739");

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(waits.get()).as("the follower waits for its own apply before it answers").isEqualTo(1);
    assertThat(leaderRequestsWhenWaited.get()).as("the wait starts once the leader answered the forwarded command").isEqualTo(1);
  }

  @Test
  void aForwardedDropDatabaseWaitsToo() throws Exception {
    leader.answer(200, "{\"result\":\"ok\"}");

    final ExecutionResponse response = forward("drop database Issue11739");

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(waits.get()).isEqualTo(1);
    assertThat(leaderRequestsWhenWaited.get()).isEqualTo(1);
  }

  /** The leader made the change, so a wait that could not confirm it still answers what the leader answered. */
  @Test
  void aWaitThatCouldNotConfirmStillRelaysTheLeadersAnswer() throws Exception {
    leader.answer(200, "{\"result\":\"ok\"}");
    confirms = false;

    final ExecutionResponse response = forward("create database Issue11739");

    assertThat(waits.get()).isEqualTo(1);
    assertThat(response.getCode()).isEqualTo(200);
    assertThat(new JSONObject(response.getResponse()).getString("result")).isEqualTo("ok");
  }

  /** Not a create or a drop of a database: the follower already answered these consistently, and nothing is added for them. */
  @Test
  void aForwardedCommandThatIsNotACreateOrDropOfADatabaseDoesNotWait() throws Exception {
    leader.answer(200, "{\"result\":\"ok\"}");

    final ExecutionResponse response = forward("drop user issue11739");

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(waits.get()).isZero();
  }

  /** The leader refused, so nothing was created: there is no entry to wait for, and its refusal reaches the client unchanged. */
  @Test
  void aForwardedCreateTheLeaderRefusedDoesNotWait() throws Exception {
    leader.answer(409, "{\"error\":\"Database 'Issue11739' already exists\"}");

    final ExecutionResponse response = forward("create database Issue11739");

    assertThat(response.getCode()).isEqualTo(409);
    assertThat(waits.get()).isZero();
  }

  private ExecutionResponse forward(final String command) throws Exception {
    final PostServerCommandHandler handler = new PostServerCommandHandler(httpServerWith(ha()));
    return handler.execute(exchange(), TestServerHelper.securityUser("root"), new JSONObject().put("command", command));
  }

  /** A follower of the stub leader. Everything it is not asked about answers what an unstubbed mock would: nothing. */
  private HAServerPlugin ha() {
    return (HAServerPlugin) Proxy.newProxyInstance(HAServerPlugin.class.getClassLoader(), new Class<?>[] { HAServerPlugin.class },
        (proxy, method, args) -> switch (method.getName()) {
          case "isLeader" -> false;
          case "getLeaderAddress" -> leader.address();
          case "getLeaderName" -> "leader";
          case "getLeaderPeerId" -> LEADER_PEER;
          case "getLocalPeerId" -> LOCAL_PEER;
          case "getClusterToken" -> "issue11739-cluster-token";
          case "awaitLeaderCommitApplied" -> {
            waits.incrementAndGet();
            leaderRequestsWhenWaited.set(leader.requests());
            yield confirms;
          }
          case "toString" -> "HAServerPlugin stand-in of issue #11739";
          case "hashCode" -> System.identityHashCode(proxy);
          case "equals" -> proxy == args[0];
          default -> defaultAnswer(method.getReturnType());
        });
  }

  private static Object defaultAnswer(final Class<?> type) {
    if (!type.isPrimitive())
      return Optional.class == type ? Optional.empty()
          : List.class == type || Collection.class == type ? List.of()
          : Map.class == type ? Map.of()
          : Set.class == type ? Set.of()
          : null;
    if (type == boolean.class)
      return false;
    if (type == long.class)
      return 0L;
    if (type == int.class)
      return 0;
    if (type == double.class)
      return 0D;
    if (type == float.class)
      return 0F;
    if (type == short.class)
      return (short) 0;
    if (type == byte.class)
      return (byte) 0;
    if (type == char.class)
      return (char) 0;
    return null;
  }

  private static HttpServer httpServerWith(final HAServerPlugin ha) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, 30_000L);
    configuration.setValue(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT, 30_000L);
    configuration.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
    configuration.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 30_000L);
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, configuration);
    server.setHA(ha);
    return HTTP_SERVERS.of(server);
  }

  private static HttpServerExchange exchange() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.setRequestPath("/api/v1/server");
    exchange.setRequestURI("/api/v1/server");
    exchange.setRequestMethod(Methods.POST);
    exchange.getRequestHeaders().put(new HttpString("Authorization"), "Basic cm9vdDpwbGF5d2l0aGRhdGE=");
    return exchange;
  }

  /** The leader: a loopback HTTP listener that counts the requests it was asked and answers as a test tells it to. */
  private static final class StubLeader implements AutoCloseable {
    private final com.sun.net.httpserver.HttpServer server;
    private final ExecutorService                  executor = Executors.newCachedThreadPool();
    private final AtomicInteger                    requests = new AtomicInteger();
    private volatile int                           status;
    private volatile String                        json;

    StubLeader() throws IOException {
      server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
      server.setExecutor(executor);
      server.createContext("/", exchange -> {
        try {
          exchange.getRequestBody().readAllBytes();
          final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          // Counted once the answer is on its way, which is when the forwarding follower can proceed
          requests.incrementAndGet();
          exchange.sendResponseHeaders(status, bytes.length);
          exchange.getResponseBody().write(bytes);
        } finally {
          exchange.close();
        }
      });
      server.start();
    }

    void answer(final int status, final String json) {
      this.status = status;
      this.json = json;
    }

    String address() {
      return "127.0.0.1:" + server.getAddress().getPort();
    }

    int requests() {
      return requests.get();
    }

    @Override
    public void close() {
      server.stop(0);
      executor.shutdownNow();
    }
  }
}
