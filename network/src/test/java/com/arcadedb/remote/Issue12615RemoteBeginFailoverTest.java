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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.utility.Pair;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12615: {@code RemoteDatabase.begin()} sent its request once, so with the server the client was connected to stopped it
 * failed with "the server could not be reached" on every call, although other nodes were up. Nothing moved the client off the
 * stopped node, because only {@code httpCommand()} fails over. No transaction exists yet when {@code begin()} runs, so it may go
 * to any live node, which is what these tests pin down against scripted loopback nodes.
 */
class Issue12615RemoteBeginFailoverTest {

  private static final String DATABASE    = "db";
  private static final String BEGIN_ROUTE = "/api/v1/begin/" + DATABASE;

  private final List<Node> nodes = new ArrayList<>();
  private       RemoteDatabase database;

  @AfterEach
  void tearDown() {
    if (database != null)
      database.close();
    for (final Node node : nodes)
      node.stop();
  }

  @Test
  void beginFailsOverToALiveNodeWhenTheConnectedOneIsStopped() throws Exception {
    final Node original = node();
    final Node liveReplica = node();

    original.answer(cluster(original.address(), liveReplica.address()));
    liveReplica.answer(cluster(liveReplica.address(), original.address()));
    database = connect(original, RemoteHttpComponent.CONNECTION_STRATEGY.ROUND_ROBIN);

    original.stop();

    database.begin();

    assertThat(liveReplica.requestsTo(BEGIN_ROUTE)).isEqualTo(1);
    assertThat(database.getSessionId()).isEqualTo(Node.SESSION_ID);
  }

  /** The strategy the report used: it pins a transaction to one node, which for a begin that has not run yet must be a live one. */
  @Test
  void stickyBeginPinsTheTransactionToTheNodeItFailedOverTo() throws Exception {
    final Node original = node();
    final Node liveReplica = node();

    original.answer(cluster(original.address(), liveReplica.address()));
    liveReplica.answer(cluster(liveReplica.address(), original.address()));
    database = connect(original, RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);

    original.stop();

    database.begin();
    database.rollback();

    assertThat(liveReplica.requestsTo(BEGIN_ROUTE)).isEqualTo(1);
    assertThat(liveReplica.requestsTo("/api/v1/rollback/" + DATABASE)).as("the transaction stays on the node that began it").isEqualTo(1);
  }

  /**
   * During an election the live replica still names the stopped node as leader. Re-pinning to the advertised leader would send every
   * retry back to the dead node: the begin goes to the server that answered the reload instead, as {@code httpCommand()} does.
   */
  @Test
  void stickyBeginIgnoresAStaleLeaderAdvertisedDuringAnElection() throws Exception {
    final Node original = node();
    final Node liveReplica = node();

    original.answer(cluster(original.address(), liveReplica.address()));
    liveReplica.answer(cluster(original.address(), liveReplica.address()));
    database = connect(original, RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);

    original.stop();

    database.begin();
    database.rollback();

    assertThat(liveReplica.requestsTo(BEGIN_ROUTE)).isEqualTo(1);
    assertThat(liveReplica.requestsTo("/api/v1/rollback/" + DATABASE)).as("the transaction stays on the node that began it").isEqualTo(1);
  }

  /**
   * No server can give the topology (the live replica refuses the cluster route), so the reload fails and changes nothing the client
   * routes by. The begin still walks the replica list the client kept, and the rest of the transaction follows it there.
   */
  @Test
  void beginWalksTheKnownReplicasWhenTheReloadFails() throws Exception {
    final Node original = node();
    final Node liveReplica = node();

    original.answer(cluster(original.address(), liveReplica.address()));
    database = connect(original, RemoteHttpComponent.CONNECTION_STRATEGY.ROUND_ROBIN);

    original.stop();

    database.begin();
    database.rollback();

    assertThat(liveReplica.requestsTo(BEGIN_ROUTE)).isEqualTo(1);
    assertThat(liveReplica.requestsTo("/api/v1/rollback/" + DATABASE)).as("the transaction stays on the node that began it").isEqualTo(1);
  }

  /**
   * Connected to a replica, whose topology names that replica too. With the reload failing, a walk that ignored what it had tried
   * started on the replica that had just failed and spent the whole budget of two attempts on it, never reaching the live one.
   */
  @Test
  void beginDoesNotRetryTheReplicaThatJustFailedWhenTheReloadFails() throws Exception {
    final Node leader = node();
    final Node connected = node();
    final Node liveReplica = node();

    connected.answer(cluster(leader.address(), connected.address(), liveReplica.address()));
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ERROR_RETRIES, 2);
    database = connect(connected, RemoteHttpComponent.CONNECTION_STRATEGY.ROUND_ROBIN, configuration);

    connected.stop();
    leader.stop();

    database.begin();
    database.rollback();

    assertThat(liveReplica.requestsTo(BEGIN_ROUTE)).isEqualTo(1);
    assertThat(liveReplica.requestsTo("/api/v1/rollback/" + DATABASE)).as("the transaction stays on the node that began it").isEqualTo(1);
  }

  /**
   * Every replica is down and the leader, the one live node, refuses the topology. The walk of the replica list never named the leader,
   * so the default budget (every known server once) went to the replicas, one of them twice, and the begin failed.
   */
  @Test
  void beginReachesTheLeaderOnceEveryReplicaWasTried() throws Exception {
    final Node leader = node();
    final Node connected = node();
    final Node otherReplica = node();

    connected.answer(cluster(leader.address(), otherReplica.address(), connected.address()));
    database = connect(connected, RemoteHttpComponent.CONNECTION_STRATEGY.ROUND_ROBIN);

    connected.stop();
    otherReplica.stop();

    database.begin();
    database.rollback();

    assertThat(leader.requestsTo(BEGIN_ROUTE)).isEqualTo(1);
    assertThat(leader.requestsTo("/api/v1/rollback/" + DATABASE)).as("the transaction stays on the node that began it").isEqualTo(1);
  }

  /**
   * A server that answered the reload but that this call already could not reach goes behind the servers not tried yet: picking it
   * again would spend the budget on it while a known replica was never tried.
   */
  @Test
  void aReloadResponderAlreadyTriedGoesBehindTheServersNotTriedYet() throws Exception {
    final Node connected = node();
    final Pair<String, Integer> leader = new Pair<>("127.0.0.1", 1);
    final Pair<String, Integer> b = new Pair<>("127.0.0.1", 2);
    final Pair<String, Integer> c = new Pair<>("127.0.0.1", 3);
    final Pair<String, Integer> d = new Pair<>("127.0.0.1", 4);

    connected.answer(cluster("127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3", "127.0.0.1:4"));
    database = connect(connected, RemoteHttpComponent.CONNECTION_STRATEGY.ROUND_ROBIN);

    final Field source = RemoteHttpComponent.class.getDeclaredField("lastConfigurationSource");
    source.setAccessible(true);
    source.set(database, b);

    assertThat(database.nextFailoverServer(c, false, new HashSet<>(List.of(leader, b, c)))).isEqualTo(d);
    assertThat(database.nextFailoverServer(c, false, new HashSet<>(List.of(leader, c)))).as("a responder not tried yet comes first")
        .isEqualTo(b);
  }

  /** FIXED means the same server, as it does for every other request: a stopped one is reported, not routed around. */
  @Test
  void fixedStrategyDoesNotLeaveTheServer() throws Exception {
    final Node original = node();
    final Node liveReplica = node();

    original.answer(cluster(original.address(), liveReplica.address()));
    liveReplica.answer(cluster(liveReplica.address(), original.address()));
    database = connect(original, RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);

    original.stop();

    assertThatThrownBy(() -> database.begin()).isInstanceOf(NeedRetryException.class);
    assertThat(liveReplica.requestsTo(BEGIN_ROUTE)).isZero();
  }

  /** The failover is bounded: with nothing left to answer, begin still ends in the retryable refusal it always gave. */
  @Test
  void beginStillGivesUpWhenNoNodeAnswers() throws Exception {
    final Node original = node();
    final Node replica = node();

    original.answer(cluster(original.address(), replica.address()));
    database = connect(original, RemoteHttpComponent.CONNECTION_STRATEGY.ROUND_ROBIN);

    original.stop();
    replica.stop();

    assertThatThrownBy(() -> database.begin()).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("the server could not be reached");
    assertThat(database.getSessionId()).isNull();
  }

  private RemoteDatabase connect(final Node node, final RemoteHttpComponent.CONNECTION_STRATEGY strategy) {
    return connect(node, strategy, new ContextConfiguration());
  }

  private RemoteDatabase connect(final Node node, final RemoteHttpComponent.CONNECTION_STRATEGY strategy,
      final ContextConfiguration configuration) {
    final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", node.port(), DATABASE, "root", "test", configuration);
    remote.setConnectionStrategy(strategy);
    return remote;
  }

  private Node node() throws IOException {
    final Node node = new Node();
    nodes.add(node);
    return node;
  }

  private static String cluster(final String leader, final String... replicas) {
    return "{\"ha\":{\"leaderAddress\":\"" + leader + "\",\"replicaAddresses\":\"" + String.join(",", replicas) + "\"}}";
  }

  /** A loopback node: answers the cluster route with its script, and begin and rollback the way a server does. */
  private static final class Node {
    private static final String SESSION_ID = "AS-12615";

    private final    HttpServer   http;
    private final    List<String> requests = new CopyOnWriteArrayList<>();
    private volatile String       clusterAnswer;
    private          boolean      stopped;

    private Node() throws IOException {
      http = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      http.createContext("/api/v1/", this::handle);
      http.start();
    }

    private void handle(final HttpExchange exchange) throws IOException {
      // NO KEEP-ALIVE: A CONNECTION THE CLIENT REUSED AFTER THE NODE STOPPED WOULD BE RETRIED SILENTLY BY THE JDK CLIENT
      exchange.getResponseHeaders().add("Connection", "close");

      final String path = exchange.getRequestURI().getPath();
      requests.add(path);

      if ("/api/v1/server".equals(path)) {
        if (clusterAnswer == null) {
          exchange.sendResponseHeaders(500, -1);
          exchange.close();
        } else {
          final byte[] body = clusterAnswer.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        }
      } else {
        if (BEGIN_ROUTE.equals(path))
          exchange.getResponseHeaders().add("arcadedb-session-id", SESSION_ID);
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
      }
    }

    private void answer(final String clusterAnswer) {
      this.clusterAnswer = clusterAnswer;
    }

    private int requestsTo(final String route) {
      return (int) requests.stream().filter(route::equals).count();
    }

    private int port() {
      return http.getAddress().getPort();
    }

    private String address() {
      return "127.0.0.1:" + port();
    }

    private void stop() {
      if (!stopped) {
        stopped = true;
        http.stop(0);
      }
    }
  }
}
