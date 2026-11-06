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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11304: the client's failover drives the real {@code reloadClusterConfiguration()} against scripted loopback nodes. A
 * failed refresh moves on to the next known server and changes nothing the client knows, and the retry that follows goes to a
 * server that is proven to answer.
 */
class Issue11304RemoteClusterReloadFailoverTest {

  private static final String SERVER_ROUTE = "/api/v1/server";
  private static final String EXISTS_ROUTE = "/api/v1/exists/db";

  private final List<Node> nodes  = new ArrayList<>();
  private       RemoteServer client;

  @AfterEach
  void tearDown() {
    if (client != null)
      client.close();
    for (final Node node : nodes)
      node.stop();
  }

  @Test
  void reloadTriesTheNextReplicaWhenTheFirstCannotProvideAClusterConfiguration() throws Exception {
    final Node original = node();
    final Node liveReplica = node();
    final String refused = refusedAddress();

    original.answer(cluster(original.address(), refused, liveReplica.address()));
    liveReplica.answer(cluster(liveReplica.address(), ""));
    client = connect(original);

    original.stop();

    assertThat(client.reloadClusterConfiguration()).isTrue();
    assertThat(liveReplica.requestsTo(SERVER_ROUTE)).isEqualTo(1);
    assertThat(client.getLeaderAddress()).isEqualTo(liveReplica.address());
  }

  @Test
  void reloadReportsFailureWhenOldLeaderReturnsMalformedConfiguration() throws Exception {
    final Node original = node();
    final Node failingReplica = node();

    original.answer(cluster(original.address(), failingReplica.address()));
    client = connect(original);

    original.answer("{\"ha\":{\"leaderAddress\":\"" + original.address() + "\"}}");

    assertThat(client.reloadClusterConfiguration()).isFalse();
    assertThat(client.getLeaderAddress()).as("a failed reload must not leave the client without a leader").isEqualTo(original.address());
  }

  /** The server's replica list names every configured peer, dead or not, so the first entry of a fresh list proves nothing. */
  @Test
  void theRetryGoesToTheServerThatAnsweredNotToTheFirstEntryOfTheNewList() throws Exception {
    final Node original = node();
    final Node newLeader = node();
    final Node follower = node();
    final String refused = refusedAddress();

    original.answer(cluster(original.address(), refused, newLeader.address(), follower.address()));
    final String afterTheFailure = cluster(newLeader.address(), original.address(), refused, follower.address());
    newLeader.answer(afterTheFailure);
    follower.answer(afterTheFailure);
    client = connect(original);

    original.stop();

    assertThat(client.exists("db")).isTrue();
    assertThat(newLeader.requestsTo(EXISTS_ROUTE)).isEqualTo(1);
  }

  /** The topology may name internal addresses the client cannot reach: the address it was given stays a way back in. */
  @Test
  void reloadAsksTheOriginalServerWhenTheAdvertisedTopologyIsUnreachable() throws Exception {
    final Node original = node();
    final String internalLeader = refusedAddress();
    final String internalReplica = refusedAddress();

    original.answer(cluster(internalLeader, internalReplica));
    client = connect(original);

    assertThat(client.reloadClusterConfiguration()).isTrue();
    assertThat(original.requestsTo(SERVER_ROUTE)).isEqualTo(2);
  }

  @Test
  void aFailedReloadKeepsWhatTheClientKnowsAndALaterOneRecovers() throws Exception {
    final Node original = node();
    final Node replica = node();

    original.answer(cluster(original.address(), replica.address()));
    client = connect(original);

    original.drop(Integer.MAX_VALUE);
    replica.drop(Integer.MAX_VALUE);

    assertThat(client.reloadClusterConfiguration()).isFalse();
    assertThat(client.getLeaderAddress()).isEqualTo(original.address());
    assertThat(client.getReplicaAddresses()).containsExactly(replica.address());

    original.drop(0);

    assertThat(client.reloadClusterConfiguration()).isTrue();
    assertThat(client.getLeaderAddress()).isEqualTo(original.address());
  }

  @Test
  void anAnswerThatIsRejectedPublishesNoneOfItsContent() throws Exception {
    final Node original = node();
    final Node replica = node();

    original.answer(cluster(original.address(), replica.address()));
    replica.answer("{\"ha\":{\"leaderAddress\":\"127.0.0.1:1\"}}");
    client = connect(original);

    original.drop(Integer.MAX_VALUE);

    assertThat(client.reloadClusterConfiguration()).isFalse();
    assertThat(client.getLeaderAddress()).isEqualTo(original.address());
    assertThat(client.getReplicaAddresses()).containsExactly(replica.address());
  }

  /** A node that is not in the cluster (yet) has no topology to give: it must not cost the client the one it has. */
  @Test
  void aReplicaThatAnswersWithoutHaDoesNotReplaceTheTopology() throws Exception {
    final Node original = node();
    final Node standalone = node();
    final Node newLeader = node();

    original.answer(cluster(original.address(), standalone.address(), newLeader.address()));
    standalone.answer("{}");
    newLeader.answer(cluster(newLeader.address(), ""));
    client = connect(original);

    original.stop();

    assertThat(client.reloadClusterConfiguration()).isTrue();
    assertThat(client.getLeaderAddress()).isEqualTo(newLeader.address());
  }

  @Test
  void aStandaloneServerStaysADirectConnectionAcrossAReload() throws Exception {
    final Node original = node();

    original.answer("{}");
    client = connect(original);

    assertThat(client.reloadClusterConfiguration()).isTrue();
    assertThat(client.getLeaderAddress()).isEqualTo(original.address());
    assertThat(client.getReplicaAddresses()).isEmpty();
  }

  /** While an election runs every follower answers without a leader: the topology is unknown, the followers are not gone. */
  @Test
  void aReadStillReachesAFollowerWhileNoServerCanNameTheLeader() throws Exception {
    final Node original = node();
    final Node follower = node();
    final Node otherFollower = node();

    original.answer(cluster(original.address(), follower.address(), otherFollower.address()));
    final String electing = "{\"ha\":{\"leaderAddress\":null,\"replicaAddresses\":\"" + original.address() + "\"}}";
    follower.answer(electing);
    otherFollower.answer(electing);
    client = connect(original);

    original.stop();

    assertThat(client.exists("db")).isTrue();
    assertThat(follower.requestsTo(EXISTS_ROUTE)).isEqualTo(1);
    assertThat(client.getReplicaAddresses()).containsExactly(follower.address(), otherFollower.address());
  }

  /** The retries the application configured are spent even when the only server misses a refresh in between. */
  @Test
  void aFailedRefreshDoesNotCancelTheConfiguredRetriesOnASingleServer() throws Exception {
    final Node only = node();

    only.answer(cluster(only.address(), ""));
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ERROR_RETRIES, 3);
    client = new RemoteServer("127.0.0.1", only.port(), "root", "test", configuration);

    only.drop(2);

    assertThat(client.exists("db")).isTrue();
    assertThat(only.requestsTo(EXISTS_ROUTE)).as("the one that got through, after the call and the refresh were both dropped").isEqualTo(1);
  }

  @Test
  void aServerBeingProbedIsNotWhereOtherCallsAreRoutedToMeanwhile() throws Exception {
    final Node original = node();
    final Node failingReplica = node();
    final Node liveReplica = node();
    final List<String> routeDuringProbe = new CopyOnWriteArrayList<>();

    original.answer(cluster(original.address(), failingReplica.address(), liveReplica.address()));
    liveReplica.answer(cluster(liveReplica.address(), ""));
    failingReplica.onServerRoute(() -> routeDuringProbe.add(client.getUrl("x")));
    client = connect(original);

    assertThat(client.reloadClusterConfiguration()).isTrue();

    assertThat(routeDuringProbe).hasSize(1);
    assertThat(routeDuringProbe.getFirst()).contains(original.address());
    assertThat(client.getUrl("x")).contains(liveReplica.address());
  }

  @Test
  void anInterruptedThreadStopsWalkingTheCluster() throws Exception {
    final Node original = node();
    final Node replica = node();
    final Node lastReplica = node();

    original.answer(cluster(original.address(), replica.address(), lastReplica.address()));
    replica.answer(cluster(replica.address(), ""));
    lastReplica.answer(cluster(lastReplica.address(), ""));
    client = connect(original);

    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> client.reloadClusterConfiguration()).isInstanceOf(RemoteException.class);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
    assertThat(lastReplica.requestsTo(SERVER_ROUTE)).isZero();
  }

  @Test
  void aReloadThatFailsEverywhereForgetsTheAdvertisedPorts() throws Exception {
    final Node original = node();

    original.answer("{\"ports\":{\"gremlin\":18182}," + cluster(original.address(), "").substring(1));
    client = connect(original);
    assertThat(client.getAdvertisedPort("gremlin")).isEqualTo(18182);

    original.drop(Integer.MAX_VALUE);

    assertThat(client.reloadClusterConfiguration()).isFalse();
    assertThat(client.getAdvertisedPort("gremlin")).as("a failed refresh must not keep the previous ports").isZero();
  }

  @Test
  void theConstructorStillFallsBackToADirectConnection() throws Exception {
    final Node original = node();

    client = connect(original);

    assertThat(client.getLeaderAddress()).isEqualTo(original.address());
    assertThat(client.getReplicaAddresses()).isEmpty();
  }

  /** A refused connection has no message of its own: the log used to end in "(null)". */
  @Test
  void aFailureWithoutAMessageIsDescribedByItsClass() {
    assertThat(RemoteHttpComponent.describeFailure(new ConnectException())).isEqualTo("ConnectException");
    assertThat(RemoteHttpComponent.describeFailure(new ConnectException("refused"))).isEqualTo("refused");
  }

  private RemoteServer connect(final Node node) {
    return new RemoteServer("127.0.0.1", node.port(), "root", "test");
  }

  private Node node() throws IOException {
    final Node node = new Node();
    nodes.add(node);
    return node;
  }

  private static String cluster(final String leader, final String... replicas) {
    return "{\"ha\":{\"leaderAddress\":\"" + leader + "\",\"replicaAddresses\":\"" + String.join(",", replicas) + "\"}}";
  }

  /** An address nothing listens on. Every node a test uses is bound before this is called, so none of them can be given it. */
  private static String refusedAddress() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
      return "127.0.0.1:" + socket.getLocalPort();
    }
  }

  /** A loopback node: answers the cluster route with its script (500 without one) and the exists route with {@code true}. */
  private static final class Node {
    private final    HttpServer    http;
    private final    List<String>  requests = new CopyOnWriteArrayList<>();
    private final    AtomicInteger drops    = new AtomicInteger();
    private volatile String        clusterAnswer;
    private volatile Runnable      onServerRoute;
    private          boolean       stopped;

    private Node() throws IOException {
      http = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      http.createContext("/api/v1/", this::handle);
      http.start();
    }

    private void handle(final HttpExchange exchange) throws IOException {
      // NO KEEP-ALIVE: THE JDK CLIENT SILENTLY RETRIES A GET WHOSE REUSED CONNECTION WAS CLOSED, WHICH WOULD HIDE A DROP
      exchange.getResponseHeaders().add("Connection", "close");

      if (drops.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
        exchange.close();
        return;
      }

      final String path = exchange.getRequestURI().getPath();
      requests.add(path);

      if (SERVER_ROUTE.equals(path)) {
        if (onServerRoute != null)
          onServerRoute.run();
        if (clusterAnswer == null) {
          exchange.sendResponseHeaders(500, -1);
          exchange.close();
        } else
          respond(exchange, clusterAnswer);
      } else
        respond(exchange, "{\"result\":true}");
    }

    private static void respond(final HttpExchange exchange, final String response) throws IOException {
      final byte[] body = response.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    }

    private void answer(final String clusterAnswer) {
      this.clusterAnswer = clusterAnswer;
    }

    private void onServerRoute(final Runnable onServerRoute) {
      this.onServerRoute = onServerRoute;
    }

    /** Closes the next {@code count} connections without an answer, which the client sees as an IO failure. */
    private void drop(final int count) {
      drops.set(count);
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
