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
package com.arcadedb.server.support;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.HAServerPlugin.ClusterPeer;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The fan-out of a support request over the other cluster nodes: what is sent to a peer (and only that), what comes back, and
 * that one peer failing is that peer's row. Peers are plain HTTP servers standing in for cluster members; the read-only gate
 * itself lives in the engine of each peer and is exercised by the query endpoint's own tests.
 */
class SupportPeerQueryTest {
  private static final String TOKEN = "cluster-secret-token";

  private record Seen(String method, String path, String token, String user, String hop, String contentType, String body) {
  }

  private final List<HttpServer> peers = new ArrayList<>();
  private final List<Seen>       seen  = new CopyOnWriteArrayList<>();

  @AfterEach
  void stop() {
    peers.forEach(p -> p.stop(0));
  }

  private String start(final Function<String, String[]> answer) throws IOException {
    final HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", exchange -> {
      final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      seen.add(new Seen(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
          exchange.getRequestHeaders().getFirst("X-ArcadeDB-Cluster-Token"),
          exchange.getRequestHeaders().getFirst("X-ArcadeDB-Forwarded-User"),
          exchange.getRequestHeaders().getFirst("X-ArcadeDB-Forwarded-To-Leader"),
          exchange.getRequestHeaders().getFirst("Content-Type"), body));
      final String[] reply = answer.apply(body); // {status, body}
      final byte[] bytes = reply[1].getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(Integer.parseInt(reply[0]), bytes.length);
      try (final OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    });
    server.start();
    peers.add(server);
    return "127.0.0.1:" + server.getAddress().getPort();
  }

  private static String[] ok(final String name) {
    return new String[] { "200", "{\"result\":[{\"node\":\"" + name + "\",\"c\":3}]}" };
  }

  private static SupportPeerQuery.Cluster cluster(final List<ClusterPeer> list, final String token, final boolean ssl) {
    return new SupportPeerQuery.Cluster() {
      @Override
      public List<ClusterPeer> peers() {
        return list;
      }

      @Override
      public String clusterToken() {
        return token;
      }

      @Override
      public boolean useSsl() {
        return ssl;
      }

      @Override
      public HttpClient httpsClient() {
        return null;
      }

      @Override
      public HttpClient plainClient() {
        return HttpClient.newHttpClient();
      }
    };
  }

  private static ClusterPeer peer(final String name, final String address) {
    return new ClusterPeer("id-" + name, name, address, null, address == null ? "no address identifies it" : null);
  }

  private static JSONObject byNode(final JSONObject result, final String node) {
    final JSONArray nodes = result.getJSONArray("nodes");
    for (int i = 0; i < nodes.length(); i++)
      if (node.equals(nodes.getJSONObject(i).getString("node")))
        return nodes.getJSONObject(i);
    throw new AssertionError("no row for " + node + " in " + result);
  }

  @Test
  void allNodesAreAskedWithTheClusterHopAndTheCallersName() throws Exception {
    final String a = start(b -> ok("b"));
    final String b = start(x -> ok("c"));
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(peer("b", a), peer("c", b)), TOKEN, false));

    final JSONObject result = query.run("root", "orders", "sql", "SELECT count(*) AS c FROM V", "all");

    assertThat(result.getBoolean("ha")).isTrue();
    assertThat(result.getJSONArray("nodes").length()).isEqualTo(2);
    assertThat(byNode(result, "b").getString("status")).isEqualTo("ok");
    assertThat(byNode(result, "b").getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(3);
    assertThat(seen).hasSize(2).allSatisfy(s -> {
      assertThat(s.method()).isEqualTo("POST");
      assertThat(s.path()).isEqualTo("/api/v1/query/orders");
      assertThat(s.token()).isEqualTo(TOKEN);
      assertThat(s.user()).isEqualTo("root");
      assertThat(s.hop()).isEqualTo("true");
      final JSONObject body = new JSONObject(s.body());
      assertThat(body.getString("language")).isEqualTo("sql");
      assertThat(body.getString("command")).isEqualTo("SELECT count(*) AS c FROM V");
      assertThat(body.getInt("limit")).isEqualTo(SupportPeerQuery.ROW_LIMIT);
    });
    assertThat(result.toString()).doesNotContain(TOKEN);
  }

  @Test
  void aNamedNodeAsksOnlyThatNode() throws Exception {
    final String a = start(b -> ok("b"));
    final String b = start(x -> ok("c"));
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(peer("b", a), peer("c", b)), TOKEN, false));

    final JSONObject result = query.run("root", "orders", "opencypher", "MATCH (n) RETURN count(n) AS c", "c");

    assertThat(result.getJSONArray("nodes").length()).isEqualTo(1);
    assertThat(byNode(result, "c").getString("status")).isEqualTo("ok");
    assertThat(seen).hasSize(1);
    assertThat(new JSONObject(seen.get(0).body()).getString("language")).isEqualTo("opencypher");
  }

  @Test
  void aNameThatIsNotAMemberIsAFailureNotAnAddress() throws Exception {
    final String a = start(b -> ok("b"));
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(peer("b", a)), TOKEN, false));

    final JSONObject result = query.run("root", "orders", "sql", "SELECT 1", "evil.example.com:80");

    assertThat(byNode(result, "evil.example.com:80").getString("status")).isEqualTo("failed");
    assertThat(seen).isEmpty();
  }

  @Test
  void aPeerThatRefusesOrIsDownIsItsOwnRowAndTheOthersStillAnswer() throws Exception {
    final String good = start(b -> ok("b"));
    final String refusing = start(b -> new String[] { "400", "{\"error\":\"Error on executing query\",\"detail\":\"Cannot execute non-idempotent statement\"}" });
    final int closed;
    try (final ServerSocket s = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
      closed = s.getLocalPort();
    }
    final SupportPeerQuery query = new SupportPeerQuery(cluster(
        List.of(peer("b", good), peer("c", refusing), peer("d", "127.0.0.1:" + closed), peer("e", null)), TOKEN, false));

    final JSONObject result = query.run("root", "orders", "sql", "SELECT 1", "all");

    assertThat(byNode(result, "b").getString("status")).isEqualTo("ok");
    assertThat(byNode(result, "c").getString("error")).contains("HTTP 400").contains("non-idempotent");
    assertThat(byNode(result, "d").getString("error")).contains("not reachable");
    assertThat(byNode(result, "e").getString("error")).contains("not reachable").contains("no address");
  }

  @Test
  void aPeerWithoutAGuardedAddressIsNeverDialled() throws Exception {
    final String a = start(b -> ok("b"));
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(peer("b", null)), TOKEN, false));

    query.run("root", "orders", "sql", "SELECT 1", "all");

    assertThat(seen).isEmpty();
    assertThat(a).isNotNull();
  }

  @Test
  void noClusterTokenMeansNoPeerIsAskedOnBehalfOfAUser() throws Exception {
    final String a = start(b -> ok("b"));
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(peer("b", a)), " ", false));

    final JSONObject result = query.run("root", "orders", "sql", "SELECT 1", "all");

    assertThat(byNode(result, "b").getString("error")).contains("no token");
    assertThat(seen).isEmpty();
  }

  @Test
  void anEncryptedPeerIsNotSentInTheClearWhenNoClientCanBeBuilt() throws Exception {
    final String a = start(b -> ok("b"));
    final ClusterPeer https = new ClusterPeer("id", "b", a, "127.0.0.1:1", null);
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(https), TOKEN, true));

    final JSONObject result = query.run("root", "orders", "sql", "SELECT 1", "all");

    assertThat(byNode(result, "b").getString("error")).contains("encrypted");
    assertThat(seen).isEmpty();
  }

  @Test
  void aSlowPeerTimesOutAndAnOversizedAnswerIsRefused() throws Exception {
    final String slow = start(b -> {
      try {
        Thread.sleep(3_000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return ok("b");
    });
    final String big = start(b -> new String[] { "200", "{\"result\":[{\"x\":\"" + "a".repeat(SupportPeerQuery.MAX_RESPONSE_BYTES) + "\"}]}" });
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(peer("b", slow), peer("c", big)), TOKEN, false), 800L);

    final JSONObject result = query.run("root", "orders", "sql", "SELECT 1", "all");

    assertThat(byNode(result, "b").getString("error")).contains("timed out");
    assertThat(byNode(result, "c").getString("error")).contains("larger than");
  }

  @Test
  void moreThanTheCapOfPeersAreNotAll() throws Exception {
    final String a = start(b -> ok("p"));
    final List<ClusterPeer> many = new ArrayList<>();
    for (int i = 0; i < SupportPeerQuery.MAX_PEERS + 5; i++)
      many.add(peer("n" + i, a));
    final SupportPeerQuery query = new SupportPeerQuery(cluster(many, TOKEN, false));

    final JSONObject result = query.run("root", "orders", "sql", "SELECT 1", "all");

    assertThat(result.getJSONArray("nodes").length()).isEqualTo(SupportPeerQuery.MAX_PEERS);
  }

  @Test
  void aServerThatIsNotInAClusterSaysSo() {
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(), TOKEN, false));

    final JSONObject result = query.run("root", "orders", "sql", "SELECT 1", "all");

    assertThat(result.getBoolean("ha")).isFalse();
    assertThat(result.getJSONArray("nodes").length()).isZero();
    assertThat(query.peers().getBoolean("ha")).isFalse();
  }

  @Test
  void theRequestIsValidatedBeforeAnyPeerIsDialled() throws Exception {
    final String a = start(b -> ok("b"));
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(peer("b", a)), TOKEN, false));

    assertThatThrownBy(() -> query.run("root", "../etc", "sql", "SELECT 1", "all")).isInstanceOf(SupportException.class);
    assertThatThrownBy(() -> query.run("root", "orders", "gremlin", "SELECT 1", "all")).isInstanceOf(SupportException.class);
    assertThatThrownBy(() -> query.run("root", "orders", "sql", "", "all")).isInstanceOf(SupportException.class);
    assertThatThrownBy(() -> query.run("root", "orders", "sql", "x".repeat(SupportPeerQuery.MAX_STATEMENT + 1), "all"))
        .isInstanceOf(SupportException.class);
    assertThatThrownBy(() -> query.run("root", "orders", "sql", "SELECT 1\u0000", "all")).isInstanceOf(SupportException.class);
    assertThatThrownBy(() -> query.run("root", "orders", "sql", "SELECT 1", "bad\nname")).isInstanceOf(SupportException.class);
    assertThatThrownBy(() -> query.run("", "orders", "sql", "SELECT 1", "all")).isInstanceOf(SupportException.class);
    assertThat(seen).isEmpty();
  }

  @Test
  void theListOfPeersCarriesNamesOnly() throws Exception {
    final String a = start(b -> ok("b"));
    final SupportPeerQuery query = new SupportPeerQuery(cluster(List.of(peer("b", a)), TOKEN, false));

    final JSONObject peers = query.peers();

    assertThat(peers.getBoolean("ha")).isTrue();
    assertThat(peers.getJSONArray("peers").getString(0)).isEqualTo("b");
    assertThat(peers.toString()).doesNotContain(a).doesNotContain(TOKEN);
  }
}
