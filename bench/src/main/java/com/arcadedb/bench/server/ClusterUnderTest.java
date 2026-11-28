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
package com.arcadedb.bench.server;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Raft cluster of the image under test: {@code nodes} members {@code ha-0..ha-<n-1>}, each a container on its own slice of
 * the server cpuset, on one Docker network. The Raft port stays inside that network.
 */
public final class ClusterUnderTest implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterUnderTest.class);

  static final int      RAFT_PORT          = 2434;
  static final int      HTTP_PORT          = 2480;
  static final Duration LEADER_WAIT        = Duration.ofSeconds(90);
  private static final Duration ANSWER_TIMEOUT = Duration.ofSeconds(2);

  /** A cluster member as the cluster sees it; the seam that lets the start logic run without a container. */
  interface Node extends AutoCloseable {
    ServerUnderTest server();

    String deathReason();

    /** The body of {@code GET /api/v1/cluster} as this member answers it; null when it does not answer. */
    String clusterAnswer();

    @Override
    void close();
  }

  @FunctionalInterface
  interface NodeStarter {
    Node start(int index, String cpuset, int memLimitGb, String heap, Map<String, String> settings);
  }

  @FunctionalInterface
  interface LeaderWait {
    /** The index of the leader, -1 when none was elected in time. */
    int await(List<Node> nodes);
  }

  private final String         image;
  private final String[]       cpusets;
  private final int            nodeMemGb;
  private final String         heap;
  private final List<Node>     nodes;
  private final AutoCloseable  network;
  private volatile int         leader;

  private ClusterUnderTest(final String image, final String[] cpusets, final int nodeMemGb, final String heap,
      final List<Node> nodes, final AutoCloseable network, final int leader) {
    this.image = image;
    this.cpusets = cpusets;
    this.nodeMemGb = nodeMemGb;
    this.heap = heap;
    this.nodes = nodes;
    this.network = network;
    this.leader = leader;
  }

  /**
   * Starts the cluster and waits up to 90 s for a leader that is ready to serve ({@link #leaderOf}). The members start
   * concurrently, each waiting for {@code /api/v1/health} (its HTTP server is up), not {@code /api/v1/ready}, which with
   * {@code readinessRequiresHA} answers only once a quorum has elected a leader; the leader wait comes after every member
   * answered. Everything started is closed on failure.
   */
  public static ClusterUnderTest start(final String image, final String serverCpus, final int memLimitGb, final int nodes,
      final Map<String, String> settings) {
    final Network network = Network.newNetwork();
    return start(image, serverCpus, memLimitGb, nodes, settings, (index, cpuset, memGb, heap, memberSettings) -> {
      final ServerUnderTest server = ServerUnderTest.startMember(image, cpuset, memGb, heap, memberSettings, network, "ha-" + index);
      return new Node() {
        @Override
        public ServerUnderTest server() {
          return server;
        }

        @Override
        public String deathReason() {
          // container state plus /api/v1/health: /api/v1/ready answers 503 during an election or while this member lags
          return server.memberDeathReason();
        }

        @Override
        public String clusterAnswer() {
          return clusterAnswerOf(server);
        }

        @Override
        public void close() {
          server.close();
        }
      };
    }, ClusterUnderTest::waitForLeader, network::close);
  }

  static ClusterUnderTest start(final String image, final String serverCpus, final int memLimitGb, final int nodes,
      final Map<String, String> settings, final NodeStarter starter, final LeaderWait leaderWait, final AutoCloseable network) {
    final String[] cpusets = Cpusets.split(serverCpus, nodes);
    final int nodeMemGb = Cpusets.nodeMemGb(memLimitGb, nodes);
    final String heap = Cpusets.nodeHeap(nodeMemGb);

    final List<Node> started = new ArrayList<>();
    final ExecutorService executor = Executors.newFixedThreadPool(nodes, r -> {
      final Thread t = new Thread(r, "bench-ha-start");
      t.setDaemon(true);
      return t;
    });
    try {
      final List<CompletableFuture<Node>> futures = new ArrayList<>();
      for (int i = 0; i < nodes; i++) {
        final int index = i;
        futures.add(CompletableFuture.supplyAsync(
            () -> starter.start(index, cpusets[index], nodeMemGb, heap, memberSettings(settings, index, nodes)), executor));
      }
      // join every future before deciding, so a failure still knows which members to close
      RuntimeException failure = null;
      for (final CompletableFuture<Node> future : futures)
        try {
          started.add(future.join());
        } catch (final RuntimeException e) {
          if (failure == null)
            failure = e.getCause() instanceof RuntimeException cause ? cause : e;
          else
            failure.addSuppressed(e);
        }
      if (failure != null)
        throw failure;

      final int leader = leaderWait.await(started);
      if (leader < 0)
        throw new IllegalStateException("No Raft leader elected and ready to serve within " + LEADER_WAIT.toSeconds() + " s");
      return new ClusterUnderTest(image, cpusets, nodeMemGb, heap, started, network, leader);
    } catch (final Throwable e) {
      closeAll(started, network);
      throw e;
    } finally {
      executor.shutdown();
    }
  }

  /** The settings of member {@code index}: the Raft ones, the caller's winning, the member name and server list last. */
  static Map<String, String> memberSettings(final Map<String, String> settings, final int index, final int nodes) {
    final Map<String, String> all = new LinkedHashMap<>();
    all.put("arcadedb.ha.enabled", "true");
    all.put("arcadedb.ha.quorum", "majority");
    all.put("arcadedb.server.readinessRequiresHA", "true");
    all.putAll(settings);
    all.put("arcadedb.server.name", "ha-" + index);
    all.put("arcadedb.ha.serverList", serverList(nodes));
    return all;
  }

  static String serverList(final int nodes) {
    final StringBuilder list = new StringBuilder();
    for (int i = 0; i < nodes; i++) {
      if (i > 0)
        list.append(',');
      list.append("ha-").append(i).append(':').append(RAFT_PORT).append(':').append(HTTP_PORT);
    }
    return list.toString();
  }

  /**
   * The index of the first {@code /api/v1/cluster} answer reporting {@code isLeader} and {@code leaderReady} both true, -1 for
   * none. {@code isLeader} is the raw Raft role: a leader just elected has not applied its current-term entry yet, and a command
   * sent to it then fails with {@code LeaderNotReadyException}. An answer without {@code leaderReady} (an image older than the
   * field) is taken on {@code isLeader} alone. A null answer, or one that is no JSON object, is a member that said nothing.
   */
  static int leaderOf(final List<String> clusterAnswers) {
    for (int i = 0; i < clusterAnswers.size(); i++) {
      final String answer = clusterAnswers.get(i);
      if (answer == null)
        continue;
      final JSONObject json;
      try {
        json = new JSONObject(answer);
      } catch (final RuntimeException e) {
        continue;
      }
      if (json.getBoolean("isLeader", false) && json.getBoolean("leaderReady", true))
        return i;
    }
    return -1;
  }

  /** {@code null}, or {@code ha-<i>: <reason>} for the first member whose reason is not null. */
  static String deathReason(final List<String> memberReasons) {
    for (int i = 0; i < memberReasons.size(); i++)
      if (memberReasons.get(i) != null)
        return "ha-" + i + ": " + memberReasons.get(i);
    return null;
  }

  private static int waitForLeader(final List<Node> nodes) {
    final long deadline = System.nanoTime() + LEADER_WAIT.toNanos();
    while (true) {
      final int leader = leaderOf(answers(nodes));
      if (leader >= 0)
        return leader;
      if (System.nanoTime() >= deadline)
        return -1;
      try {
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
  }

  private static List<String> answers(final List<Node> nodes) {
    final List<String> answers = new ArrayList<>(nodes.size());
    for (final Node node : nodes)
      answers.add(node.clusterAnswer());
    return answers;
  }

  private static String clusterAnswerOf(final ServerUnderTest server) {
    try (final HttpClient client = HttpClient.newBuilder().connectTimeout(ANSWER_TIMEOUT).build()) {
      final String auth = Base64.getEncoder().encodeToString(("root:" + ServerUnderTest.PASSWORD).getBytes(StandardCharsets.UTF_8));
      final HttpRequest request = HttpRequest.newBuilder(URI.create(server.httpUrl() + "/api/v1/cluster"))
          .timeout(ANSWER_TIMEOUT)
          .header("Authorization", "Basic " + auth)
          .GET()
          .build();
      final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      return response.statusCode() == 200 ? response.body() : null;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (final IOException | RuntimeException e) {
      return null;
    }
  }

  public List<ServerUnderTest> members() {
    final List<ServerUnderTest> members = new ArrayList<>(nodes.size());
    for (final Node node : nodes)
      members.add(node.server());
    return members;
  }

  public int leaderIndex() {
    return leader;
  }

  /** Asks the members again who leads (after a failover) and remembers it; -1 when none answers as leader. */
  public int refreshLeader() {
    leader = leaderOf(answers(nodes));
    return leader;
  }

  /** Null while every member is alive, else {@code ha-<i>: <reason>} for the first dead one. */
  public String deathReason() {
    final List<String> reasons = new ArrayList<>(nodes.size());
    for (final Node node : nodes)
      reasons.add(node.deathReason());
    return deathReason(reasons);
  }

  public JSONObject config() {
    final JSONArray members = new JSONArray();
    for (int i = 0; i < cpusets.length; i++)
      members.put(new JSONObject().put("name", "ha-" + i).put("cpuset", cpusets[i]));
    return new JSONObject().put("image", image)
        .put("nodes", nodes.size())
        .put("members", members)
        .put("nodeMemGb", nodeMemGb)
        .put("heap", heap)
        .put("cpusets", new HashSet<>(List.of(cpusets)).size() == cpusets.length ? "split" : "shared");
  }

  /** Closes the members, then the network; never throws. */
  @Override
  public void close() {
    closeAll(nodes, network);
  }

  private static void closeAll(final List<Node> nodes, final AutoCloseable network) {
    for (final Node node : nodes)
      try {
        node.close();
      } catch (final Throwable e) {
        LOGGER.warn("Cluster member failed to close: {}", e.toString());
      }
    try {
      network.close();
    } catch (final Throwable e) {
      LOGGER.warn("Cluster network failed to close: {}", e.toString());
    }
  }
}
