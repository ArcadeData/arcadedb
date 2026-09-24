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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.containers.ha.chaos.ClusterState.NodeState;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.ServerWrapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Statistics;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Long-running chaos test: a seeded sequence of process, network and freeze faults against one Raft cluster under
 * continuous write load, with every acknowledged write checked after every step. Configured by {@code chaos.*}
 * system properties (see {@link ChaosConfig}); excluded from the nightly HA job and run by ha-chaos-tests.yml.
 * Every node's Raft and forwarding traffic goes through Toxiproxy so any node can be degraded at any step.
 */
@Tag("chaos")
class HaChaosIT extends ContainersTestTemplate {
  private static final int    RAFT_PROXY_BASE = 8660;
  private static final int    HTTP_PROXY_BASE = 8670;
  private static final String NODE_HEAP       = "-Xms1G -Xmx1G";
  private static final long   NODE_MEMORY     = 2048L * 1024 * 1024;

  @Override
  protected boolean useToxiproxy() {
    return true;
  }

  @Test
  @DisplayName("Seeded chaos run: randomized faults under write load, ledger-checked after every step")
  void chaos() throws Exception {
    final ChaosConfig config = ChaosConfig.fromProperties(System.getProperties());
    logger.info("CHAOS seed={} nodes={} duration={} faults={} | replay: {}", config.seed(), config.nodes(),
        config.duration(), config.faultsSpec(), config.replayCommand());

    final List<Proxy> raftProxies = new ArrayList<>();
    final StringBuilder serverList = new StringBuilder();
    for (int i = 0; i < config.nodes(); i++) {
      raftProxies.add(toxiproxyClient.createProxy("raftProxy" + i, "0.0.0.0:" + (RAFT_PROXY_BASE + i), "arcadedb-" + i + ":2434"));
      toxiproxyClient.createProxy("httpProxy" + i, "0.0.0.0:" + (HTTP_PROXY_BASE + i), "arcadedb-" + i + ":2480");
      if (i > 0)
        serverList.append(',');
      serverList.append("proxy:").append(RAFT_PROXY_BASE + i).append(':').append(HTTP_PROXY_BASE + i);
    }
    final List<GenericContainer<?>> nodes = new ArrayList<>();
    for (int i = 0; i < config.nodes(); i++)
      nodes.add(createPersistentArcadeContainer("arcadedb-" + i, serverList.toString(), "majority", network, NODE_HEAP,
          NODE_MEMORY));

    final List<ServerWrapper> servers = startCluster();
    final int leader = waitForRaftLeader(servers, 120);
    assertThat(leader).as("initial leader election").isGreaterThanOrEqualTo(0);
    waitForAllNodesKnowLeader(servers, 60);

    final DockerNodeControl control = new DockerNodeControl(nodes, raftProxies);
    createDatabaseAndSchema(servers.get(leader), control);

    final Ledger ledger = new Ledger(config.writers());
    final InvariantChecker checker = new InvariantChecker(ledger);
    final Path reportDir = Path.of("target", "chaos", Long.toString(config.seed()));
    ChaosResult result;
    try (final ChaosReport report = new ChaosReport(reportDir, config);
        final Workload workload = new Workload(config, ledger, control, ChaosSchema.DATABASE)) {
      final Checkpoint checkpoint = new Checkpoint(new HttpNodeReader(control, ChaosSchema.DATABASE), ledger, checker,
          config.nodes(), config.convergenceTimeout(), Duration.ofSeconds(1));
      final ChaosRunner runner = new ChaosRunner(config, new ClusterState(config.nodes()), control,
          new FaultPicker(config.faultWeights(), config.electionTimeout()), workload, ledger, checkpoint, checker, report,
          control, duration -> Thread.sleep(duration.toMillis()));
      result = runner.run();

      // the evidence must be what the servers flushed on a clean shutdown, not a SIGKILL snapshot
      stopNodesGracefully(nodes);
      dumpContainerLogs("chaos-" + config.seed());
      if (result.kind() == ResultKind.PASS)
        try {
          compareAllDatabases(ChaosSchema.DATABASE);
        } catch (final Throwable t) {
          final Violation violation = new Violation(ResultKind.SAFETY, "PAGE_COMPARE", String.valueOf(t.getMessage()),
              new long[0]);
          result = new ChaosResult(ResultKind.SAFETY, violation.describe(), result.steps(), List.of(violation));
          report.summary(result, ledger, checker.lateCommits());
        }
      if (result.kind() != ResultKind.PASS)
        for (int i = 0; i < config.nodes(); i++)
          copyDirectory(Path.of("target", "databases", "arcadedb-" + i),
              reportDir.resolve("databases").resolve("arcadedb-" + i));
      for (final GenericContainer<?> node : nodes)
        node.stop();
    }
    assertThat(result.kind())
        .as(result.message() + " | report: " + reportDir.toAbsolutePath() + " | replay: " + config.replayCommand())
        .isEqualTo(ResultKind.PASS);
  }

  /**
   * Unpauses a paused node, then stops every running node with SIGTERM and a 30 s grace, so each server runs its
   * shutdown and closes its files. {@link #stopContainers()} is not used here: it SIGKILLs. A failure is logged and never
   * changes the run's result.
   */
  private void stopNodesGracefully(final List<GenericContainer<?>> nodes) {
    final DockerClient docker = DockerClientFactory.instance().client();
    for (final GenericContainer<?> node : nodes) {
      final String id = node.getContainerId();
      try {
        final InspectContainerResponse.ContainerState state = docker.inspectContainerCmd(id).exec().getState();
        if (Boolean.TRUE.equals(state.getPaused()))
          docker.unpauseContainerCmd(id).exec();
        if (Boolean.TRUE.equals(state.getRunning())) {
          logger.info("Stopping container {} gracefully", node.getContainerName());
          docker.stopContainerCmd(id).withTimeout(30).exec();
        }
      } catch (final Exception e) {
        logger.warn("Could not stop container {} gracefully: {}", node.getContainerName(), e.getMessage());
      }
    }
  }

  private void createDatabaseAndSchema(final ServerWrapper leader, final DockerNodeControl control) {
    final RemoteServer server = new RemoteServer(leader.host(), leader.httpPort(), "root", PASSWORD);
    server.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    Awaitility.await("database creation").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2))
        .ignoreExceptions().until(() -> {
          if (!server.exists(ChaosSchema.DATABASE))
            server.create(ChaosSchema.DATABASE);
          return true;
        });

    final RemoteDatabase database = new RemoteDatabase(leader.host(), leader.httpPort(), ChaosSchema.DATABASE, "root",
        PASSWORD);
    database.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    try {
      for (final String ddl : ChaosSchema.DDL)
        database.command("sql", ddl);
    } finally {
      database.close();
    }

    final HttpNodeReader reader = new HttpNodeReader(control, ChaosSchema.DATABASE);
    for (int i = 0; i < control.size(); i++) {
      final int node = i;
      Awaitility.await("schema on node " + node).atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1))
          .ignoreExceptions().until(() -> reader.counts(node)[0] == 0);
    }
  }

  /**
   * Keeps the database files of a failed run: the template's teardown deletes {@code target/databases}, which is the
   * only copy of the state the failure is about. A failed copy is logged and never changes the run's result.
   */
  private void copyDirectory(final Path from, final Path to) {
    if (!Files.isDirectory(from)) {
      logger.warn("Cannot preserve {}: not a directory", from);
      return;
    }
    try (final Stream<Path> paths = Files.walk(from)) {
      paths.forEach(source -> {
        final Path target = to.resolve(from.relativize(source).toString());
        try {
          if (Files.isDirectory(source))
            Files.createDirectories(target);
          else
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException e) {
          logger.warn("Could not preserve {}: {}", source, e.getMessage());
        }
      });
      logger.info("Preserved database files of the failed run in {}", to.toAbsolutePath());
    } catch (final IOException | UncheckedIOException e) {
      logger.warn("Could not preserve {} in {}: {}", from, to, e.getMessage());
    }
  }

  static long directorySize(final Path dir) {
    if (!Files.isDirectory(dir))
      return -1;
    try (final Stream<Path> files = Files.walk(dir)) {
      return files.filter(Files::isRegularFile).mapToLong(file -> {
        try {
          return Files.size(file);
        } catch (final IOException e) {
          return 0;
        }
      }).sum();
    } catch (final IOException | UncheckedIOException e) {
      return -1;
    }
  }

  /**
   * Docker and Toxiproxy backed {@link NodeControl}. Containers are killed, stopped, paused and restarted in place
   * through the Docker API, so their bind-mounted data survives; the host port of a restarted container is re-read
   * because Docker may assign a new one.
   */
  private final class DockerNodeControl implements NodeControl, Endpoints, ChaosRunner.TrendSource {
    private static final Duration HEALTH_TIMEOUT = Duration.ofSeconds(120);

    private final List<GenericContainer<?>> nodes;
    private final List<Proxy>               raftProxies;
    private final Endpoint[]                endpoints;

    DockerNodeControl(final List<GenericContainer<?>> nodes, final List<Proxy> raftProxies) {
      this.nodes = nodes;
      this.raftProxies = raftProxies;
      this.endpoints = new Endpoint[nodes.size()];
      for (int i = 0; i < nodes.size(); i++)
        refreshEndpoint(i);
    }

    private DockerClient docker() {
      return DockerClientFactory.instance().client();
    }

    private String id(final int node) {
      return nodes.get(node).getContainerId();
    }

    @Override
    public int size() {
      return nodes.size();
    }

    @Override
    public synchronized Endpoint endpoint(final int node) {
      return endpoints[node];
    }

    private synchronized void refreshEndpoint(final int node) {
      final InspectContainerResponse inspect = docker().inspectContainerCmd(id(node)).exec();
      if (!Boolean.TRUE.equals(inspect.getState().getRunning()))
        throw new ChaosFailure(ResultKind.AVAILABILITY, describeExit(node, inspect.getState(), "exited right after start"));
      final Ports.Binding[] bindings = inspect.getNetworkSettings().getPorts().getBindings().get(ExposedPort.tcp(2480));
      if (bindings == null || bindings.length == 0)
        throw new ChaosFailure(ResultKind.HARNESS, "Node " + node + " has no host binding for port 2480");
      endpoints[node] = new Endpoint(nodes.get(node).getHost(), Integer.parseInt(bindings[0].getHostPortSpec()));
    }

    @Override
    public String unexpectedExit(final ClusterState state) {
      for (int i = 0; i < size(); i++) {
        final NodeState believed = state.state(i);
        if (believed != NodeState.UP && believed != NodeState.DEGRADED)
          continue;
        final InspectContainerResponse.ContainerState container = docker().inspectContainerCmd(id(i)).exec().getState();
        if (!Boolean.TRUE.equals(container.getRunning()))
          return describeExit(i, container, "exited unexpectedly");
      }
      return null;
    }

    private static String describeExit(final int node, final InspectContainerResponse.ContainerState container,
        final String what) {
      return "node " + node + " " + what + ": exitCode=" + container.getExitCodeLong() + " OOMKilled="
          + container.getOOMKilled();
    }

    @Override
    public void kill(final int node) {
      docker().killContainerCmd(id(node)).exec();
    }

    @Override
    public void stopGracefully(final int node) {
      docker().stopContainerCmd(id(node)).withTimeout(30).exec();
    }

    @Override
    public void start(final int node) throws InterruptedException {
      docker().startContainerCmd(id(node)).exec();
      refreshEndpoint(node);
      awaitHealthy(node);
    }

    private void awaitHealthy(final int node) throws InterruptedException {
      final long deadline = System.nanoTime() + HEALTH_TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        final Endpoint endpoint = endpoint(node);
        try {
          final HttpURLConnection connection = (HttpURLConnection) URI.create(
              "http://" + endpoint.host() + ":" + endpoint.port() + "/api/v1/health").toURL().openConnection();
          connection.setConnectTimeout(2_000);
          connection.setReadTimeout(2_000);
          try {
            if (connection.getResponseCode() == 204)
              return;
          } finally {
            connection.disconnect();
          }
        } catch (final IOException e) {
          // still starting
        }
        Thread.sleep(1_000);
      }
      throw new ChaosFailure(ResultKind.AVAILABILITY, "Node " + node + " not healthy within " + HEALTH_TIMEOUT + " after restart");
    }

    @Override
    public void pause(final int node) {
      docker().pauseContainerCmd(id(node)).exec();
    }

    @Override
    public void unpause(final int node) {
      docker().unpauseContainerCmd(id(node)).exec();
    }

    @Override
    public void disconnect(final int node) {
      disconnectFromNetwork(nodes.get(node));
    }

    @Override
    public void reconnect(final int node) {
      reconnectToNetwork(nodes.get(node));
      // On Linux, reconnecting a container to its only network publishes it on a NEW ephemeral host port
      refreshEndpoint(node);
    }

    @Override
    public void addLatency(final int node, final int latencyMs, final int jitterMs) throws IOException {
      raftProxies.get(node).toxics().latency("chaos-latency", ToxicDirection.DOWNSTREAM, latencyMs).setJitter(jitterMs);
    }

    @Override
    public void addLoss(final int node, final float toxicity) throws IOException {
      raftProxies.get(node).toxics().limitData("chaos-loss", ToxicDirection.DOWNSTREAM, 0).setToxicity(toxicity);
    }

    @Override
    public void clearToxics(final int node) throws IOException {
      for (final Toxic toxic : raftProxies.get(node).toxics().getAll())
        toxic.remove();
    }

    @Override
    public int findLeader() {
      return findLeaderIndex(servers());
    }

    @Override
    public boolean awaitLeader(final Duration timeout) {
      final List<ServerWrapper> servers = servers();
      if (waitForRaftLeader(servers, (int) timeout.toSeconds()) < 0)
        return false;
      // A node that never learns the leader (e.g. it crashed) must fail here, so the runner's crash check reports it
      return allNodesKnowLeader(servers, (int) timeout.toSeconds());
    }

    @Override
    public String leaderView() {
      final StringBuilder view = new StringBuilder();
      for (int i = 0; i < size(); i++) {
        if (i > 0)
          view.append("; ");
        final Endpoint endpoint = endpoint(i);
        view.append("node ").append(i).append(": ");
        try {
          final HttpURLConnection connection = (HttpURLConnection) URI.create(
              "http://" + endpoint.host() + ":" + endpoint.port() + "/api/v1/cluster").toURL().openConnection();
          connection.setRequestProperty("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
          connection.setConnectTimeout(2_000);
          connection.setReadTimeout(2_000);
          try {
            final int status = connection.getResponseCode();
            view.append("HTTP ").append(status);
            if (status == 200) {
              final JSONObject json = new JSONObject(
                  new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
              view.append(" isLeader=").append(json.getBoolean("isLeader", false))
                  .append(" leader=").append(json.isNull("leaderHttpAddress") ? "none" : json.getString("leaderHttpAddress"));
            }
          } finally {
            connection.disconnect();
          }
        } catch (final Exception e) {
          view.append("unreachable on ").append(endpoint.host()).append(':').append(endpoint.port()).append(" (")
              .append(e.getClass().getSimpleName()).append(": ").append(e.getMessage()).append(')');
        }
      }
      return view.toString();
    }

    private List<ServerWrapper> servers() {
      final List<ServerWrapper> servers = new ArrayList<>(size());
      for (int i = 0; i < size(); i++) {
        final Endpoint endpoint = endpoint(i);
        servers.add(new ServerWrapper(endpoint.host(), endpoint.port(), 0));
      }
      return servers;
    }

    @Override
    public TrendRow sample(final int step, final double acksPerSecond, final long checkpointMs) {
      final int n = size();
      final long[] memory = new long[n];
      final long[] databases = new long[n];
      final long[] replication = new long[n];
      for (int i = 0; i < n; i++) {
        memory[i] = memoryUsage(i);
        databases[i] = directorySize(Path.of("target", "databases", "arcadedb-" + i, ChaosSchema.DATABASE));
        replication[i] = directorySize(Path.of("target", "databases", "arcadedb-" + i, ".raft-storage"));
      }
      return new TrendRow(step, memory, databases, replication, acksPerSecond, checkpointMs);
    }

    private long memoryUsage(final int node) {
      final AtomicLong usage = new AtomicLong(-1);
      try (final ResultCallback.Adapter<Statistics> callback = new ResultCallback.Adapter<>() {
        @Override
        public void onNext(final Statistics statistics) {
          if (statistics.getMemoryStats() != null && statistics.getMemoryStats().getUsage() != null)
            usage.set(statistics.getMemoryStats().getUsage());
        }
      }) {
        docker().statsCmd(id(node)).withNoStream(true).exec(callback).awaitCompletion(10, TimeUnit.SECONDS);
      } catch (final Exception e) {
        logger.warn("Could not read the memory usage of node {}: {}", node, e.getMessage());
      }
      return usage.get();
    }
  }
}
