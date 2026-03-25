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
package com.arcadedb.test.support;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.utility.FileUtils;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public abstract class ContainersTestTemplate {
  public static final String                    IMAGE      = "arcadedata/arcadedb:latest";
  public static final String                    PASSWORD   = "playwithdata";
  public static final String                    DATABASE   = "playwithpictures";
  protected           LoggingMeterRegistry      loggingMeterRegistry;
  protected           Logger                    logger     = LoggerFactory.getLogger(getClass());
  protected           Network                   network;
  protected           ToxiproxyContainer        toxiproxy;
  protected           ToxiproxyClient           toxiproxyClient;
  protected           List<GenericContainer<?>> containers = new ArrayList<>();

  /**
   * Supplier to generate unique IDs.
   */
  protected Supplier<Integer> idSupplier = new Supplier<>() {

    private final AtomicInteger id = new AtomicInteger();

    @Override
    public Integer get() {
      return id.getAndIncrement();
    }
  };

  protected Supplier<String> wordSupplier = new Supplier<>() {
    {

      try {
        words = Arrays.stream(
                FileUtils.readStreamAsString(ContainersTestTemplate.class.getResourceAsStream("/english_words.txt"), "UTF-8")
                    .split("\n"))
            .toList();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private final List<String> words;

    private final Random random = new Random(words.size());

    @Override
    public String get() {
      return words.get(random.nextInt(words.size()));
    }
  };

  @BeforeEach
  void setUp() throws IOException, InterruptedException {
    deleteContainersDirectories();

    // METRICS
    LoggingRegistryConfig config = new LoggingRegistryConfig() {
      @Override
      public String get(String key) {
        return null;
      }

      @Override
      public Duration step() {
        return Duration.ofSeconds(10);
      }
    };

    Metrics.addRegistry(new SimpleMeterRegistry());
    loggingMeterRegistry = LoggingMeterRegistry.builder(config).build();
    Metrics.addRegistry(loggingMeterRegistry);

    // NETWORK
    network = Network.newNetwork();

    // Toxiproxy
    logger.info("Creating a Toxiproxy container");
    toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0")
        .withNetwork(network)
        .withNetworkAliases("proxy");
    Startables.deepStart(toxiproxy).join();
    toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());

  }

  @AfterEach
  public void tearDown() {
    stopContainers();

    logger.info("Stopping the Toxiproxy container");
    toxiproxy.stop();

    deleteContainersDirectories();

    Metrics.removeRegistry(loggingMeterRegistry);
  }

  private void deleteContainersDirectories() {
    logger.info("Deleting containers directories");
    FileUtils.deleteRecursively(Path.of("./target/databases").toFile());
    FileUtils.deleteRecursively(Path.of("./target/replication").toFile());
    FileUtils.deleteRecursively(Path.of("./target/logs").toFile());
  }

  private void makeContainersDirectories(String name) {
    logger.info("Creating containers directories");
    Path.of("./target/databases/" + name).toFile().mkdirs();
    Path.of("./target/databases/" + name).toFile().setWritable(true, false);
    Path.of("./target/replication/" + name).toFile().mkdirs();
    Path.of("./target/replication/" + name).toFile().setWritable(true, false);
    Path.of("./target/logs/" + name).toFile().mkdirs();
    Path.of("./target/logs/" + name).toFile().setWritable(true, false);
  }

  /**
   * Disconnects a container from the Docker network, fully isolating it from other containers.
   * Unlike Toxiproxy, this creates a true symmetric network partition.
   */
  protected void disconnectFromNetwork(final GenericContainer<?> container) {
    final String containerId = container.getContainerId();
    final String networkId = network.getId();
    logger.info("Disconnecting container {} from network {}", container.getContainerName(), networkId);
    container.getDockerClient().disconnectFromNetworkCmd()
        .withContainerId(containerId)
        .withNetworkId(networkId)
        .exec();
  }

  /**
   * Reconnects a container to the Docker network after a partition.
   */
  protected void reconnectToNetwork(final GenericContainer<?> container) {
    final String containerId = container.getContainerId();
    final String networkId = network.getId();
    logger.info("Reconnecting container {} to network {}", container.getContainerName(), networkId);
    container.getDockerClient().connectToNetworkCmd()
        .withContainerId(containerId)
        .withNetworkId(networkId)
        .exec();
  }

  /**
   * Stops all containers and clears the list of containers.
   */
  protected void stopContainers() {
    logger.info("Stopping all containers");
    containers.stream()
        .filter(ContainerState::isRunning)
        .peek(container -> logger.info("Stopping container {}", container.getContainerName()))
        .forEach(GenericContainer::stop);
    containers.clear();
  }

  /**
   * Starts all containers that are not already running.
   */
  protected List<ServerWrapper> startContainers() {
    logger.info("Starting all containers");
    containers.stream()
        .filter(container -> !container.isRunning())
        .forEach(container -> Startables.deepStart(container).join());
    return containers.stream()
        .map(ServerWrapper::new)
        .toList();
  }

  /**
   * Starts all containers and waits for the Raft cluster to elect a leader.
   * Use this instead of {@link #startContainers()} for HA clusters.
   */
  protected List<ServerWrapper> startCluster() {
    final List<ServerWrapper> servers = startContainers();
    waitForRaftLeader(servers, 60);
    return servers;
  }

  /**
   * Waits for a Raft leader to be elected by polling {@code /api/v1/cluster} on all servers.
   * Call this after {@link #startContainers()} before issuing any write operations.
   *
   * @param servers        the server wrappers returned by {@link #startContainers()}
   * @param timeoutSeconds maximum time to wait for leader election
   * @return the index of the leader server, or -1 if no leader was elected within the timeout
   */
  protected int waitForRaftLeader(final List<ServerWrapper> servers, final int timeoutSeconds) {
    final long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < servers.size(); i++) {
        try {
          final HttpURLConnection conn = (HttpURLConnection) URI.create(
              "http://" + servers.get(i).host() + ":" + servers.get(i).httpPort() + "/api/v1/cluster").toURL().openConnection();
          conn.setRequestProperty("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + PASSWORD).getBytes()));
          conn.setConnectTimeout(2000);
          conn.setReadTimeout(2000);
          try {
            if (conn.getResponseCode() == 200) {
              final String body = new String(conn.getInputStream().readAllBytes());
              if (body.contains("\"isLeader\":true")) {
                logger.info("Raft leader elected on node {} after {}ms",
                    i, System.currentTimeMillis() - (deadline - timeoutSeconds * 1000L));
                return i;
              }
            }
          } finally {
            conn.disconnect();
          }
        } catch (final Exception e) {
          // Node not ready yet, keep polling
        }
      }
      try {
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    logger.error("No Raft leader elected within {}s", timeoutSeconds);
    diagnoseContainers();
    return -1;
  }

  /**
   * Creates a standalone ArcadeDB container without HA.
   */
  protected GenericContainer<?> createArcadeContainer(String name, Network network) {
    makeContainersDirectories(name);

    GenericContainer<?> container = new GenericContainer<>(IMAGE)
        .withExposedPorts(2480, 5432, 50051)
        .withNetwork(network)
        .withNetworkAliases(name)
        .withStartupTimeout(Duration.ofSeconds(90))
        .withCopyToContainer(MountableFile.forHostPath("./target/databases/" + name, 0777), "/home/arcadedb/databases")
        .withCopyToContainer(MountableFile.forHostPath("./target/logs/" + name, 0777), "/home/arcadedb/logs")
        .withEnv("JAVA_OPTS", String.format("""
            -Darcadedb.server.rootPassword=playwithdata
            -Darcadedb.server.plugins=GrpcServerPlugin,PrometheusMetricsPlugin
            -Darcadedb.server.name=%s
            -Darcadedb.backup.enabled=false
            -Darcadedb.typeDefaultBuckets=10
            """, name))
        .withEnv("ARCADEDB_OPTS_MEMORY", "-Xms2G -Xmx2G")
        .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(3L * 1024 * 1024 * 1024))
        .waitingFor(Wait.forHttp("/api/v1/ready").forPort(2480).forStatusCode(204));
    containers.add(container);
    return container;
  }

  /**
   * Creates a new ArcadeDB container with Raft HA enabled.
   *
   * @param name       The container name (also used as server name and network alias).
   * @param serverList The Raft server list in format: host:raftPort:httpPort,host:raftPort:httpPort,...
   * @param quorum     The quorum configuration (majority, none, all).
   * @param network    The network to attach the container to.
   *
   * @return A GenericContainer instance representing the ArcadeDB container.
   */
  protected GenericContainer<?> createArcadeContainer(
      String name,
      String serverList,
      String quorum,
      Network network) {

    makeContainersDirectories(name);

    GenericContainer<?> container = new GenericContainer<>(IMAGE)
        .withExposedPorts(2480, 2434, 5432, 50051)
        .withNetwork(network)
        .withNetworkAliases(name)
        .withStartupTimeout(Duration.ofSeconds(90))
        .withCopyToContainer(MountableFile.forHostPath("./target/databases/" + name, 0777), "/home/arcadedb/databases")
        .withCopyToContainer(MountableFile.forHostPath("./target/replication/" + name, 0777), "/home/arcadedb/replication")
        .withCopyToContainer(MountableFile.forHostPath("./target/logs/" + name, 0777), "/home/arcadedb/logs")

        .withEnv("JAVA_OPTS", String.format("""
            -Darcadedb.server.rootPassword=playwithdata
            -Darcadedb.server.plugins=GrpcServerPlugin,PrometheusMetricsPlugin
            -Darcadedb.server.name=%s
            -Darcadedb.backup.enabled=false
            -Darcadedb.typeDefaultBuckets=10
            -Darcadedb.ha.enabled=true
            -Darcadedb.ha.implementation=raft
            -Darcadedb.ha.quorum=%s
            -Darcadedb.ha.raft.port=2434
            -Darcadedb.ha.serverList=%s
            """, name, quorum, serverList))
        .withEnv("ARCADEDB_OPTS_MEMORY", "-Xms2G -Xmx2G")
        .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(3L * 1024 * 1024 * 1024))
        .waitingFor(Wait.forHttp("/api/v1/ready").forPort(2480).forStatusCode(204));
    containers.add(container);
    return container;
  }

  /**
   * Compares all databases in the cluster to verify data consistency.
   * Opens databases from the target/databases directory and compares them pairwise.
   */
  protected void compareAllDatabases() {
    compareAllDatabases(DATABASE);
  }

  protected void compareAllDatabases(final String databaseName) {
    final File databasesDir = Path.of("./target/databases").toFile();
    if (!databasesDir.exists() || !databasesDir.isDirectory()) {
      logger.warn("Cannot compare databases: directory ./target/databases does not exist");
      return;
    }

    final File[] serverDirs = databasesDir.listFiles(File::isDirectory);
    if (serverDirs == null || serverDirs.length < 2) {
      logger.warn("Cannot compare databases: need at least 2 server directories, found {}",
          serverDirs == null ? 0 : serverDirs.length);
      return;
    }

    final List<Database> openDatabases = new ArrayList<>();
    final List<DatabaseFactory> factories = new ArrayList<>();

    try {
      for (final File serverDir : serverDirs) {
        final String dbPath = serverDir.getAbsolutePath() + "/" + databaseName;
        final File dbDir = new File(dbPath);
        if (!dbDir.exists()) {
          logger.warn("Database directory does not exist: {}", dbPath);
          continue;
        }

        final DatabaseFactory factory = new DatabaseFactory(dbPath);
        factories.add(factory);
        try {
          final Database db = factory.open(ComponentFile.MODE.READ_ONLY);
          openDatabases.add(db);
          logger.info("Opened database: {} (server: {})", databaseName, serverDir.getName());
        } catch (final Exception e) {
          logger.error("Failed to open database at {}: {}", dbPath, e.getMessage());
        }
      }

      if (openDatabases.size() < 2) {
        logger.warn("Need at least 2 databases to compare, found {}", openDatabases.size());
        return;
      }

      final DatabaseComparator comparator = new DatabaseComparator();
      for (int i = 0; i < openDatabases.size(); i++) {
        for (int j = i + 1; j < openDatabases.size(); j++) {
          comparator.compare(openDatabases.get(i), openDatabases.get(j));
          logger.info("Databases {} and {} are identical", i + 1, j + 1);
        }
      }
    } finally {
      for (final Database db : openDatabases) {
        try {
          db.close();
        } catch (final Exception e) {
          logger.error("Error closing database: {}", e.getMessage());
        }
      }
      for (final DatabaseFactory factory : factories) {
        try {
          factory.close();
        } catch (final Exception e) {
          logger.error("Error closing database factory: {}", e.getMessage());
        }
      }
    }
  }

  /**
   * Checks the health status of all containers and logs diagnostics for any that have stopped.
   */
  protected void diagnoseContainers() {
    for (final GenericContainer<?> container : containers) {
      final String name = container.getContainerName();
      final boolean running = container.isRunning();

      if (!running) {
        logger.error("Container {} is NOT running!", name);
        try {
          final var dockerClient = container.getDockerClient();
          final var info = dockerClient.inspectContainerCmd(container.getContainerId()).exec();
          final var state = info.getState();
          logger.error("Container {} state: Status={}, ExitCode={}, OOMKilled={}, Error={}",
              name, state.getStatus(), state.getExitCodeLong(), state.getOOMKilled(), state.getError());

          final String logs = container.getLogs();
          final String[] logLines = logs.split("\n");
          final int start = Math.max(0, logLines.length - 50);
          logger.error("Last 50 log lines for container {}:", name);
          for (int i = start; i < logLines.length; i++)
            logger.error("  {}", logLines[i]);
        } catch (final Exception e) {
          logger.error("Failed to get diagnostics for container {}: {}", name, e.getMessage());
        }
      } else {
        logger.info("Container {} is running", name);
      }
    }
  }

  /**
   * Waits for a container to be healthy (running) with diagnostics on failure.
   */
  protected boolean waitForContainerHealthy(final GenericContainer<?> container, final int timeoutSeconds) {
    final long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);
    while (System.currentTimeMillis() < deadline) {
      if (container.isRunning())
        return true;
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    logger.error("Container {} failed health check after {}s", container.getContainerName(), timeoutSeconds);
    diagnoseContainers();
    return false;
  }

}
