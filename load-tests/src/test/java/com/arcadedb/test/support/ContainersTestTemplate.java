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
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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

  /**
   * Compares all databases in the cluster to verify data consistency.
   * Opens databases from the target/databases directory and compares them pairwise.
   */
  protected void compareAllDatabases() {
    compareAllDatabases(DATABASE);
  }

  /**
   * Compares all databases in the cluster to verify data consistency.
   * Opens databases from the target/databases directory and compares them pairwise.
   *
   * @param databaseName The name of the database to compare.
   */
  protected void compareAllDatabases(String databaseName) {
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
      logger.info("Opening databases for comparison");

      // Open all databases in read-only mode
      for (File serverDir : serverDirs) {
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
        } catch (Exception e) {
          logger.error("Failed to open database at {}: {}", dbPath, e.getMessage());
        }
      }

      if (openDatabases.size() < 2) {
        logger.warn("Need at least 2 databases to compare, found {}", openDatabases.size());
        return;
      }

      logger.info("Comparing {} databases", openDatabases.size());
      final DatabaseComparator comparator = new DatabaseComparator();

      // Compare all pairs of databases
      for (int i = 0; i < openDatabases.size(); i++) {
        for (int j = i + 1; j < openDatabases.size(); j++) {
          final Database db1 = openDatabases.get(i);
          final Database db2 = openDatabases.get(j);
          logger.info("Comparing database {} with database {}", i + 1, j + 1);

          try {
            comparator.compare(db1, db2);
            logger.info("Databases {} and {} are identical ✓", i + 1, j + 1);
          } catch (DatabaseComparator.DatabaseAreNotIdentical e) {
            logger.error("Databases {} and {} are NOT identical: {}", i + 1, j + 1, e.getMessage());
            throw e;
          }
        }
      }

      logger.info("All databases are identical ✓");

    } finally {
      // Close all databases and factories
      for (Database db : openDatabases) {
        try {
          db.close();
        } catch (Exception e) {
          logger.error("Error closing database: {}", e.getMessage());
        }
      }

      for (DatabaseFactory factory : factories) {
        try {
          factory.close();
        } catch (Exception e) {
          logger.error("Error closing database factory: {}", e.getMessage());
        }
      }
    }
  }

  @AfterEach
  public void tearDown() {
    stopContainers();

    logger.info("Stopping the Toxiproxy container");
    toxiproxy.stop();

//    deleteContainersDirectories();

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
   * Checks the health status of all containers and logs diagnostics for any that have stopped.
   * Call this method when you suspect a container has died unexpectedly.
   */
  protected void diagnoseContainers() {
    for (GenericContainer<?> container : containers) {
      String name = container.getContainerName();
      boolean running = container.isRunning();

      if (!running) {
        logger.error("Container {} is NOT running!", name);

        try {
          // Get container inspection info
          var dockerClient = container.getDockerClient();
          var info = dockerClient.inspectContainerCmd(container.getContainerId()).exec();

          var state = info.getState();
          logger.error("Container {} state: Status={}, ExitCode={}, OOMKilled={}, Error={}",
              name,
              state.getStatus(),
              state.getExitCodeLong(),
              state.getOOMKilled(),
              state.getError());

          // Log the last lines of container logs
          String logs = container.getLogs();
          String[] logLines = logs.split("\n");
          int start = Math.max(0, logLines.length - 50);
          logger.error("Last 50 log lines for container {}:", name);
          for (int i = start; i < logLines.length; i++) {
            logger.error("  {}", logLines[i]);
          }

        } catch (Exception e) {
          logger.error("Failed to get diagnostics for container {}: {}", name, e.getMessage());
        }
      } else {
        logger.info("Container {} is running", name);
      }
    }
  }

  /**
   * Waits for a container to be healthy (running) with diagnostics on failure.
   *
   * @param container The container to check
   * @param timeoutSeconds Timeout in seconds
   * @return true if container is running, false otherwise
   */
  protected boolean waitForContainerHealthy(GenericContainer<?> container, int timeoutSeconds) {
    long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);

    while (System.currentTimeMillis() < deadline) {
      if (container.isRunning()) {
        return true;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    // Container not running - diagnose
    logger.error("Container {} failed health check after {}s", container.getContainerName(), timeoutSeconds);
    diagnoseContainers();
    return false;
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
   * Creates a new ArcadeDB container with the specified name and server list.
   *
   * @param name       The name of the container.
   * @param serverList The server list for HA configuration.
   * @param quorum     The quorum configuration for HA.
   * @param network    The network to attach the container to.
   *
   * @return A GenericContainer instance representing the ArcadeDB container.
   */
  protected GenericContainer createArcadeContainer(
      String name,
      String serverList,
      String quorum,
      String role,
      Network network) {
    return createArcadeContainer(name, serverList, quorum, role, true, network);
  }

  /**
   * Creates a new ArcadeDB container with the specified name and server list.
   *
   * @param name       The name of the container.
   * @param serverList The server list for HA configuration.
   * @param quorum     The quorum configuration for HA.
   * @param role       The role of the server (e.g., "leader", "follower").
   * @param ha         Whether to enable HA.
   * @param network    The network to attach the container to.
   *
   * @return A GenericContainer instance representing the ArcadeDB container.
   */
  protected GenericContainer<?> createArcadeContainer(String name,
      String serverList,
      String quorum,
      String role,
      boolean ha,
      Network network) {

    makeContainersDirectories(name);

    GenericContainer<?> container = new GenericContainer<>(IMAGE)
        .withExposedPorts(2480, 5432, 50051)
        .withNetwork(network)
        .withNetworkAliases(name)
        .withStartupTimeout(Duration.ofSeconds(90))
//        .withCopyToContainer(MountableFile.forHostPath("./target/databases/" + name, 0777), "/home/arcadedb/databases")
//        .withCopyToContainer(MountableFile.forHostPath("./target/replication/" + name, 0777), "/home/arcadedb/replication")
//        .withCopyToContainer(MountableFile.forHostPath("./target/logs/" + name, 0777), "/home/arcadedb/logs")

        .withFileSystemBind("./target/databases/" + name,  "/home/arcadedb/databases")
        .withFileSystemBind("./target/replication/" + name,  "/home/arcadedb/replication")
        .withFileSystemBind("./target/logs/" + name,  "/home/arcadedb/logs")

        .withEnv("JAVA_OPTS", String.format("""
            -Darcadedb.server.rootPassword=playwithdata
            -Darcadedb.server.plugins=PostgresProtocolPlugin,GrpcServerPlugin,PrometheusMetricsPlugin
            -Darcadedb.server.httpsIoThreads=30
            -Darcadedb.bucketReuseSpaceMode=low
            -Darcadedb.server.name=%s
            -Darcadedb.backup.enabled=false
            -Darcadedb.typeDefaultBuckets=10
            -Darcadedb.ha.enabled=%s
            -Darcadedb.ha.quorum=%s
            -Darcadedb.ha.serverRole=%s
            -Darcadedb.ha.serverList=%s
            -Darcadedb.ha.replicationQueueSize=1024
            """, name, ha, quorum, role, serverList))
        .withEnv("ARCADEDB_OPTS_MEMORY", "-Xms12G -Xmx12G")
        .waitingFor(Wait.forHttp("/api/v1/ready").forPort(2480).forStatusCode(204));
    containers.add(container);
    return container;
  }

}
