package com.arcadedb.resilience;

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
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public abstract class ResilienceTestTemplate {
  public static final String                 IMAGE      = "arcadedata/arcadedb:latest";
  public static final String                 PASSWORD   = "playwithdata";
  private             LoggingMeterRegistry   loggingMeterRegistry;
  private             AtomicInteger          id         = new AtomicInteger();
  protected           Logger                 logger     = LoggerFactory.getLogger(getClass());
  protected           Network                network;
  protected           ToxiproxyContainer     toxiproxy;
  protected           ToxiproxyClient        toxiproxyClient;
  protected           List<GenericContainer> containers = new ArrayList<>();

  protected Supplier<Integer> idSupplier = new Supplier<>() {

    @Override
    public Integer get() {
      return id.getAndIncrement();
    }
  };

  @BeforeEach
  void setUp() throws IOException, InterruptedException {
    logger.info("Cleaning up the target directory");
    FileUtils.deleteRecursively(Path.of("./target/databases").toFile());
    FileUtils.deleteRecursively(Path.of("./target/replication").toFile());
    FileUtils.deleteRecursively(Path.of("./target/logs").toFile());

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

    network = Network.newNetwork();

    logger.info("Creating a Toxiproxy container");
    toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0")
        .withNetwork(network)
        .withNetworkAliases("proxy");
    Startables.deepStart(toxiproxy).join();
    toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());

  }

  @AfterEach
  void tearDown() {
    stopContainers();

    logger.info("Stopping the Toxiproxy container");
    toxiproxy.stop();

    logger.info("Removing the target directory");
    FileUtils.deleteRecursively(Path.of("./target/databases").toFile());
    FileUtils.deleteRecursively(Path.of("./target/replication").toFile());
    FileUtils.deleteRecursively(Path.of("./target/logs").toFile());

    Metrics.removeRegistry(loggingMeterRegistry);
  }

  protected void stopContainers() {
    logger.info("Stopping all containers");
    containers.stream()
        .filter(ContainerState::isRunning)
        .peek(container -> logger.info("Stopping container {}", container.getContainerName()))
        .forEach(GenericContainer::stop);
    containers.clear();
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
  protected GenericContainer createArcadeContainer(String name,
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
  protected GenericContainer createArcadeContainer(String name,
      String serverList,
      String quorum,
      String role,
      boolean ha,
      Network network) {
    GenericContainer container = new GenericContainer(IMAGE)
        .withExposedPorts(2480, 5432)
        .withNetwork(network)
        .withNetworkAliases(name)
        .withStartupTimeout(Duration.ofSeconds(90))
        .withFileSystemBind("./target/databases/" + name, "/home/arcadedb/databases", BindMode.READ_WRITE)
        .withFileSystemBind("./target/replication/" + name, "/home/arcadedb/replication", BindMode.READ_WRITE)
        .withFileSystemBind("./target/logs/" + name, "/home/arcadedb/log", BindMode.READ_WRITE)
        .withEnv("JAVA_OPTS", String.format("""
            -Darcadedb.server.rootPassword=playwithdata
            -Darcadedb.server.plugins=Postgres:com.arcadedb.postgres.PostgresProtocolPlugin
            -Darcadedb.server.httpsIoThreads=10
            -Darcadedb.server.name=%s
            -Darcadedb.backup.enabled=false
            -Darcadedb.typeDefaultBuckets=10
            -Darcadedb.ha.enabled=%s
            -Darcadedb.ha.quorum=%s
            -Darcadedb.ha.serverRole=%s
            -Darcadedb.ha.serverList=%s
            -Darcadedb.ha.replicationQueueSize=1024
            """, name, ha, quorum, role, serverList))
        .waitingFor(Wait.forHttp("/api/v1/ready").forPort(2480).forStatusCode(204));
    containers.add(container);
    return container;
  }

}
