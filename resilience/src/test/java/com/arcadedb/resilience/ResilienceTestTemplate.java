package com.arcadedb.resilience;

import com.arcadedb.remote.RemoteDatabase;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class ResilienceTestTemplate {
  public static final String IMAGE    = "arcadedata/arcadedb:latest";
  public static final String PASSWORD = "playwithdata";

  protected Logger             logger = LoggerFactory.getLogger(getClass());
  protected Network            network;
  protected ToxiproxyContainer toxiproxy;
  protected ToxiproxyClient    toxiproxyClient;

  protected Supplier<Integer> idSupplier = new Supplier<>() {
    private AtomicInteger id = new AtomicInteger();

    @Override
    public Integer get() {
      return id.getAndIncrement();
    }
  };

  @BeforeEach
  void setUp() {
    network = Network.newNetwork();

    logger.info("Creating a Toxiproxy container");
    toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.11.0")
        .withNetwork(network)
        .withNetworkAliases("proxy");
    Startables.deepStart(toxiproxy).join();
    toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());

  }

  protected void checkSchema(RemoteDatabase db) {
    assertThat(db.getSchema().existsType("Photo")).isTrue();
    assertThat(db.getSchema().existsType("User")).isTrue();
    assertThat(db.getSchema().existsType("HasUploaded")).isTrue();
    assertThat(db.getSchema().existsType("IsFriendOf")).isTrue();
    assertThat(db.getSchema().existsType("Likes")).isTrue();
  }

  void createSchema(RemoteDatabase db) {
    //this is a test-double of HTTPGraphIT.testOneEdgePerTx test
    db.command("sqlscript",
        """
            CREATE VERTEX TYPE User;
            CREATE PROPERTY User.id STRING;
            CREATE INDEX ON  User (id) UNIQUE;
            CREATE VERTEX TYPE Photo;
            CREATE PROPERTY Photo.id STRING;
            CREATE INDEX ON Photo (id) UNIQUE;
            CREATE EDGE TYPE HasUploaded;
            CREATE EDGE TYPE IsFriendOf;
            CREATE EDGE TYPE Likes;""");
  }

  void addUserAndPhotos(RemoteDatabase db, int numberOfUsers, int numberOfPhotos) {
    for (int userIndex = 1; userIndex <= numberOfUsers; userIndex++) {
      String userId = String.format("u%04d", idSupplier.get());
      db.command("sql", String.format("CREATE VERTEX User SET id = '%s'", userId));

      for (int photoIndex = 1; photoIndex <= numberOfPhotos; photoIndex++) {
        String photoId = String.format("p%04d", idSupplier.get());
        String photoName = String.format("download-%s.jpg", photoId);
        String sqlScript = String.format("""
            BEGIN;
            LET photo = CREATE VERTEX Photo SET id = '%s', name = '%s';
            LET user = SELECT FROM User WHERE id = '%s';
            LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo SET kind = 'User_Photos';
            COMMIT RETRY 30;
            RETURN $photo;""", photoId, photoName, userId);
        db.command("sqlscript", sqlScript);
      }
    }
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
  GenericContainer createArcadeContainer(String name, String serverList, String quorum, String role, Network network) {
    return new GenericContainer(IMAGE)
        .withExposedPorts(2480, 5432)
        .withNetwork(network)
        .withNetworkAliases(name)
        .withStartupTimeout(Duration.ofSeconds(90))
        .withEnv("JAVA_OPTS", String.format("""
            -Darcadedb.server.rootPassword=playwithdata
            -Darcadedb.server.plugins=Postgres:com.arcadedb.postgres.PostgresProtocolPlugin
            -Darcadedb.ha.enabled=true
            -Darcadedb.ha.quorum=%s
            -Darcadedb.server.name=%s
            -Darcadedb.ha.serverRole=%s
            -Darcadedb.ha.serverList=%s
            """, quorum, name, role, serverList))
        .waitingFor(Wait.forHttp("/api/v1/ready").forPort(2480).forStatusCode(204));
  }
}
