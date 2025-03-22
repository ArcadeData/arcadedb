package com.arcadedb.e2e;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class BasicHaTest {
  private static final String IMAGE = "arcadedata/arcadedb:latest";
  private              int    id    = 0;

  @Test
  void simpleHaResyncScenarioAfterNetworkCrash() throws InterruptedException, IOException {

    Network network = Network.newNetwork();

    //create a Toxiproxy container
    ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.11.0")
        .withNetwork(network)
        .withNetworkAliases("proxy");
    Startables.deepStart(toxiproxy).join();
    final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
    //create a proxy for each arcade container
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("proxyOne", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("proxyTwo", "0.0.0.0:8667", "arcade2:2424");

    //create two arcade containers
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "proxy:8667", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "proxy:8666", network);

    //start the containers in sequence: arcade1 will be the leaser
    Startables.deepStart(arcade1).join();
    Startables.deepStart(arcade2).join();

    //create the database on the first arcade container
    RemoteServer server1 = new RemoteServer(arcade1.getHost(), arcade1.getMappedPort(2480), "root",
        "playwithdata");
    if (!server1.exists("ha-test"))
      server1.create("ha-test");

    //create database connections
    RemoteDatabase db1 = new RemoteDatabase(arcade1.getHost(), arcade1.getMappedPort(2480), "ha-test", "root",
        "playwithdata");
    RemoteDatabase db2 = new RemoteDatabase(arcade2.getHost(), arcade2.getMappedPort(2480), "ha-test", "root",
        "playwithdata");

    //create schema on database one
    createTypes(db1);

    //check if the database is replicated
    checkSchema(db1);
    checkSchema(db2);

    //add data to database one;
    addUserAndPhotos(db1, 10, 10);
    //query on database two
    ResultSet resultSet = db2.query("sql",
        """
            SELECT expand( out('HasUploaded') ) FROM User WHERE id = "u0000"
            """);
    assertThat(resultSet.stream()).hasSize(10);

    ResultSet resultSet1 = db2.query("sql", "SELECT count() as count FROM User");
    assertThat(resultSet1.next().<Integer>getProperty("count")).isEqualTo(10);

    //disconnect the two nodes
    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    //add to arcadw onw
    addUserAndPhotos(db1, 10, 10);
    // 20 on db1
    resultSet1 = db1.query("sql", "SELECT count() as count FROM User");
    assertThat(resultSet1.next().<Integer>getProperty("count")).isEqualTo(20);

    //still 10 on db2
    resultSet1 = db2.query("sql", "SELECT count() as count FROM User");
    assertThat(resultSet1.next().<Integer>getProperty("count")).isEqualTo(10);

    //reconnect nodes
    arcade1Proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
    arcade1Proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();

    //wait for resync
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).until(() -> {
      try {
        ResultSet resultSet2 = db2.query("sql", "SELECT count() as count FROM User");
        Integer count = resultSet2.next().<Integer>getProperty("count");
        return count.equals(20);
      } catch (Exception e) {
        return false;
      }
    });

  }

  private void checkSchema(RemoteDatabase db) {
    assertThat(db.getSchema().existsType("Photo")).isTrue();
    assertThat(db.getSchema().existsType("User")).isTrue();
    assertThat(db.getSchema().existsType("HasUploaded")).isTrue();
    assertThat(db.getSchema().existsType("IsFriendOf")).isTrue();
    assertThat(db.getSchema().existsType("Likes")).isTrue();
  }

  void createTypes(RemoteDatabase db) {
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
      String userId = String.format("u%04d", netxtId());
      db.command("sql", String.format("CREATE VERTEX User SET id = '%s'", userId));

      for (int photoIndex = 1; photoIndex <= numberOfPhotos; photoIndex++) {
        String photoId = String.format("p%04d", netxtId());
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

  int netxtId() {
    return id++;
  }

  /**
   * Creates a new ArcadeDB container with the specified name and server list.
   *
   * @param name       The name of the container.
   * @param serverList The server list for HA configuration.
   * @param network    The network to attach the container to.
   *
   * @return A GenericContainer instance representing the ArcadeDB container.
   */
  GenericContainer createArcadeContainer(String name, String serverList, Network network) {
    return new GenericContainer(IMAGE)
        .withExposedPorts(2480, 5432)
        .withNetwork(network)
        .withNetworkAliases(name)
        .withStartupTimeout(Duration.ofSeconds(90))
        .withEnv("JAVA_OPTS", String.format("""
            -Darcadedb.server.rootPassword=playwithdata
            -Darcadedb.server.plugins=Postgres:com.arcadedb.postgres.PostgresProtocolPlugin
            -Darcadedb.ha.enabled=true
            -Darcadedb.ha.quorum=none
            -Darcadedb.server.name=%s
            -Darcadedb.ha.serverList=%s
            """, name, serverList))
        .waitingFor(Wait.forHttp("/api/v1/ready").forPort(2480).forStatusCode(204));
  }
}
