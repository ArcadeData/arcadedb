package com.arcadedb.resilience;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class SimpleHaScenarioIT extends ResilienceTestTemplate {

  @Test
  void twoInstancesResyncAfterNetworkCrash() throws InterruptedException, IOException {

    logger.info("Creating a proxy for each arcade container");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");

    logger.info("Creating two arcade containers");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "proxy:8667", "none", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "proxy:8666", "none", network);

    logger.info("Starting the containers in sequence: arcade1 will be the leader");
    Startables.deepStart(arcade1).join();
    Startables.deepStart(arcade2).join();

    logger.info("Creating the database on the first arcade container");
    RemoteServer server1 = new RemoteServer(arcade1.getHost(), arcade1.getMappedPort(2480), "root",
        "playwithdata");
    if (!server1.exists("ha-test"))
      server1.create("ha-test");

    RemoteDatabase db1 = new RemoteDatabase(arcade1.getHost(), arcade1.getMappedPort(2480), "ha-test", "root",
        "playwithdata");
    RemoteDatabase db2 = new RemoteDatabase(arcade2.getHost(), arcade2.getMappedPort(2480), "ha-test", "root",
        "playwithdata");

    logger.info("Creating schema on database 1");
    createSchema(db1);

    logger.info("Checking that the database schema is replicated");
    checkSchema(db1);
    checkSchema(db2);

    logger.info("Adding data to database 1");
    addUserAndPhotos(db1, 10, 10);

    logger.info("Check that all the data are replicated on database 2");
    ResultSet resultSet = db2.query("sql",
        """
            SELECT expand( out('HasUploaded') ) FROM User WHERE id = "u0000"
            """);
    assertThat(resultSet.stream()).hasSize(10);

    resultSet = db2.query("sql", "SELECT count() as count FROM User");
    assertThat(resultSet.next().<Integer>getProperty("count")).isEqualTo(10);

    logger.info("Disconnecting the two instances");
    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Adding more data to arcade 1");
    addUserAndPhotos(db1, 10, 1000);

    logger.info("Verifying 20 users on arcade 1");
    resultSet = db1.query("sql", "SELECT count() as count FROM User");
    assertThat(resultSet.next().<Integer>getProperty("count")).isEqualTo(20);

    logger.info("Verifying still only 10 users on arcade 2");
    resultSet = db2.query("sql", "SELECT count() as count FROM User");
    assertThat(resultSet.next().<Integer>getProperty("count")).isEqualTo(10);

    logger.info("Reconnecting instances");
    arcade1Proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
    arcade1Proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();

    logger.info("Waiting for resync");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          try {
            ResultSet resultSet1 = db1.query("sql", "SELECT count() as count FROM User");
            Integer users1 = resultSet1.next().<Integer>getProperty("count");
            ResultSet resultSet2 = db2.query("sql", "SELECT count() as count FROM User");
            Integer users2 = resultSet2.next().<Integer>getProperty("count");
            resultSet1 = db1.query("sql", "SELECT count() as count FROM Photo");
            Integer photos1 = resultSet1.next().<Integer>getProperty("count");
            resultSet2 = db2.query("sql", "SELECT count() as count FROM Photo");
            Integer photos2 = resultSet2.next().<Integer>getProperty("count");
            logger.info("Users:: {} --> {} - Photos:: {} --> {} ", users1, users2, photos1, photos2);
            return users2.equals(users1) && photos2.equals(photos1);
          } catch (Exception e) {
            return false;
          }
        });
  }
}
