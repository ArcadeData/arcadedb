package com.arcadedb.containers.resilience;

import com.arcadedb.containers.support.ContainersTestTemplate;
import com.arcadedb.containers.support.DatabaseWrapper;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Testcontainers
public class SimpleHaScenarioIT extends ContainersTestTemplate {

  @Test
  @DisplayName("Test resync after network crash with 2 sewers in HA mode")
  void twoInstancesResyncAfterNetworkCrash() throws InterruptedException, IOException {

    logger.info("Creating a proxy for each arcade container");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");

    logger.info("Creating two arcade containers");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667", "none", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666", "none", "any", network);

    logger.info("Starting the containers in sequence: arcade1 will be the leader");
    startContainers();

    logger.info("Creating the database on the first arcade container");
    DatabaseWrapper db1 = new DatabaseWrapper(arcade1, idSupplier);
    logger.info("Creating the database on arcade server 1");
    db1.createDatabase();

    DatabaseWrapper db2 = new DatabaseWrapper(arcade2, idSupplier);
    logger.info("Creating schema on database 1");
    db1.createSchema();

    logger.info("Checking that the database schema is replicated");
    db1.checkSchema();
    db2.checkSchema();

    logger.info("Adding data to database 1");
    db1.addUserAndPhotos(10, 10);

    logger.info("Check that all the data are replicated on database 2");
    db2.assertThatUserCountIs(10);

    logger.info("Disconnecting the two instances");
    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Adding more data to arcade 1");
    db1.addUserAndPhotos(10, 1000);

    logger.info("Verifying 20 users on arcade 1");
    db1.assertThatUserCountIs(20);

    logger.info("Verifying still only 10 users on arcade 2");
    db2.assertThatUserCountIs(10);

    logger.info("Reconnecting instances");
    arcade1Proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
    arcade1Proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();

    logger.info("Waiting for resync");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Integer users1 = db1.countUsers();
            Integer photos1 = db1.countPhotos();
            Integer users2 = db2.countUsers();
            Integer photos2 = db2.countPhotos();
            logger.info("Users:: {} --> {} - Photos:: {} --> {} ", users1, users2, photos1, photos2);
            return users2.equals(users1) && photos2.equals(photos1);
          } catch (Exception e) {
            return false;
          }
        });
  }
}
