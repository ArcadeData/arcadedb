package com.arcadedb.resilience;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ThreeInstancesScenarioIT extends ResilienceTestTemplate {

  @Test
  @DisplayName("Test with 3 instances: 1 leader and 2 replicas")
  void oneLeaderAndTwoReplicas() throws IOException, InterruptedException {

    logger.info("Creating a proxy for each arcade container");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3 arcade containers");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any",
        network);

    logger.info("Starting the containers in sequence: arcade1 will be the leader");
    Startables.deepStart(arcade1).join();
    Startables.deepStart(arcade2).join();
    Startables.deepStart(arcade3).join();

    DatabaseWrapper db2 = new DatabaseWrapper(arcade2, idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(arcade3, idSupplier);
    DatabaseWrapper db1 = new DatabaseWrapper(arcade1, idSupplier);
    logger.info("Creating the database on arcade server 1");
    db1.createDatabase();

    logger.info("Creating schema on database 1");
    db1.createSchema();

    logger.info("Checking that the database schema is replicated");
    db1.checkSchema();
    db2.checkSchema();
    db3.checkSchema();

    logger.info("Adding data to databases");
    db1.addUserAndPhotos(10, 10);
    db2.addUserAndPhotos(10, 10);
    db3.addUserAndPhotos(10, 10);

    logger.info("Check that all the data are replicated on each instance");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);
    db3.assertThatUserCountIs(30);

    logger.info("Disconnecting arcade1 form others");
    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Adding data to database");

    assertThatThrownBy(() -> {
      db1.addUserAndPhotos(10, 10);
    }).hasMessageContaining("Timeout waiting for quorum");

    logger.info("Reconnecting arcade1 ");
    arcade1Proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
    arcade1Proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();


    logger.info("Adding data to database");
    db1.addUserAndPhotos(10, 10);

    logger.info("Check that all the data are replicated on each instance");
    db1.assertThatUserCountIs(40);
    db2.assertThatUserCountIs(40);
    db3.assertThatUserCountIs(40);

//    logger.info("Still 30 users on arcade3");


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
            Integer users3 = db3.countUsers();
            Integer photos3 = db3.countPhotos();
            logger.info("Users:: {} --> {} --> {} - Photos:: {} --> {} --> {}  ", users1, users2, users3, photos1, photos2,
                photos3);
            return users2.equals(users1) && photos2.equals(photos1) && users3.equals(users1) && photos3.equals(photos1);
          } catch (Exception e) {
            return false;
          }
        });
  }

}
