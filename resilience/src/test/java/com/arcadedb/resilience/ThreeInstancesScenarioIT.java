package com.arcadedb.resilience;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;

public class ThreeInstancesScenarioIT extends ResilienceTestTemplate {

  @Test
  @DisplayName("Test with 3 instances: 1 leader and 2 replicas")
  void oneLeaderAndTwoReplicas() throws IOException, InterruptedException {

    logger.info("Creating a proxy for each arcade container");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3 arcade containers");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "proxy:8667,proxy:8668", "majority", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "proxy:8666,proxy:8668", "majority", "replica", network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "proxy:8666,proxy:8667", "majority", "replica", network);

    logger.info("Starting the containers in sequence: arcade1 will be the leader");
    Startables.deepStart(arcade1).join();
    Startables.deepStart(arcade2).join();
    Startables.deepStart(arcade3).join();

    DatabaseWrapper db1 = new DatabaseWrapper(arcade1, idSupplier);
    logger.info("Creating the database on arcade server 1");
    db1.createDatabase();

    DatabaseWrapper db2 = new DatabaseWrapper(arcade2, idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(arcade3, idSupplier);

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
    db1.addUserAndPhotos(10, 10);

    logger.info("Check that all the data are replicated on each instance");
    db1.assertThatUserCountIs(40);

//    TimeUnit.SECONDS.sleep(180);
    logger.info("Still 30 users on arcade1");
    db2.assertThatUserCountIs(30);

    db3.assertThatUserCountIs(40);

  }

}
