package com.arcadedb.containers.resilience;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Testcontainers
public class SimpleHaScenarioIT extends ContainersTestTemplate {

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test resync after network crash with 2 sewers in HA mode")
  void twoInstancesResyncAfterNetworkCrash() throws InterruptedException, IOException {


    logger.info("Creating two arcade containers");
    createArcadeContainer("arcade1", "{arcade2}arcade2:2424", "none", "any", network);
    createArcadeContainer("arcade2", "{arcade1}arcade1:2424", "none", "any", network);

    logger.info("Starting the containers in sequence: arcade1 will be the leader");
    List<ServerWrapper> servers = startContainers();

    logger.info("Creating the database on the first arcade container");
    DatabaseWrapper db1 = new DatabaseWrapper(servers.getFirst(), idSupplier);
    logger.info("Creating the database on arcade server 1");
    db1.createDatabase();
    db1.createSchema();
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    logger.info("Checking that the database schema is replicated");
    db1.checkSchema();
    db2.checkSchema();

    logger.info("Adding data to database 1");
    db1.addUserAndPhotos(10, 10);

    logger.info("Check that all the data are replicated on database 2");
    db2.assertThatUserCountIs(10);
    db2.assertThatPhotoCountIs(100);

    logger.info("Adding more data to arcade 1");
    db1.addUserAndPhotos(10, 1000);

    logger.info("Verifying 20 users on arcade 1");
    db1.assertThatUserCountIs(20);
    db1.assertThatPhotoCountIs(10100);

    logStatus(db1, db2);
    logger.info("Verifying 20 users on arcade 2");
    db2.assertThatUserCountIs(20);

    logger.info("Waiting for resync");

    // Wait for replication to complete
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          Long users1 = db1.countUsers();
          Long photos1 = db1.countPhotos();
          Long users2 = db2.countUsers();
          Long photos2 = db2.countPhotos();
          logger.info("Users:: {} --> {} - Photos:: {} --> {} ", users1, users2, photos1, photos2);
          return users2.equals(users1) && photos2.equals(photos1);
        });

  }

  private void logStatus(DatabaseWrapper db1, DatabaseWrapper db2) {
    logger.info("Maybe  resynced?");
    Long users1 = db1.countUsers();
    Long photos1 = db1.countPhotos();
    Long users2 = db2.countUsers();
    Long photos2 = db2.countPhotos();
    logger.info("Users:: {} --> {} - Photos:: {} --> {} ", users1, users2, photos1, photos2);
  }
}
