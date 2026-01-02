package com.arcadedb.containers.ha;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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

    logger.info("Creating the database on the first arcade container {} ", servers.getFirst().aliases());
    DatabaseWrapper db1 = new DatabaseWrapper(servers.getFirst(), idSupplier);
    logger.info("Creating the database on arcade server 1");
    db1.createDatabase();
    db1.createSchema();
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    logger.info("Checking that the database schema is replicated");
    db1.checkSchema();
    db2.checkSchema();

    IntStream.range(1, 10).forEach(
        x -> {
          logger.info("Adding data to database 1 iteration {}", x);
          db2.addUserAndPhotos(10, 10);
          db1.addUserAndPhotos(10, 10);

          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          logStatus(db1, db2);
        }

    );

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
    Long users1 = db1.countUsers();
    Long photos1 = db1.countPhotos();
    Long users2 = db2.countUsers();
    Long photos2 = db2.countPhotos();
    logger.info("Users:: {} --> {} - Photos:: {} --> {} ", users1, users2, photos1, photos2);
  }
}
