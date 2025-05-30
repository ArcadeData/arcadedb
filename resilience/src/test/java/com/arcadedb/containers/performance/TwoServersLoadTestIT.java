package com.arcadedb.containers.performance;

import com.arcadedb.containers.support.ContainersTestTemplate;
import com.arcadedb.containers.support.DatabaseWrapper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This test is designed to check the behavior of two servers under heavy load.
 * It creates two servers, adds data to one of them, and checks that the data is replicated to the other server.
 * It also checks that the schema is replicated correctly.
 */
public class TwoServersLoadTestIT extends ContainersTestTemplate {


  @Test
  @DisplayName("Load test 2 servers in HA mode")
  void twoServersMassiveInert() throws InterruptedException, IOException {

    logger.info("Creating two arcade containers");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}arcade2", "none", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}arcade1", "none", "any", network);

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

    final int numOfThreads = 5;
    final int numOfUsers = 1000;
    int numOfPhotos = 5;
    logger.info("Adding {} users with {} photos per user to database 1 using {} threads", numOfUsers, numOfPhotos, numOfThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);

    for (int i = 0; i < numOfThreads; i++) {
      executor.submit(() -> {
        DatabaseWrapper db = new DatabaseWrapper(arcade1, idSupplier);
        db.addUserAndPhotos(numOfUsers, numOfPhotos);
        db.close();
      });
      TimeUnit.SECONDS.sleep(1);
    }

    executor.shutdown();
    while (!executor.isTerminated()) {
      logger.info("Waiting for tasks to complete");
      Integer users2 = db2.countUsers();
      Integer photos2 = db2.countPhotos();
      Integer users1 = db1.countUsers();
      Integer photos1 = db1.countPhotos();
      logger.info("Users:: {} --> {} - Photos:: {} --> {} ", users1, users2, photos1, photos2);

      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    logger.info("Waiting for resync");
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .pollInterval(5, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Integer users2 = db2.countUsers();
            Integer photos2 = db2.countPhotos();
            Integer users1 = db1.countUsers();
            Integer photos1 = db1.countPhotos();

            logger.info("Users({}):: {} --> {} - Photos({}):: {} --> {} ", numOfThreads * numOfUsers,
                users1, users2,
                numOfThreads * numOfUsers * numOfPhotos, photos1, photos2);
            return users1.equals(numOfThreads * numOfUsers) &&
                photos1.equals(numOfThreads * numOfUsers * numOfPhotos) &&
                users2.equals(users1) &&
                photos2.equals(photos1);
          } catch (Exception e) {
            return false;
          }
        });


    db1.close();
    db2.close();
  }

}
