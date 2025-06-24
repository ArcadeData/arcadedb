package com.arcadedb.containers.performance;

import com.arcadedb.containers.support.ContainersTestTemplate;
import com.arcadedb.containers.support.DatabaseWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SingleServerLoadTestIT extends ContainersTestTemplate {

  @Test
  @DisplayName("Single server load test")
  void singleServerLoadTest() throws InterruptedException, IOException {

    GenericContainer<?> arcadeContainer = createArcadeContainer("arcade", "none", "none", "any", false, network);

    startContainers();

    DatabaseWrapper db = new DatabaseWrapper(arcadeContainer, idSupplier);
    db.createDatabase();
    db.createSchema();

    final int numOfThreads = 5;
    final int numOfUsers = 1000;
    final int numOfPhotos = 5;
    final int numOfFriendshipIterations = 10;
    final int numOfFriendshipPerIterations = 10;

    int expectedUsersCount = numOfUsers * numOfThreads;
    int expectedFriendshipCount = numOfFriendshipIterations * numOfFriendshipPerIterations * numOfThreads;
    int expectedPhotoCount = expectedUsersCount * numOfPhotos;

    logger.info("Creating {} users using {} threads", numOfUsers, numOfThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
    for (int i = 0; i < numOfThreads; i++) {
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(arcadeContainer, idSupplier);
        db1.addUserAndPhotos(numOfUsers, numOfPhotos);
        db1.close();
      });

      TimeUnit.SECONDS.sleep(1);

      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(arcadeContainer, idSupplier);
        db1.createFriendships(numOfFriendshipIterations, numOfFriendshipPerIterations);
        db1.close();
      });
    }

    executor.shutdown();
    while (!executor.isTerminated()) {
      long users = db.countUsers();
      long friendships = db.countFriendships();
      logger.info("Current user count: {} - {}", users, friendships);
      // Wait for 2 seconds before checking again
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    db.assertThatUserCountIs(expectedUsersCount);
    db.assertThatPhotoCountIs(expectedPhotoCount);
    db.assertThatFriendshipCountIs(expectedFriendshipCount);

  }

}
