package com.arcadedb.test.performance;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SingleServerSimpleLoadTestIT extends ContainersTestTemplate {

  @Test
  @DisplayName("Single server load test")
  void singleServerLoadTest() throws InterruptedException, IOException {

    GenericContainer<?> arcadeContainer = createArcadeContainer("arcade", "none", "none", "any", false, network);

    startContainers();

    DatabaseWrapper db = new DatabaseWrapper(arcadeContainer, idSupplier);
    db.createDatabase();
    db.createSchema();

    final int numOfThreads = 1;
    final int numOfUsers = 10000;
    final int numOfPhotos = 0;
    final int numOfFriendship = 0;

    int expectedUsersCount = numOfUsers * numOfThreads;
    int expectedFriendshipCount = numOfFriendship * numOfThreads;
    int expectedPhotoCount = expectedUsersCount * numOfPhotos;

    logger.info("Creating {} users using {} threads", expectedUsersCount, numOfThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
    for (int i = 0; i < numOfThreads; i++) {
      // Each thread will create users and photos
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(arcadeContainer, idSupplier);
        db1.addUserAndPhotos(numOfUsers, numOfPhotos);
        db1.close();
      });
    }

    executor.shutdown();

    while (!executor.isTerminated()) {
      long users = db.countUsers();
      long friendships = db.countFriendships();
      long photos = db.countPhotos();
      logger.info("Current users: {} - photos: {} - friendships: {}", users, photos, friendships);
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
