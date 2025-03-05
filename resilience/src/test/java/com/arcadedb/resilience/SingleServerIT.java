package com.arcadedb.resilience;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SingleServerIT extends ResilienceTestTemplate {

  @Test
  @DisplayName("Test single server under heavy load")
  void singleServerUnderMassiveLoad() throws InterruptedException, IOException {

    GenericContainer arcadeContainer = createArcadeContainer("arcade", "none", "none", "any", false, network);

    Startables.deepStart(arcadeContainer).join();

    DatabaseWrapper db = new DatabaseWrapper(arcadeContainer, idSupplier);
    db.createDatabase();
    db.createSchema();

    final int numOfThreads = 5;
    final int numOfUsers = 1000;

    ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
    for (int i = 0; i < numOfThreads; i++) {
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(arcadeContainer, idSupplier);
        db1.addUserAndPhotos(numOfUsers, 0);
        db1.close();
      });

      TimeUnit.SECONDS.sleep(1);

      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(arcadeContainer, idSupplier);
        for (int f = 0; f < 10; f++) {
          List<String> userIds = db.getUserIds(10, f * 10);
          for (int j = 0; j < userIds.size(); j++) {
            db1.addFriendship(userIds.get(j), userIds.get((j + 1) % userIds.size()));
          }
          logger.info("Added {} friendships", userIds.size() * f);
        }
        db1.close();
      });
    }

    executor.shutdown();
    while (!executor.isTerminated()) {
      int userCount = db.countUsers();
      int friendships = db.countFriendships();
      logger.info("Current user count: {} - {}", userCount, friendships);
      // Wait for 2 seconds before checking again
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    db.assertThatUserCountIs(numOfUsers * numOfThreads);

    Assertions.assertThat(db.countFriendships()).isEqualTo(500);

  }
}
