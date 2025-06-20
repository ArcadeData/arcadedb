package com.arcadedb.containers.performance;

import com.arcadedb.containers.support.ContainersTestTemplate;
import com.arcadedb.containers.support.DatabaseWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class SingleServerLoadTestIT extends ContainersTestTemplate {

  @Test
  @DisplayName("Test single server under heavy load")
  void singleServerUnderMassiveLoad() throws InterruptedException, IOException {

    GenericContainer<?> arcadeContainer = createArcadeContainer("arcade", "none", "none", "any", false, network);

    startContainers();

    DatabaseWrapper db = new DatabaseWrapper(arcadeContainer, idSupplier);
    db.createDatabase();
    db.createSchema();

    final int numOfThreads = 5;
    final int numOfUsers = 1000;
    final int numOfFriendshipIterarion = 10;
    final int numOfFriendshipPerIteration = 10;

    int expectedUsersCount = numOfUsers * numOfThreads;
    int expectedFriendshipCount = numOfFriendshipIterarion * numOfFriendshipPerIteration * numOfThreads;

    logger.info("Creating {} users using {} threads", numOfUsers, numOfThreads);
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
        for (int f = 0; f < numOfFriendshipIterarion; f++) {
          List<Integer> userIds = db.getUserIds(numOfFriendshipPerIteration, f * 10);
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
      int users = db.countUsers();
      int friendships = db.countFriendships();
      logger.info("Current user count: {} - {}", users, friendships);
      // Wait for 2 seconds before checking again
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    db.assertThatUserCountIs(expectedUsersCount);
    db.assertThatFriendshipCountIs(expectedFriendshipCount);

  }
}
