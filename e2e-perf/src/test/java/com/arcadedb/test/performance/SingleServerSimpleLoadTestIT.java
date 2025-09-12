package com.arcadedb.test.performance;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SingleServerSimpleLoadTestIT extends ContainersTestTemplate {

  @DisplayName("Single server load test")
  @ParameterizedTest
  @EnumSource(DatabaseWrapper.Protocol.class)
    //to eneable only one protocol use the following annotation
    //@EnumSource(value = DatabaseWrapper.Protocol.class, names = "GRPC")
  void singleServerLoadTest(DatabaseWrapper.Protocol protocol) throws InterruptedException, IOException {

    createArcadeContainer("arcade", "none", "none", "any", false, network);

    List<ServerWrapper> serverWrappers = startContainers();
    ServerWrapper server = serverWrappers.getFirst();
    DatabaseWrapper db = new DatabaseWrapper(server, idSupplier, protocol);
    db.createDatabase();
    db.createSchema();

    final int numOfThreads = 1;
    final int numOfUsers = 10000;
    final int numOfPhotos = 10;

    int expectedUsersCount = numOfUsers * numOfThreads;
    int expectedPhotoCount = expectedUsersCount * numOfPhotos;

    logger.info("Creating {} users using {} threads", expectedUsersCount, numOfThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
    for (int i = 0; i < numOfThreads; i++) {
      // Each thread will create users and photos
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(server, idSupplier, protocol);
        db1.addUserAndPhotos(numOfUsers, numOfPhotos);
        db1.close();
      });
    }

    executor.shutdown();

    while (!executor.isTerminated()) {
      long users = db.countUsers();
      long photos = db.countPhotos();
      logger.info("Current users: {} - photos: {} ", users, photos);
      // Wait for 2 seconds before checking again
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    db.assertThatUserCountIs(expectedUsersCount);
    db.assertThatPhotoCountIs(expectedPhotoCount);

  }

}
