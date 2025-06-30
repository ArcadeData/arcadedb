package com.arcadedb.test.performance;

import com.arcadedb.test.support.LocalhostDatabaseWrapper;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class SingleLocalhostServerSimpleLoadTestIT {
  protected LoggingMeterRegistry loggingMeterRegistry;
  protected Logger               logger = LoggerFactory.getLogger(getClass());

  protected Supplier<Integer> idSupplier = new Supplier<>() {

    private final AtomicInteger id = new AtomicInteger();

    @Override
    public Integer get() {
      return id.getAndIncrement();
    }
  };

  @BeforeEach
  void setUp() throws IOException, InterruptedException {

    // METRICS
    LoggingRegistryConfig config = new LoggingRegistryConfig() {
      @Override
      public String get(@NotNull String key) {
        return null;
      }

      @Override
      public @NotNull Duration step() {
        return Duration.ofSeconds(10);
      }
    };

    Metrics.addRegistry(new SimpleMeterRegistry());
    loggingMeterRegistry = LoggingMeterRegistry.builder(config).build();
    Metrics.addRegistry(loggingMeterRegistry);

  }

  @AfterEach
  public void tearDown() {

    Metrics.removeRegistry(loggingMeterRegistry);
  }

  @Test
  @DisplayName("Single server load test")
  void singleServerLoadTest() throws InterruptedException, IOException {

    LocalhostDatabaseWrapper db = new LocalhostDatabaseWrapper(idSupplier);
    db.createDatabase();
    db.createSchema();

    final int numOfThreads = 5;
    final int numOfUsers = 1000000;
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
        LocalhostDatabaseWrapper db1 = new LocalhostDatabaseWrapper(idSupplier);
        db1.addUserAndPhotos(numOfUsers, numOfPhotos);
        db1.close();
      });
    }

    executor.shutdown();

    while (!executor.isTerminated()) {
      try {
        long users = db.countUsers();
        long friendships = db.countFriendships();
        long photos = db.countPhotos();
        logger.info("Current users: {} - photos: {} - friendships: {}", users, photos, friendships);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
      try {
        // Wait for 2 seconds before checking again
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
