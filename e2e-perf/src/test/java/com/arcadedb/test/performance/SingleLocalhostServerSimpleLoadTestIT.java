package com.arcadedb.test.performance;

import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * How to run this test locally
 * 1 - build the package: mvn clea install -DskipTests
 * 2 - run the server :
 * export ARCADEDB_OPTS_MEMORY="-Xms16G -Xmx16G" && \
 * ./package/target/arcadedb-25.6.1-SNAPSHOT.dir/arcadedb-25.6.1-SNAPSHOT/bin/server.sh \
 * -Darcadedb.server.rootPassword=playwithdata \
 * -Darcadedb.serverMetrics=true \
 * -Darcadedb.typeDefaultBuckets=10 -Darcadedb.bucketReuseSpaceMode=low
 * 3 - when the server is up and running, remove the Disabled annotation and run the test
 * <p>
 * The test uses a fixed size thread pool to execute operations in parallel.
 */
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
  @Disabled
  @DisplayName("Single server load test")
  void singleServerLoadTest() throws InterruptedException, IOException {

    ServerWrapper server = new ServerWrapper("localhost", 2480, 50051);
    DatabaseWrapper db = new DatabaseWrapper(server, idSupplier);
    db.createDatabase();
    db.createSchema();

    // Parameters for the test
    final int numOfThreads = 5; //number of threads to use to insert users and photos
    final int numOfUsers = 200000; // Each thread will create 200000 users
    final int numOfPhotos = 10; // Each user will have 5 photos
    final int numOfFriendship = 100000; // Each thread will create 100000 friendships
    final int numOfLike = 100000; // Each thread will create 100000 likes

    int expectedUsersCount = numOfUsers * numOfThreads;
    int expectedPhotoCount = expectedUsersCount * numOfPhotos;
    int expectedFriendshipCount = numOfFriendship;
    int expectedLikeCount = numOfLike;
    LocalDateTime startedAt = LocalDateTime.now();
    logger.info("Creating {} users using {} threads", expectedUsersCount, numOfThreads);
    logger.info("Expected users: {} - photos: {} - friendships: {} - likes: {}", expectedUsersCount, expectedPhotoCount,
        expectedFriendshipCount, expectedLikeCount);
    logger.info("Starting at {}", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(startedAt));

    ExecutorService executor = Executors.newFixedThreadPool(10);
    for (int i = 0; i < numOfThreads; i++) {
      // Each thread will create users and photos
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(server, idSupplier);
        db1.addUserAndPhotos(numOfUsers, numOfPhotos);
        db1.close();
      });
    }

    if (numOfFriendship > 0) {
      // Each thread will create friendships
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(server, idSupplier);
        db1.createFriendships(numOfFriendship);
        db1.close();
      });
    }

    if (numOfLike > 0) {
      // Each thread will create friendships
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(server, idSupplier);
        ;
        db1.createLike(numOfLike);
        db1.close();
      });
    }

    executor.shutdown();

    while (!executor.isTerminated()) {
      try {
        long users = db.countUsers();
        long friendships = db.countFriendships();
        long photos = db.countPhotos();
        long likes = db.countLikes();
        logger.info("Current users: {} - photos: {} - friendships: {} - likes: {}", users, photos, friendships, likes);
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

    LocalDateTime finishedAt = LocalDateTime.now();
    logger.info("Finishing at {}", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(finishedAt));
    logger.info("Total time: {} minutes", Duration.between(startedAt, finishedAt).toMinutes());

    Metrics.globalRegistry.getMeters().forEach(meter -> {
      logger.info("Meter: {} - {}", meter.getId().getName(), meter.measure());
    });

    db.assertThatUserCountIs(expectedUsersCount);
    db.assertThatPhotoCountIs(expectedPhotoCount);
    db.assertThatFriendshipCountIs(expectedFriendshipCount);
    db.assertThatLikesCountIs(expectedLikeCount);

  }

}
