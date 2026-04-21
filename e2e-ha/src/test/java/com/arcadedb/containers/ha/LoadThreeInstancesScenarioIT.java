/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.containers.ha;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import io.micrometer.core.instrument.Metrics;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class LoadThreeInstancesScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    // Skip compareAllDatabases(): with non-persistent containers, database files are not
    // on the host after stop. The test body already verifies convergence via Awaitility.
    super.tearDown();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: replication across all nodes with consistency check")
  void threeNodeReplication() {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting all containers");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    logger.info("Creating database and schema");
    db1.createDatabase();
    db1.createSchema();

    logger.info("Checking schema replicated to all nodes");
    db1.checkSchema();
    db2.checkSchema();
    db3.checkSchema();

    logger.info("Adding data to node 1");
    db1.addUserAndPhotos(500, 10);
    logger.info("Adding data to node 2");
    db2.addUserAndPhotos(500, 10);
    logger.info("Adding data to node 3");
    db3.addUserAndPhotos(500, 10);

    logger.info("Verifying replication across all nodes");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users1 = db1.countUsers();
            final long users2 = db2.countUsers();
            final long users3 = db3.countUsers();
            final long photos1 = db1.countPhotos();
            final long photos2 = db2.countPhotos();
            final long photos3 = db3.countPhotos();
            logger.info("Users: {} / {} / {} | Photos: {} / {} / {}", users1, users2, users3, photos1, photos2, photos3);
            return users1 == users2 && users2 == users3 && photos1 == photos2 && photos2 == photos3;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Adding more data from node 2");
    db2.addUserAndPhotos(500, 10);

    logger.info("Waiting for full convergence");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users1 = db1.countUsers();
            final long photos1 = db1.countPhotos();
            final long users2 = db2.countUsers();
            final long photos2 = db2.countPhotos();
            final long users3 = db3.countUsers();
            final long photos3 = db3.countPhotos();
            logger.info("Users: {} / {} / {} | Photos: {} / {} / {}", users1, users2, users3, photos1, photos2, photos3);
            return users1 == users2 && users2 == users3 && photos1 == photos2 && photos2 == photos3;
          } catch (final Exception e) {
            return false;
          }
        });

    db1.close();
    db2.close();
    db3.close();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: replication across all nodes with consistency check")
  void threeNodeReplicationMulti() throws Exception {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting all containers");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    logger.info("Creating database and schema");
    db1.createDatabase();
    db1.createSchema();

    logger.info("Checking schema replicated to all nodes");
    db1.checkSchema();
    db2.checkSchema();
    db3.checkSchema();

    // Parameters for the test
    final int numOfThreads = 1; //number of threads to use to insert users and photos
    final int numOfUsers = 500; // Each thread will create 200000 users
    final int numOfPhotos = 10; // Each user will have 5 photos
    final int numOfFriendship = 100; // Each thread will create 100000 friendships
    final int numOfLike = 100; // Each thread will create 100000 likes

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
        DatabaseWrapper dbn = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
        dbn.addUserAndPhotos(numOfUsers, numOfPhotos);
        dbn.close();
      });
    }

    TimeUnit.SECONDS.sleep(10);
    if (numOfFriendship > 0) {
      // Each thread will create friendships
      executor.submit(() -> {
        DatabaseWrapper dbn = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
        dbn.createFriendships(numOfFriendship);
        dbn.close();
      });
    }

    if (numOfLike > 0) {
      // Each thread will create friendships
      executor.submit(() -> {
        DatabaseWrapper dbn = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
        dbn.createLike(numOfLike);
        dbn.close();
      });
    }

    executor.shutdown();

    while (!executor.isTerminated()) {
      try {
        final long users1 = db1.countUsers();
        final long users2 = db2.countUsers();
        final long users3 = db3.countUsers();
        final long photos1 = db1.countPhotos();
        final long photos2 = db2.countPhotos();
        final long photos3 = db3.countPhotos();
        logger.info("Users: {} / {} / {} | Photos: {} / {} / {}", users1, users2, users3, photos1, photos2, photos3);
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

    logger.info("Waiting for full convergence");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
          try {
            final long users1 = db1.countUsers();
            final long photos1 = db1.countPhotos();
            final long users2 = db2.countUsers();
            final long photos2 = db2.countPhotos();
            final long users3 = db3.countUsers();
            final long photos3 = db3.countPhotos();
            logger.info("Users: {} / {} / {} | Photos: {} / {} / {}", users1, users2, users3, photos1, photos2, photos3);
            return users1 == users2 && users2 == users3 && photos1 == photos2 && photos2 == photos3;
          } catch (final Exception e) {
            return false;
          }
        });

    LocalDateTime finishedAt = LocalDateTime.now();
    logger.info("Finishing at {}", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(finishedAt));
    logger.info("Total time: {} minutes", Duration.between(startedAt, finishedAt).toMinutes());

    Metrics.globalRegistry.getMeters().forEach(meter -> {
      logger.info("Meter: {} - {}", meter.getId().getName(), meter.measure());
    });

    db1.close();
    db2.close();
    db3.close();
  }
}
