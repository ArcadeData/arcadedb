/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.test.performance;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SingleServerLoadTestIT extends ContainersTestTemplate {

  @DisplayName("Single server load test")
  @ParameterizedTest
  @EnumSource(DatabaseWrapper.Protocol.class)
  void singleServerLoadTest(DatabaseWrapper.Protocol protocol) throws InterruptedException, IOException {

    createArcadeContainer("arcade", "none", "none", "any", false, network);
    ServerWrapper server = startContainers().get(0);
    DatabaseWrapper db = new DatabaseWrapper(server, idSupplier, protocol);
    db.createDatabase();
    db.createSchema();

    // Parameters for the test
    final int numOfThreads = 5; //number of threads to use to insert users and photos
    final int numOfUsers = 10000; // Each thread will create 200000 users
    final int numOfPhotos = 10; // Each user will have 5 photos
    final int numOfFriendship = 1000; // Each thread will create 100000 friendships
    final int numOfLike = 1000; // Each thread will create 100000 likes

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
        DatabaseWrapper db1 = new DatabaseWrapper(server, idSupplier, protocol);
        db1.addUserAndPhotos(numOfUsers, numOfPhotos);
        db1.close();
      });
    }

    if (numOfFriendship > 0) {
      // Each thread will create friendships
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(server, idSupplier, protocol);
        db1.createFriendships(numOfFriendship);
        db1.close();
      });
    }

    if (numOfLike > 0) {
      // Each thread will create friendships
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(server, idSupplier, protocol);
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
