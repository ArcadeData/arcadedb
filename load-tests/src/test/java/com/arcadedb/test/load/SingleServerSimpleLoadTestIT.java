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
package com.arcadedb.test.load;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class SingleServerSimpleLoadTestIT extends ContainersTestTemplate {

  @DisplayName("Single server load test")
  @ParameterizedTest(name = "Load test with {0} protocol")
  @EnumSource(DatabaseWrapper.Protocol.class)
    //to eneable only one protocol use the following annotation
    //@EnumSource(value = DatabaseWrapper.Protocol.class, names = "GRPC")
  void singleServerLoadTest(DatabaseWrapper.Protocol protocol) throws Exception {

    createArcadeContainer("arcade", "none", "none", "any", false, network);

    List<ServerWrapper> serverWrappers = startContainers();
    ServerWrapper server = serverWrappers.getFirst();
    DatabaseWrapper db = new DatabaseWrapper(server, idSupplier, wordSupplier, protocol);
    db.createDatabase();
    db.createSchema();

    final int numOfThreads = 1;
    final int numOfUsers = 1000;
    final int numOfPhotos = 10;

    int expectedUsersCount = numOfUsers * numOfThreads;
    int expectedPhotoCount = expectedUsersCount * numOfPhotos;

    logger.info("Starting load test on protocol {}", protocol.name());
    logger.info("Creating {} users using {} threads", expectedUsersCount, numOfThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
    for (int i = 0; i < numOfThreads; i++) {
      // Each thread will create users and photos
      executor.submit(() -> {
        DatabaseWrapper db1 = new DatabaseWrapper(server, idSupplier, wordSupplier, protocol);
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
