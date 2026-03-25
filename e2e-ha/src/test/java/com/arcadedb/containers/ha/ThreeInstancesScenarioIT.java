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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

class ThreeInstancesScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "ArcadeDB_0:2434:2480,ArcadeDB_1:2434:2480,ArcadeDB_2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    stopContainers();
    logger.info("Comparing databases for consistency verification");
    compareAllDatabases();
    super.tearDown();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: replication across all nodes with consistency check")
  void threeNodeReplication() {
    createArcadeContainer("ArcadeDB_0", SERVER_LIST, "majority", network);
    createArcadeContainer("ArcadeDB_1", SERVER_LIST, "majority", network);
    createArcadeContainer("ArcadeDB_2", SERVER_LIST, "majority", network);

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

    logger.info("Adding data from each node");
    db1.addUserAndPhotos(10, 10);
    db2.addUserAndPhotos(10, 10);
    db3.addUserAndPhotos(10, 10);

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
            return users1 == 30 && users2 == 30 && users3 == 30
                && photos1 == 300 && photos2 == 300 && photos3 == 300;
          } catch (final Exception e) {
            return false;
          }
        });

    logger.info("Adding more data from node 2");
    db2.addUserAndPhotos(100, 10);

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
}
