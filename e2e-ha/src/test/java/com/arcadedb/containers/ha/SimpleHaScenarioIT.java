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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Testcontainers
class SimpleHaScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "ArcadeDB_0:2434:2480,ArcadeDB_1:2434:2480";

  @Test
  @DisplayName("Two-node Raft HA: schema and data replication")
  void twoNodeRaftReplication() throws InterruptedException {
    createArcadeContainer("ArcadeDB_0", SERVER_LIST, "none", network);
    createArcadeContainer("ArcadeDB_1", SERVER_LIST, "none", network);

    logger.info("Starting the containers");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db1 = new DatabaseWrapper(servers.getFirst(), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);

    logger.info("Creating database and schema on server 1");
    db1.createDatabase();
    db1.createSchema();

    logger.info("Checking schema is replicated to server 2");
    db1.checkSchema();
    db2.checkSchema();

    logger.info("Adding data to server 1");
    db1.addUserAndPhotos(10, 10);

    logger.info("Verifying data replicated to server 2");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          final long users1 = db1.countUsers();
          final long users2 = db2.countUsers();
          final long photos1 = db1.countPhotos();
          final long photos2 = db2.countPhotos();
          logger.info("Users: {} -> {} | Photos: {} -> {}", users1, users2, photos1, photos2);
          return users2 == users1 && photos2 == photos1;
        });

    db1.assertThatUserCountIs(10);
    db2.assertThatUserCountIs(10);
    db1.assertThatPhotoCountIs(100);
    db2.assertThatPhotoCountIs(100);

    db1.close();
    db2.close();
  }
}
