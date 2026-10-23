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
package com.arcadedb.mongo;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10562, through a real driver: the MongoDB protocol caches one database wrapper per name, and that wrapper used to keep
 * the instance it was built around for good. When the server replaced the instance behind the name - a {@code restore backup}
 * with {@code overwrite}, a {@code close database} followed by {@code open database}, a {@code drop database} followed by a
 * {@code create database} - every later MongoDB command failed with {@code DatabaseIsClosedException} until the server
 * restarted, while every other protocol served the new instance at once. The same client keeps working across each swap here.
 */
public class Issue10562MongoReplacedDatabaseTest extends BaseMongoServerTest {
  private static final String NAME       = "mongoswap10562";
  private static final String COLLECTION = "S";

  private MongoClient        client;
  private ServerControlPlane controlPlane;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    final ArcadeDBServer server = getServer(0);
    controlPlane = new ServerControlPlane(server);
    controlPlane.createDatabase(NAME);
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", "admin", DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    insert(5);
    assertThat(count()).isEqualTo(5);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    try {
      if (client != null)
        client.close();
      final ArcadeDBServer server = getServer(0);
      if (server.existsDatabase(NAME))
        server.dropLocalDatabase(server.getDatabase(NAME).getEmbedded(), NAME);
      FileUtils.deleteRecursively(
          new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
    } finally {
      super.endTest();
    }
  }

  @Test
  void aDatabaseClosedAndOpenedAgainIsServedAgain() {
    controlPlane.closeDatabase(NAME);
    controlPlane.openDatabase(NAME);

    assertThat(count()).as("the reopened instance, not the closed one").isEqualTo(5);
    insert(2);
    assertThat(count()).isEqualTo(7);
  }

  @Test
  void aDatabaseDroppedAndCreatedAgainIsServedAgain() {
    for (final int rows : new int[] { 7, 9 }) {
      controlPlane.dropDatabase(NAME);
      controlPlane.createDatabase(NAME);

      insert(rows);
      assertThat(count()).as("the database created again, holding only what was written to it").isEqualTo(rows);
    }
  }

  @Test
  void aDatabaseARestoreReplacedIsServedAgain() {
    // THE FILE NAME, RESOLVED UNDER THE SERVER'S BACKUP DIRECTORY
    final String backupFile = new File(controlPlane.triggerBackup(NAME).getString("backupFile")).getName();
    insert(2);
    assertThat(count()).isEqualTo(7);

    try {
      controlPlane.restoreBackup(NAME, backupFile, NAME, true, ServerControlPlane.ProgressListener.NOOP);

      assertThat(count()).as("the restored copy, taken before the last two inserts").isEqualTo(5);
      insert(1);
      assertThat(count()).isEqualTo(6);
    } finally {
      FileUtils.deleteRecursively(controlPlane.resolveBackupFile(NAME, backupFile).getParent().toFile());
    }
  }

  private MongoCollection<Document> collection() {
    return client.getDatabase(NAME).getCollection(COLLECTION);
  }

  private long count() {
    return collection().countDocuments();
  }

  private void insert(final int rows) {
    final List<Document> documents = new ArrayList<>(rows);
    for (int i = 0; i < rows; i++)
      documents.add(new Document("i", i));
    collection().insertMany(documents);
  }
}
