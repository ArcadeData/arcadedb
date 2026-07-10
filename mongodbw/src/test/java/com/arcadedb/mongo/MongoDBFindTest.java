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
import com.arcadedb.database.Database;
import com.arcadedb.server.BaseGraphServerTest;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4746 / #4745: the MongoDB plugin returned a single document on an unfiltered
 * {@code find()}, did not see collections created after the first connection, and stopped returning data after a
 * client reconnected.
 */
public class MongoDBFindTest extends BaseGraphServerTest {

  private static final int DEF_PORT = 27017;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void findWithoutFilterReturnsAllDocuments() {
    getDatabase(0);
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT))) {
      client.getDatabase(getDatabaseName()).createCollection("Numbers");
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("Numbers");
      for (int i = 0; i < 10; i++)
        collection.insertOne(new Document("value", i));

      final List<Document> all = new ArrayList<>();
      collection.find().into(all);

      assertThat(all).hasSize(10);
    }
  }

  @Test
  void collectionCreatedAfterConnectionIsVisible() {
    getDatabase(0);
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT))) {
      // Force the database wrapper to be created (and its collection cache built) before the new type exists.
      client.getDatabase(getDatabaseName()).createCollection("PreExisting");
      client.getDatabase(getDatabaseName()).getCollection("PreExisting").countDocuments();

      // Create a brand-new type out-of-band, as Studio or a SQL command would.
      final Database db = getServerDatabase(0, getDatabaseName());
      db.getSchema().createDocumentType("LateType");
      db.transaction(() -> db.newDocument("LateType").set("value", 42).save());

      final List<Document> all = new ArrayList<>();
      client.getDatabase(getDatabaseName()).getCollection("LateType").find().into(all);

      assertThat(all).hasSize(1);
      assertThat(all.getFirst().getInteger("value")).isEqualTo(42);
    }
  }

  @Test
  void dataStillReturnedAfterClientReconnect() {
    getDatabase(0);

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT))) {
      client.getDatabase(getDatabaseName()).createCollection("Reconnect");
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("Reconnect");
      for (int i = 0; i < 5; i++)
        collection.insertOne(new Document("value", i));

      final List<Document> all = new ArrayList<>();
      collection.find().into(all);
      assertThat(all).hasSize(5);
    }

    // Second, independent connection: data must still be visible.
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT))) {
      final List<Document> all = new ArrayList<>();
      client.getDatabase(getDatabaseName()).getCollection("Reconnect").find().into(all);
      assertThat(all).hasSize(5);
    }
  }
}
