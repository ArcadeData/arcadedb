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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.BaseGraphServerTest;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4751: support the MongoDB {@code createIndexes} command (used by the
 * driver's {@code createIndex} / {@code createIndexes} helpers) in the mongodbw plugin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBCreateIndexTest extends BaseGraphServerTest {

  private static final int                       DEF_PORT = 27017;
  private              MongoClient               client;
  private              MongoCollection<Document> collection;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();

    getDatabase(0);

    client = new MongoClient(new ServerAddress("localhost", DEF_PORT));
    client.getDatabase(getDatabaseName()).createCollection("doc");
    collection = client.getDatabase(getDatabaseName()).getCollection("doc");

    for (int i = 0; i < 10; i++)
      collection.insertOne(new Document("test", i).append("name", "Jay" + i));
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  @Test
  void createSimpleIndex() {
    // This is exactly the call from issue #4751: db.doc.createIndex({test:1})
    collection.createIndex(new Document("test", 1));

    final Database database = getServerDatabase(0, getDatabaseName());
    final DocumentType type = database.getSchema().getType("doc");
    final TypeIndex index = type.getIndexByProperties("test");
    assertThat(index).isNotNull();
    assertThat(index.isUnique()).isFalse();
  }

  @Test
  void createUniqueIndex() {
    collection.createIndex(new Document("name", 1), new IndexOptions().unique(true));

    final Database database = getServerDatabase(0, getDatabaseName());
    final TypeIndex index = database.getSchema().getType("doc").getIndexByProperties("name");
    assertThat(index).isNotNull();
    assertThat(index.isUnique()).isTrue();
  }

  @Test
  void createCompoundIndex() {
    collection.createIndex(new Document("test", 1).append("name", -1));

    final Database database = getServerDatabase(0, getDatabaseName());
    final TypeIndex index = database.getSchema().getType("doc").getIndexByProperties("test", "name");
    assertThat(index).isNotNull();
  }

  @Test
  void createIndexIsIdempotent() {
    collection.createIndex(new Document("test", 1));
    // Re-creating the same index must not throw (MongoDB treats it as a no-op).
    collection.createIndex(new Document("test", 1));

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.getSchema().getType("doc").getIndexByProperties("test")).isNotNull();
  }

  @Test
  void createIndexOnNewCollection() {
    final MongoCollection<Document> fresh = client.getDatabase(getDatabaseName()).getCollection("freshCollection");
    fresh.createIndex(new Document("field", 1));

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.getSchema().existsType("freshCollection")).isTrue();
    assertThat(database.getSchema().getType("freshCollection").getIndexByProperties("field")).isNotNull();
  }
}
