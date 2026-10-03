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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #8931: in production mode the MongoDB wire protocol answered a duplicated-key failure with
 * the raw exception text, key values included, which the HTTP and gRPC surfaces replace with a placeholder. The error
 * code stays, because it is the bounded part a driver acts on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8931MongoProductionConcealmentTest extends BaseMongoServerTest {
  private static final String SECRET = "already-there-8931";

  private MongoClient               client;
  private MongoCollection<Document> collection;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
    GlobalConfiguration.SERVER_MODE.setValue("production");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    getDatabase(0);
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    final MongoDatabase mongoDatabase = client.getDatabase(getDatabaseName());
    mongoDatabase.createCollection("doc8931");
    collection = mongoDatabase.getCollection("doc8931");
    collection.createIndex(new Document("k", 1), new IndexOptions().unique(true));
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
  void duplicatedKeyValueIsNotEchoedInProductionMode() {
    collection.insertOne(new Document("k", SECRET));

    final Throwable thrown = catchThrowable(() -> collection.insertOne(new Document("k", SECRET)));

    assertThat(thrown).isNotNull();
    assertThat(thrown.toString()).doesNotContain(SECRET);
    assertThat(collection.countDocuments()).isEqualTo(1);
  }
}
