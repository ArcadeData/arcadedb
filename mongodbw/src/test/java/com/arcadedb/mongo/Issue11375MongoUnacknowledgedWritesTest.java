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
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11375: an unacknowledged ({@code w=0}) write, an OP_MSG with the {@code moreToCome} flag, runs. The library refused
 * the flag, so the write was dropped with the connection and the client, which reads no reply, saw no error.
 */
class Issue11375MongoUnacknowledgedWritesTest extends BaseMongoServerTest {

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
  void unacknowledgedWritesRunAndTheConnectionStaysInStep() {
    getDatabase(0);
    // one connection: the count is read after the unacknowledged writes, on the connection they were sent on
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).connectionsPerHost(1).build())) {
      client.getDatabase(getDatabaseName()).createCollection("w0");
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("w0");
      final MongoCollection<Document> unacknowledged = collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED);

      for (int i = 0; i < 3; i++)
        unacknowledged.insertOne(new Document("i", i));

      assertThat(collection.countDocuments()).isEqualTo(3);
      // an acknowledged write after them gets its own reply, not one of theirs
      collection.insertOne(new Document("i", 3));
      assertThat(collection.countDocuments()).isEqualTo(4);
    }
  }
}
