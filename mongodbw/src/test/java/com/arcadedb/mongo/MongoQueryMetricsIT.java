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
import com.arcadedb.server.BaseGraphServerTest;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that queries executed over a MongoDB connection are tagged with protocol="mongo"
 * in the arcadedb.query.duration Micrometer timer.
 */
public class MongoQueryMetricsIT extends BaseGraphServerTest {

  private static final int DEF_PORT = 27017;

  private MongoClient client;

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
    client.getDatabase(getDatabaseName()).createCollection("MongoMetricsCollection");
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("MongoMetricsCollection");
    collection.insertOne(new Document("name", "test"));
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    client.close();
    super.endTest();
  }

  @Test
  void mongoQueriesTaggedWithMongoProtocol() {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("MongoMetricsCollection");
    collection.find(BsonDocument.parse("{ name: { $eq: \"test\" } }")).first();

    final Timer timer = Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", "mongo").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
  }
}
