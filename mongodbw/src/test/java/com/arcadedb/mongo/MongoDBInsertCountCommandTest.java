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
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6940: the raw {@code insert} command reported {@code n} one higher than the number
 * of documents actually inserted (a leftover {@code ++n;} from a per-document loop that was replaced by a single
 * bulk call), and the raw {@code count} command ignored its {@code query}, {@code skip} and {@code limit}
 * arguments entirely, always answering with the whole collection size.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBInsertCountCommandTest extends BaseGraphServerTest {

  private static final int          DEF_PORT = 27017;
  private              MongoClient  client;
  private              MongoDatabase        mongoDatabase;
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
    client = new MongoClient(new ServerAddress("localhost", DEF_PORT),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    mongoDatabase = client.getDatabase(getDatabaseName());
    mongoDatabase.createCollection("doc");
    collection = mongoDatabase.getCollection("doc");
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
  void insertCommandReportsExactInsertedCount() {
    final Document command = new Document("insert", "doc")
        .append("documents", List.of(new Document("a", 1)));

    final Document result = mongoDatabase.runCommand(command);

    assertThat(result.getInteger("n")).isEqualTo(1);
  }

  @Test
  void insertManyReportsExactInsertedCount() {
    collection.insertMany(List.of(new Document("a", 1), new Document("a", 2), new Document("a", 3)));

    assertThat(collection.countDocuments()).isEqualTo(3);
  }

  @Test
  void countCommandHonoursQuery() {
    collection.insertOne(new Document("name", "found"));
    collection.insertOne(new Document("name", "other"));

    final Document command = new Document("count", "doc").append("query", new Document("name", "nope"));
    final Document result = mongoDatabase.runCommand(command);

    assertThat(result.getInteger("n")).isEqualTo(0);

    final Document matching = mongoDatabase.runCommand(
        new Document("count", "doc").append("query", new Document("name", "found")));
    assertThat(matching.getInteger("n")).isEqualTo(1);
  }

  @Test
  void countCommandHonoursSkipAndLimit() {
    for (int i = 0; i < 10; i++)
      collection.insertOne(new Document("counter", i));

    final Document skipped = mongoDatabase.runCommand(new Document("count", "doc").append("skip", 5));
    assertThat(skipped.getInteger("n")).isEqualTo(5);

    final Document limited = mongoDatabase.runCommand(new Document("count", "doc").append("limit", 3));
    assertThat(limited.getInteger("n")).isEqualTo(3);

    final Document both = mongoDatabase.runCommand(new Document("count", "doc").append("skip", 8).append("limit", 5));
    assertThat(both.getInteger("n")).isEqualTo(2);
  }
}
