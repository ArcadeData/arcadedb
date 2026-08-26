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
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.gte;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issues #6746 and #6747: {@code find().skip(n)} was silently ignored (the skip loop advanced
 * a counter without consuming the iterator, so every query still started at element 0), and {@code find().sort(...)}
 * was never read by the {@code find} command handler at all, so results came back in unspecified order.
 */
public class MongoDBSortAndSkipTest extends BaseGraphServerTest {

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
    client = new MongoClient(new ServerAddress("localhost", DEF_PORT),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    client.getDatabase(getDatabaseName()).createCollection("doc");
    collection = client.getDatabase(getDatabaseName()).getCollection("doc");

    for (int i = 0; i < 5; i++)
      collection.insertOne(new Document("value", i));
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
  void skipSkipsTheRequestedNumberOfDocumentsOnAnUnfilteredScan() {
    final List<Document> found = collection.find().skip(2).into(new ArrayList<>());

    assertThat(found).hasSize(3);
  }

  @Test
  void skipSkipsTheRequestedNumberOfDocumentsOnAFilteredQuery() {
    final List<Document> found = collection.find(gte("value", 1)).skip(2).into(new ArrayList<>());

    assertThat(found).hasSize(2);
  }

  @Test
  void sortOrdersAnUnfilteredScanAscending() {
    final List<Document> found = collection.find().sort(new Document("value", 1)).into(new ArrayList<>());

    assertThat(found).extracting(d -> d.getInteger("value")).containsExactly(0, 1, 2, 3, 4);
  }

  @Test
  void sortOrdersAnUnfilteredScanDescending() {
    final List<Document> found = collection.find().sort(new Document("value", -1)).into(new ArrayList<>());

    assertThat(found).extracting(d -> d.getInteger("value")).containsExactly(4, 3, 2, 1, 0);
  }

  @Test
  void sortOrdersAFilteredQuery() {
    final List<Document> found = collection.find(gte("value", 1)).sort(new Document("value", -1)).into(new ArrayList<>());

    assertThat(found).extracting(d -> d.getInteger("value")).containsExactly(4, 3, 2, 1);
  }

  @Test
  void sortAndSkipCombineForPagination() {
    final List<Document> found = collection.find().sort(new Document("value", 1)).skip(3).into(new ArrayList<>());

    assertThat(found).extracting(d -> d.getInteger("value")).containsExactly(3, 4);
  }
}
