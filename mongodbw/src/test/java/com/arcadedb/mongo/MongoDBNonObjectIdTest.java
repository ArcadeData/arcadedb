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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6939: a document with a client-supplied {@code _id} that is not a 24-char hex
 * ObjectId string (an integer, or an odd-length string) made every subsequent unfiltered read of the collection
 * throw, because {@code convertMapToMongoDB} treated every {@code _id} as an ObjectId hex string unconditionally.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBNonObjectIdTest extends BaseGraphServerTest {

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
  void integerIdDoesNotBreakSubsequentReads() {
    collection.insertOne(new Document("_id", 1).append("name", "x"));

    final Document found = collection.find().first();

    assertThat(found).isNotNull();
    assertThat(found.get("_id")).isEqualTo(1);
    assertThat(found.getString("name")).isEqualTo("x");
  }

  @Test
  void oddLengthStringIdDoesNotBreakSubsequentReads() {
    collection.insertOne(new Document("_id", "abc").append("name", "y"));

    final Document found = collection.find().first();

    assertThat(found).isNotNull();
    assertThat(found.get("_id")).isEqualTo("abc");
    assertThat(found.getString("name")).isEqualTo("y");
  }

  @Test
  void evenLengthNonHexStringIdIsPreservedNotSilentlyCorrupted() {
    collection.insertOne(new Document("_id", "not-a-hex-strng!").append("name", "z"));

    final Document found = collection.find().first();

    assertThat(found).isNotNull();
    assertThat(found.get("_id")).isEqualTo("not-a-hex-strng!");
  }

  @Test
  void mixedIdTypesInSameCollectionAllReadBack() {
    collection.insertOne(new Document("_id", 1).append("name", "int-id"));
    collection.insertOne(new Document("_id", "abc").append("name", "odd-string-id"));

    final List<Document> all = collection.find().into(new ArrayList<>());

    assertThat(all).hasSize(2);
    assertThat(all).extracting(d -> d.getString("name")).containsExactlyInAnyOrder("int-id", "odd-string-id");
  }
}
