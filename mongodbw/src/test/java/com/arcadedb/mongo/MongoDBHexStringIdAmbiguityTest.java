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
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A client-supplied {@code String _id} of exactly 24 hex characters (issue #6955) used to be stored as the same bare hex string
 * as an {@code ObjectId _id}, so a plain {@code insertOne} followed by a {@code find} read it back as an {@code ObjectId}, and the
 * two were one value to the {@code _id} index and to filters. Since issue #12073 such a string is stored escaped: it reads back as
 * the {@code String} it was, next to the {@code ObjectId} with the same hex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBHexStringIdAmbiguityTest extends BaseMongoServerTest {

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
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
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
  void hexLookingStringIdInsertedPlainRoundTripsAsAString() {
    final String hexLookingId = "abcdef0123456789abcdef01";
    collection.insertOne(new Document("_id", hexLookingId).append("name", "x"));
    collection.insertOne(new Document("_id", new ObjectId(hexLookingId)).append("name", "y"));

    assertThat(collection.find(new Document("_id", hexLookingId)).first().get("name")).isEqualTo("x");
    assertThat(collection.find(new Document("_id", hexLookingId)).first().get("_id")).isEqualTo(hexLookingId);
    assertThat(collection.find(new Document("_id", new ObjectId(hexLookingId))).first().get("name")).isEqualTo("y");
    assertThat(collection.find(new Document("_id", new ObjectId(hexLookingId))).first().get("_id")).isInstanceOf(ObjectId.class);
    assertThat(collection.countDocuments()).isEqualTo(2);
  }
}
