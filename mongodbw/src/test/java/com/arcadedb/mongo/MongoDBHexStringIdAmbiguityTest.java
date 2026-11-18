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
 * Issue #6955, fixed by #12073: a client-supplied {@code String _id} that happens to be exactly 24 hex characters used to be stored
 * as the same bare hex string as an {@code ObjectId}, and read back as one. Such a String is now stored escaped, so it reads back as
 * the String it was and is a different key from the ObjectId. A bare hex {@code _id} already on disk (stored before, or written
 * through SQL) keeps reading back as an ObjectId, the form it always had.
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
  void hexLookingStringIdRoundTripsAsAString() {
    final String hexLookingId = "abcdef0123456789abcdef01";
    collection.insertOne(new Document("_id", hexLookingId).append("name", "x"));

    final Document found = collection.find().first();

    assertThat(found).isNotNull();
    assertThat(found.get("_id")).isEqualTo(hexLookingId);
    assertThat(collection.find(new Document("_id", hexLookingId)).first()).isNotNull();
    assertThat(collection.find(new Document("_id", new ObjectId(hexLookingId))).first()).isNull();
  }

  @Test
  void aBareHexIdOnDiskStillReadsAsAnObjectId() {
    final String hex = "abcdef0123456789abcdef02";
    final Database db = getServer(0).getDatabaseForOperator(getDatabaseName());
    db.transaction(() -> db.newDocument("doc").set("_id", hex).set("name", "legacy").save());

    final Document found = collection.find(new Document("_id", new ObjectId(hex))).first();
    assertThat(found).isNotNull();
    assertThat(found.get("_id")).isEqualTo(new ObjectId(hex));
    assertThat(found.get("name")).isEqualTo("legacy");
  }
}
