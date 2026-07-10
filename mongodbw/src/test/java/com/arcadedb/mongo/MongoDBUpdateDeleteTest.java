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
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4750: support the MongoDB {@code update} (replaceOne/updateOne/...) and
 * {@code delete} (deleteOne/deleteMany) commands in the mongodbw plugin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBUpdateDeleteTest extends BaseGraphServerTest {

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
      collection.insertOne(new Document("test", "v" + i).append("counter", i));
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
  void deleteOne() {
    final DeleteResult result = collection.deleteOne(eq("test", "v5"));
    assertThat(result.getDeletedCount()).isEqualTo(1);
    assertThat(collection.countDocuments()).isEqualTo(9);
    assertThat(collection.find(eq("test", "v5")).first()).isNull();
  }

  @Test
  void deleteOneMatchingManyRemovesSingle() {
    // All 10 documents match; deleteOne must remove exactly one.
    final DeleteResult result = collection.deleteOne(new Document());
    assertThat(result.getDeletedCount()).isEqualTo(1);
    assertThat(collection.countDocuments()).isEqualTo(9);
  }

  @Test
  void deleteMany() {
    final DeleteResult result = collection.deleteMany(new Document("counter", new Document("$gte", 5)));
    assertThat(result.getDeletedCount()).isEqualTo(5);
    assertThat(collection.countDocuments()).isEqualTo(5);
  }

  @Test
  void replaceOne() {
    // This is the exact call from issue #4750: db.doc.replaceOne({test:{$eq:"v3"}},{test:"vwxyz"})
    final UpdateResult result = collection.replaceOne(new Document("test", new Document("$eq", "v3")),
        new Document("test", "vwxyz"));
    assertThat(result.getModifiedCount()).isEqualTo(1);

    assertThat(collection.find(eq("test", "v3")).first()).isNull();
    final Document replaced = collection.find(eq("test", "vwxyz")).first();
    assertThat(replaced).isNotNull();
    // The replacement document fully replaces the old one: the 'counter' field is gone.
    assertThat(replaced.get("counter")).isNull();
  }

  @Test
  void updateOneWithSet() {
    final UpdateResult result = collection.updateOne(eq("test", "v2"), new Document("$set", new Document("name", "Jay")));
    assertThat(result.getModifiedCount()).isEqualTo(1);

    final Document updated = collection.find(eq("test", "v2")).first();
    assertThat(updated).isNotNull();
    assertThat(updated.get("name")).isEqualTo("Jay");
    // $set keeps the other fields.
    assertThat(updated.get("counter")).isEqualTo(2);
  }

  @Test
  void updateManyWithInc() {
    final UpdateResult result = collection.updateMany(new Document("counter", new Document("$gte", 8)),
        new Document("$inc", new Document("counter", 100)));
    assertThat(result.getModifiedCount()).isEqualTo(2);

    assertThat(collection.find(eq("counter", 108)).first()).isNotNull();
    assertThat(collection.find(eq("counter", 109)).first()).isNotNull();
  }

  @Test
  void updateOneWithUnset() {
    final UpdateResult result = collection.updateOne(eq("test", "v1"), new Document("$unset", new Document("counter", "")));
    assertThat(result.getModifiedCount()).isEqualTo(1);

    final Document updated = collection.find(eq("test", "v1")).first();
    assertThat(updated).isNotNull();
    assertThat(updated.get("counter")).isNull();
  }

  @Test
  void upsertInsertsWhenNoMatch() {
    final UpdateResult result = collection.updateOne(eq("test", "missing"),
        new Document("$set", new Document("counter", 999)), new UpdateOptions().upsert(true));

    assertThat(result.getUpsertedId()).isNotNull();
    final Document inserted = collection.find(eq("test", "missing")).first();
    assertThat(inserted).isNotNull();
    assertThat(inserted.get("counter")).isEqualTo(999);
  }
}
