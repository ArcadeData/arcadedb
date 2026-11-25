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
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An {@code update} of a collection that does not exist answers as before (nothing matched, nothing modified, its statements still
 * validated) and leaves nothing behind for that name: the per-collection state the plugin keeps is for the collections that exist,
 * so it does not grow with the names a client sends. An update of a collection that exists, and an upsert that creates one, still
 * run under the collection's lock.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoUpdateMissingCollectionTest extends BaseMongoServerTest {
  private static final String PREFIX = "updmissing";

  private MongoClient client;
  private Database   database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    database = getServerDatabase(0, getDatabaseName());
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    try {
      if (client != null)
        client.close();
      if (database != null)
        MongoDBCollectionWrapper.forgetIdIndexes(database);
    } finally {
      super.endTest();
    }
  }

  @Test
  void anUpdateOfMissingCollectionsAnswersNothingMatchedAndLeavesNothingPerName() {
    final List<String> names = new ArrayList<>();
    for (int i = 0; i < 200; i++)
      names.add(PREFIX + i + "x".repeat(i % 7 * 40));

    for (final String name : names) {
      final Document reply = client.getDatabase(getDatabaseName()).runCommand(new Document("update", name).append("updates",
          List.of(new Document("q", new Document("a", 1)).append("u", Document.parse("{$set: {b: 2}}")).append("multi", true))));

      assertThat(reply.getInteger("n")).isEqualTo(0);
      assertThat(reply.getInteger("nModified")).isEqualTo(0);
      assertThat(reply.get("ok")).isEqualTo(1.0);
      assertThat(reply.containsKey("writeErrors")).isFalse();
    }

    for (final String name : names) {
      assertThat(database.getSchema().existsType(name)).as("the collection %s", name).isFalse();
      assertThat(MongoDBCollectionWrapper.hasIdIndexLock(database, name)).as("the lock of %s", name).isFalse();
    }
  }

  @Test
  void anInvalidUpdateOfAMissingCollectionIsStillRefused() {
    final String name = PREFIX + "refused";
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection(name);

    assertThatThrownBy(() -> collection.updateMany(new Document(), Document.parse("{$inc: {s: 'a'}}"))).isInstanceOfSatisfying(
        MongoException.class, e -> assertThat(e.getCode()).isEqualTo(14));
    assertThatThrownBy(() -> collection.updateMany(Document.parse("{$or: 5}"), Document.parse("{$set: {s: 1}}"))).isInstanceOf(
        MongoException.class);

    assertThat(database.getSchema().existsType(name)).isFalse();
    assertThat(MongoDBCollectionWrapper.hasIdIndexLock(database, name)).isFalse();
  }

  @Test
  void anUpdateOfAnExistingCollectionRunsUnderItsLock() {
    final String name = PREFIX + "existing";
    // created and filled outside the plugin: nothing of its own exists yet
    database.getSchema().createDocumentType(name);
    database.transaction(() -> database.newDocument(name).set("a", 1).save());
    assertThat(MongoDBCollectionWrapper.hasIdIndexLock(database, name)).isFalse();

    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection(name);
    final UpdateResult result = collection.updateOne(new Document("a", 1), Document.parse("{$set: {b: 2}}"));

    assertThat(result.getMatchedCount()).isEqualTo(1);
    assertThat(result.getModifiedCount()).isEqualTo(1);
    assertThat(collection.find(new Document("a", 1)).first().get("b")).isEqualTo(2);
    assertThat(MongoDBCollectionWrapper.hasIdIndexLock(database, name)).isTrue();
  }

  @Test
  void anUpsertCreatesTheCollectionAndRunsUnderItsLock() {
    final String name = PREFIX + "upserted";
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection(name);

    final UpdateResult result = collection.updateOne(new Document("a", 1), Document.parse("{$set: {b: 2}}"), new UpdateOptions().upsert(true));

    assertThat(result.getMatchedCount()).isEqualTo(0);
    assertThat(result.getUpsertedId()).isNotNull();
    assertThat(database.getSchema().existsType(name)).isTrue();
    assertThat(MongoDBCollectionWrapper.hasIdIndexLock(database, name)).isTrue();
    assertThat(collection.countDocuments(new Document("a", 1).append("b", 2))).isEqualTo(1);
  }
}
