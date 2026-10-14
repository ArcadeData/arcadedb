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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9974: the counts a MongoDB write command replies are the records the engine wrote. A write a before-event listener vetoed
 * was counted anyway: {@code record.delete()} / {@code record.save()} cannot answer whether the record went, so {@code delete}'s
 * {@code n}, {@code update}'s {@code n}/{@code nModified}, an upsert's {@code upserted} and {@code insert}'s {@code n} all reported
 * work the engine refused to do.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9974MongoVetoCountTest extends BaseMongoServerTest {
  private MongoClient               client;
  private MongoDatabase             mongoDatabase;
  private MongoCollection<Document> collection;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    mongoDatabase = client.getDatabase(getDatabaseName());
    mongoDatabase.createCollection("doc");
    collection = mongoDatabase.getCollection("doc");
    for (final String name : new String[] { "a", "keep", "b" })
      collection.insertOne(new Document("name", name));
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
  void deleteManyCountsOnlyTheRecordsItDeleted() {
    vetoDeleteOf("keep");
    assertThat(collection.deleteMany(new Document("name", new Document("$in", List.of("a", "keep", "b")))).getDeletedCount()).isEqualTo(2L);
    assertThat(collection.countDocuments()).isEqualTo(1L);
  }

  @Test
  void deleteOneOfAVetoedRecordCountsZero() {
    vetoDeleteOf("keep");
    assertThat(collection.deleteOne(eq("name", "keep")).getDeletedCount()).isZero();
    assertThat(collection.countDocuments()).isEqualTo(3L);
  }

  /** A veto is where MongoDB's {@code n} (matched) and {@code nModified} (changed) part ways. */
  @Test
  void updateManyCountsTheVetoedRecordAsMatchedButNotModified() {
    vetoUpdateOf("keep");

    final UpdateResult result = collection.updateMany(new Document("name", new Document("$in", List.of("a", "keep", "b"))),
        new Document("$set", new Document("touched", true)));
    assertThat(result.getMatchedCount()).isEqualTo(3L);
    assertThat(result.getModifiedCount()).isEqualTo(2L);
    assertThat(collection.countDocuments(eq("touched", true))).isEqualTo(2L);
  }

  /** Without a filter the update runs as one SQL UPDATE: the same two counts must come out of it. */
  @Test
  void anUnfilteredUpdateManyCountsTheVetoedRecordAsMatchedButNotModified() {
    vetoUpdateOf("keep");

    final UpdateResult result = collection.updateMany(new Document(), new Document("$set", new Document("touched", true)));
    assertThat(result.getMatchedCount()).isEqualTo(3L);
    assertThat(result.getModifiedCount()).isEqualTo(2L);
    assertThat(collection.countDocuments(eq("touched", true))).isEqualTo(2L);
  }

  /**
   * A match whose update was vetoed is not "no match": an upsert must not insert a second document for it, and the single-record
   * update must not select it again and ask the listener again.
   */
  @Test
  void anUpsertWhoseMatchIsVetoedInsertsNothing() {
    final AtomicInteger asked = new AtomicInteger();
    serverDatabase().getSchema().getType("doc").getEvents().registerListener((BeforeRecordUpdateListener) record -> {
      if (!"keep".equals(record.asDocument().getString("name")))
        return true;
      asked.incrementAndGet();
      return false;
    });

    final UpdateResult result = collection.updateOne(eq("name", "keep"), new Document("$set", new Document("v", 1)),
        new UpdateOptions().upsert(true));
    assertThat(result.getUpsertedId()).isNull();
    assertThat(result.getMatchedCount()).isEqualTo(1L);
    assertThat(result.getModifiedCount()).isZero();
    assertThat(collection.countDocuments()).isEqualTo(3L);
    assertThat(collection.countDocuments(eq("name", "keep"))).isEqualTo(1L);
    assertThat(asked.get()).isEqualTo(1);
  }

  @Test
  void aVetoedUpsertReportsNothingUpserted() {
    vetoCreateOf("refused");
    final UpdateResult result = collection.updateOne(eq("name", "refused"), new Document("$set", new Document("v", 1)),
        new UpdateOptions().upsert(true));
    assertThat(result.getUpsertedId()).isNull();
    assertThat(collection.countDocuments()).isEqualTo(3L);
  }

  @Test
  void insertCountsOnlyTheDocumentsItStored() {
    vetoCreateOf("refused");
    final Document reply = mongoDatabase.runCommand(new Document("insert", "doc").append("documents",
        List.of(new Document("name", "c"), new Document("name", "refused"), new Document("name", "d"))));
    assertThat(((Number) reply.get("n")).intValue()).isEqualTo(2);
    assertThat(collection.countDocuments()).isEqualTo(5L);
  }

  private Database serverDatabase() {
    return getServerDatabase(0, getDatabaseName());
  }

  private void vetoDeleteOf(final String name) {
    serverDatabase().getSchema().getType("doc").getEvents()
        .registerListener((BeforeRecordDeleteListener) record -> !name.equals(record.asDocument().getString("name")));
  }

  private void vetoUpdateOf(final String name) {
    serverDatabase().getSchema().getType("doc").getEvents()
        .registerListener((BeforeRecordUpdateListener) record -> !name.equals(record.asDocument().getString("name")));
  }

  private void vetoCreateOf(final String name) {
    serverDatabase().getSchema().getType("doc").getEvents()
        .registerListener((BeforeRecordCreateListener) record -> !name.equals(record.asDocument().getString("name")));
  }
}
