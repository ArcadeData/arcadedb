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
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Filter values are bound as SQL parameters rather than spelled into the statement. These tests drive a real server through
 * the Mongo driver and assert the observable half of that: a value that would have needed escaping now matches the document
 * that actually holds it, on all three paths that reach the translator ({@code find}, {@code update}, {@code delete}).
 * <p>
 * A value that only ever produced zero matches would pass an injection test for the wrong reason, so every case here pins a
 * positive match against a document seeded with the same awkward text.
 */
public class MongoDBParameterBindingTest extends BaseGraphServerTest {

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
  void aValueHoldingASingleQuoteIsFoundByFind() {
    final String awkward = "O'Brien";
    collection.insertOne(new Document("name", awkward));
    collection.insertOne(new Document("name", "someone else"));

    final List<Document> found = collection.find(eq("name", awkward)).into(new ArrayList<>());

    assertThat(found).hasSize(1);
    assertThat(found.getFirst().getString("name")).isEqualTo(awkward);
  }

  @Test
  void aValueHoldingABackslashIsFoundByFind() {
    final String awkward = "C:\\data\\'";
    collection.insertOne(new Document("path", awkward));

    final List<Document> found = collection.find(eq("path", awkward)).into(new ArrayList<>());

    assertThat(found).hasSize(1);
    assertThat(found.getFirst().getString("path")).isEqualTo(awkward);
  }

  @Test
  void aValueHoldingASingleQuoteIsMatchedByUpdate() {
    final String awkward = "it's \"quoted\"";
    collection.insertOne(new Document("name", awkward));
    collection.insertOne(new Document("name", "untouched"));

    final UpdateResult result = collection.updateMany(new Document("name", awkward),
        new Document("$set", new Document("touched", "yes")));

    assertThat(result.getModifiedCount()).isEqualTo(1);
    assertThat(collection.find(eq("name", "untouched")).first().containsKey("touched")).isFalse();
  }

  @Test
  void aValueHoldingASingleQuoteIsMatchedByDelete() {
    final String awkward = "drop' me";
    collection.insertOne(new Document("name", awkward));
    collection.insertOne(new Document("name", "keep me"));

    final DeleteResult result = collection.deleteMany(new Document("name", awkward));

    assertThat(result.getDeletedCount()).isEqualTo(1);
    assertThat(collection.countDocuments()).isEqualTo(1);
  }

  @Test
  void anInFilterMatchesEveryListedValueIncludingAwkwardOnes() {
    collection.insertOne(new Document("name", "plain"));
    collection.insertOne(new Document("name", "with' quote"));
    collection.insertOne(new Document("name", "not listed"));

    final UpdateResult result = collection.updateMany(in("name", "plain", "with' quote"),
        new Document("$set", new Document("touched", "yes")));

    assertThat(result.getModifiedCount()).isEqualTo(2);
  }

  @Test
  void anInFilterMatchesOnTheFindPathToo() {
    collection.insertOne(new Document("name", "plain"));
    collection.insertOne(new Document("name", "with' quote"));
    collection.insertOne(new Document("name", "not listed"));

    // find builds its own SELECT, so the binding has to hold on the read path independently of update/delete
    final List<Document> found = collection.find(in("name", "plain", "with' quote")).into(new ArrayList<>());

    assertThat(found).hasSize(2);
  }

  @Test
  void aNotInFilterExcludesOnlyTheListedValues() {
    collection.insertOne(new Document("name", "keep"));
    collection.insertOne(new Document("name", "drop' me"));

    final UpdateResult result = collection.updateMany(new Document("name", new Document("$nin", List.of("drop' me"))),
        new Document("$set", new Document("touched", "yes")));

    assertThat(result.getModifiedCount()).isEqualTo(1);
    assertThat(collection.find(eq("name", "keep")).first().getString("touched")).isEqualTo("yes");
  }

  @Test
  void anEmptyInFilterMatchesNothing() {
    collection.insertOne(new Document("name", "a"));
    collection.insertOne(new Document("name", "b"));

    // a real driver emits {field: {$in: []}} for an empty candidate set; the old code built "IN ()", which is not
    // valid SQL, so this shape has never been exercised
    final UpdateResult result = collection.updateMany(new Document("name", new Document("$in", List.of())),
        new Document("$set", new Document("touched", "yes")));

    assertThat(result.getModifiedCount()).isZero();
    assertThat(collection.countDocuments(new Document("touched", "yes"))).isZero();
  }

  @Test
  void anEmptyNotInFilterMatchesEverything() {
    collection.insertOne(new Document("name", "a"));
    collection.insertOne(new Document("name", "b"));

    final UpdateResult result = collection.updateMany(new Document("name", new Document("$nin", List.of())),
        new Document("$set", new Document("touched", "yes")));

    assertThat(result.getModifiedCount()).isEqualTo(2);
  }

  @Test
  void anEmptyInFilterMatchesNothingOnTheFindPath() {
    collection.insertOne(new Document("name", "a"));

    final List<Document> found = collection.find(new Document("name", new Document("$in", List.of())))
        .into(new ArrayList<>());

    assertThat(found).isEmpty();
  }

  @Test
  void aQuoteBearingValueSurvivesBeingSetByAnUpdate() {
    collection.insertOne(new Document("name", "target"));

    // $set values travel as a JSON literal rather than as a bound parameter, so their escaping is a separate mechanism
    // from the WHERE clause: this pins that it actually holds
    final String awkward = "it's a \"quoted\" C:\\path";
    collection.updateOne(eq("name", "target"), new Document("$set", new Document("note", awkward)));

    assertThat(collection.find(eq("name", "target")).first().getString("note")).isEqualTo(awkward);
  }

  @Test
  void aQuoteBearingValueSurvivesAFullReplacement() {
    collection.insertOne(new Document("name", "target"));

    // a replacement document goes out as SQL CONTENT <json>, another inlined-JSON path
    final String awkward = "replaced' with \"quotes\"";
    collection.replaceOne(eq("name", "target"), new Document("name", "target").append("note", awkward));

    assertThat(collection.find(eq("name", "target")).first().getString("note")).isEqualTo(awkward);
  }

  @Test
  void aNumericValueIsComparedAsANumberNotAsText() {
    collection.insertOne(new Document("name", "big").append("size", 10_000_000_000L));
    collection.insertOne(new Document("name", "small").append("size", 1L));

    // stringifying the bound value would have emitted scientific notation for a double-typed operand
    final UpdateResult result = collection.updateMany(new Document("size", 10_000_000_000L),
        new Document("$set", new Document("touched", "yes")));

    assertThat(result.getModifiedCount()).isEqualTo(1);
    assertThat(collection.find(eq("name", "big")).first().getString("touched")).isEqualTo("yes");
  }
}
