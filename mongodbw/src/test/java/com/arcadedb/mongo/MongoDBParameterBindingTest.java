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
import java.util.Date;
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

    // $set values are bound as the payload of MERGE, so this is the observable contract the binding has to preserve
    final String awkward = "it's a \"quoted\" C:\\path";
    collection.updateOne(eq("name", "target"), new Document("$set", new Document("note", awkward)));

    assertThat(collection.find(eq("name", "target")).first().getString("note")).isEqualTo(awkward);
  }

  @Test
  void aQuoteBearingValueSurvivesAFullReplacement() {
    collection.insertOne(new Document("name", "target"));

    // a replacement document goes out as SQL CONTENT :p<n>, bound the same way
    final String awkward = "replaced' with \"quotes\"";
    collection.replaceOne(eq("name", "target"), new Document("name", "target").append("note", awkward));

    assertThat(collection.find(eq("name", "target")).first().getString("note")).isEqualTo(awkward);
  }

  @Test
  void aNestedDocumentSurvivesBeingSetByAnUpdate() {
    collection.insertOne(new Document("name", "target"));

    // the bound payload is a nested Map rather than JSON text, so the nesting has to survive the parameter round trip
    collection.updateOne(eq("name", "target"),
        new Document("$set", new Document("address", new Document("city", "Rome").append("zip", 145))));

    final Document address = (Document) collection.find(eq("name", "target")).first().get("address");
    assertThat(address.getString("city")).isEqualTo("Rome");
    assertThat(address.getInteger("zip")).isEqualTo(145);
  }

  @Test
  void anArraySurvivesBeingSetByAnUpdate() {
    collection.insertOne(new Document("name", "target"));

    collection.updateOne(eq("name", "target"), new Document("$set", new Document("tags", List.of("a", "b' c"))));

    assertThat(collection.find(eq("name", "target")).first().getList("tags", String.class)).containsExactly("a", "b' c");
  }

  @Test
  void aNestedDocumentSurvivesAFullReplacement() {
    collection.insertOne(new Document("name", "target"));

    collection.replaceOne(eq("name", "target"),
        new Document("name", "target").append("address", new Document("city", "Rome' \"quoted\"")));

    final Document address = (Document) collection.find(eq("name", "target")).first().get("address");
    assertThat(address.getString("city")).isEqualTo("Rome' \"quoted\"");
  }

  @Test
  void aCombinedSetAndIncUpdateAppliesBothOperations() {
    collection.insertOne(new Document("name", "target").append("count", 1));

    // a driver can send both operators in one update: the statement then chains MERGE :p0 with SET ... += :p1, so
    // this is what proves the bound payload does not swallow the SET keyword that follows it
    collection.updateOne(eq("name", "target"),
        new Document("$set", new Document("note", "v1' \"x\"")).append("$inc", new Document("count", 3)));

    final Document found = collection.find(eq("name", "target")).first();
    assertThat(found.getString("note")).isEqualTo("v1' \"x\"");
    assertThat(((Number) found.get("count")).intValue()).isEqualTo(4);
  }

  @Test
  void aDateSurvivesBeingSetByAnUpdate() {
    collection.insertOne(new Document("name", "target"));

    // the fidelity half of the binding: routing through JSONObject reshaped a Date before it reached the record, so
    // this guards the stated behaviour change end to end rather than only at the parameter map
    final Date when = new Date(1_700_000_000_000L);
    collection.updateOne(eq("name", "target"), new Document("$set", new Document("when", when)));

    assertThat(collection.find(eq("name", "target")).first().getDate("when")).isEqualTo(when);
  }

  @Test
  void aDateSurvivesBeingInsertedAndReadBack() {
    // the insert path never went through the update binding, so this covers the read-side conversion on its own: a
    // stored temporal property used to reach the BSON encoder as a java.time value and kill the connection
    final Date when = new Date(1_700_000_000_000L);
    collection.insertOne(new Document("name", "inserted").append("when", when));

    assertThat(collection.find(eq("name", "inserted")).first().getDate("when")).isEqualTo(when);
  }

  @Test
  void aDateNestedInsideASubDocumentSurvivesTheRoundTrip() {
    final Date when = new Date(1_700_000_000_000L);
    collection.insertOne(new Document("name", "nested").append("meta", new Document("created", when)));

    final Document meta = (Document) collection.find(eq("name", "nested")).first().get("meta");
    assertThat(meta.getDate("created")).isEqualTo(when);
  }

  @Test
  void aLargeDoubleSurvivesBeingSetByAnUpdate() {
    collection.insertOne(new Document("name", "target"));

    // inlining stringified this into scientific notation on its way into the statement
    final double big = 1.0E10d;
    collection.updateOne(eq("name", "target"), new Document("$set", new Document("ratio", big)));

    assertThat(((Number) collection.find(eq("name", "target")).first().get("ratio")).doubleValue()).isEqualTo(big);
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
