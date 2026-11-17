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
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12074: the filters below answered as the embedded mongo-java-server matcher does, not as {@code mongod}. Every expectation
 * is the answer of a MongoDB 7 server, measured in the issue; each filter is also checked through count, update and delete, which
 * select with the same matcher.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12074MongoMatcherSemanticsTest extends BaseMongoServerTest {
  private MongoClient client;
  private int         collections;

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
  void anEmptyDocumentIsEqualOnlyToAnEmptyDocument() {
    assertMatches("{a: {}}", List.of(3), "{_id: 1, a: 1}", "{_id: 2}", "{_id: 3, a: {}}");
    assertMatches("{a: {$eq: {}}}", List.of(3), "{_id: 1, a: 1}", "{_id: 2}", "{_id: 3, a: {}}");
    assertMatches("{a: {$ne: {}}}", List.of(1, 2), "{_id: 1, a: 1}", "{_id: 2}", "{_id: 3, a: {}}");
  }

  @Test
  void aRegularExpressionOnlyMatchesAString() {
    assertMatches(new Document("a", Pattern.compile("^[0-9]+$")), List.of(2), "{_id: 1, a: 2}", "{_id: 2, a: '2'}");
    assertMatches(new Document("a", Pattern.compile("true")), List.of(2), "{_id: 1, a: true}", "{_id: 2, a: 'true'}");
    assertMatches(new Document("a", Pattern.compile("Object")), List.of(),
        new Document("_id", 1).append("a", new ObjectId("5f0000000000000000000001")));
    // an element of an array that is a string still matches
    assertMatches(new Document("a", Pattern.compile("^x")), List.of(1), "{_id: 1, a: [1, 'xy']}", "{_id: 2, a: [1, 2]}");
  }

  @Test
  void modComputesOnLongs() {
    assertMatches("{a: {$mod: [3, 1]}}", List.of(1), new Document("_id", 1).append("a", 1099511627776L));
    assertMatches("{a: {$mod: [4, 0]}}", List.of(1, 2), "{_id: 1, a: 8}", "{_id: 2, a: 8.9}", "{_id: 3, a: 9}");
  }

  @Test
  void gteAndLteNullMatchNullAndMissing() {
    assertMatches("{s: {$gte: null}}", List.of(1, 2), "{_id: 1}", "{_id: 2, s: null}", "{_id: 3, s: 5}");
    assertMatches("{s: {$lte: null}}", List.of(1, 2), "{_id: 1}", "{_id: 2, s: null}", "{_id: 3, s: 5}");
    assertMatches("{s: {$gt: null}}", List.of(), "{_id: 1}", "{_id: 2, s: null}", "{_id: 3, s: 5}");
  }

  @Test
  void aNumericSegmentIsAlsoAFieldOfTheEmbeddedDocuments() {
    assertMatches("{'arr.0': null}", List.of(1), "{_id: 1, arr: [3, {x: 1}]}");
    assertMatches("{'arr.0': {$ne: null}}", List.of(1), "{_id: 1, arr: []}");
    assertMatches("{'arr.0': 'z'}", List.of(1, 2), "{_id: 1, arr: ['z']}", "{_id: 2, arr: [{'0': 'z'}]}", "{_id: 3, arr: ['y']}");
  }

  @Test
  void aScalarOfAnArrayReachesNothing() {
    assertMatches("{'arr.x': {$ne: null}}", List.of(1, 2), "{_id: 1, arr: [1, 2]}", "{_id: 2, arr: [[4, 2]]}");
    assertMatches("{'arr.x.y': null}", List.of(), "{_id: 1, arr: [1, 2]}");
    // outside an array a scalar has a missing field, which is null
    assertMatches("{'a.b': null}", List.of(1, 2), "{_id: 1, a: 5}", "{_id: 2}", "{_id: 3, a: {b: 1}}");
  }

  @Test
  void allWithNoValueMatchesNothingAndAllNullMatchesMissing() {
    assertMatches("{'arr.x': {$all: []}}", List.of(), "{_id: 1, arr: [1, 2]}", "{_id: 2, arr: [{x: 1}]}");
    assertMatches("{a: {$all: [null]}}", List.of(1), "{_id: 1}");
    assertMatches("{a: {$all: [1, 2]}}", List.of(1), "{_id: 1, a: [2, 1, 3]}", "{_id: 2, a: [1]}");
  }

  @Test
  void theRulesTheLibraryAlreadyFollowedStillHold() {
    assertMatches("{tags: 'b'}", List.of(1, 2), "{_id: 1, tags: ['a', 'b']}", "{_id: 2, tags: ['b']}", "{_id: 3, tags: []}");
    assertMatches("{tags: ['b']}", List.of(2), "{_id: 1, tags: ['a', 'b']}", "{_id: 2, tags: ['b']}", "{_id: 3, tags: []}");
    assertMatches("{tags: {$size: 0}}", List.of(3), "{_id: 1, tags: ['a', 'b']}", "{_id: 2, tags: ['b']}", "{_id: 3, tags: []}");
    assertMatches("{n: {$gt: 1}}", List.of(2, 3), "{_id: 1, n: 1}", "{_id: 2, n: 2.5}", "{_id: 3, n: [0, 7]}", "{_id: 4, n: '9'}");
    assertMatches("{n: {$in: [1, null]}}", List.of(1, 4), "{_id: 1, n: 1.0}", "{_id: 2, n: 2}", "{_id: 3, n: '1'}", "{_id: 4}");
    assertMatches("{n: {$nin: [1]}}", List.of(2, 3), "{_id: 1, n: [1, 2]}", "{_id: 2, n: [2]}", "{_id: 3}");
    assertMatches("{v: {$elemMatch: {$gt: 1, $lt: 3}}}", List.of(1), "{_id: 1, v: [0, 2]}", "{_id: 2, v: [0, 3]}");
    assertMatches("{v: {$elemMatch: {k: 1, w: 2}}}", List.of(1), "{_id: 1, v: [{k: 1, w: 2}]}", "{_id: 2, v: [{k: 1}, {w: 2}]}");
    assertMatches("{v: {$exists: false}}", List.of(2), "{_id: 1, v: null}", "{_id: 2}");
    assertMatches("{v: {$type: 'array'}}", List.of(1), "{_id: 1, v: []}", "{_id: 2, v: 1}");
    assertMatches("{v: {$not: {$gt: 1}}}", List.of(1, 3), "{_id: 1, v: 1}", "{_id: 2, v: 2}", "{_id: 3}");
    assertMatches("{$or: [{a: 1}, {b: 1}], c: {$exists: true}}", List.of(1), "{_id: 1, a: 1, c: 0}", "{_id: 2, b: 1}", "{_id: 3, c: 1}");
    assertMatches("{'d.e': 1}", List.of(1, 2), "{_id: 1, d: {e: 1}}", "{_id: 2, d: [{e: 2}, {e: 1}]}", "{_id: 3, d: {e: 2}}");
  }

  @Test
  void anUnknownOperatorIsRefusedWhateverTheDocumentsAnswer() {
    final MongoCollection<Document> collection = collection("{_id: 1, a: -1}");
    final Document filter = Document.parse("{a: {$gt: 0, $bogus: 1}}");
    assertThatThrownBy(() -> collection.find(filter).first()).hasMessageContaining("unknown operator: $bogus");
    // the upsert must not insert a document for a filter mongod refuses
    assertThatThrownBy(() -> collection.updateOne(filter, new Document("$set", new Document("b", 1)), new UpdateOptions().upsert(true)))
        .hasMessageContaining("unknown operator: $bogus");
    assertThat(collection.countDocuments()).isEqualTo(1);
    // on a collection that holds no document too
    final MongoCollection<Document> empty = collection("{_id: 1}");
    empty.deleteMany(new Document());
    assertThatThrownBy(() -> empty.find(filter).first()).hasMessageContaining("unknown operator: $bogus");
  }

  @Test
  void aValueFormElemMatchTestsTheElementItself() {
    // the element [1, 5] is an array: neither 1 nor 5 stands for it
    assertMatches("{v: {$elemMatch: {$gt: 2, $lt: 4}}}", List.of(2), "{_id: 1, v: [[1, 5]]}", "{_id: 2, v: [3]}");
    assertMatches("{v: {$elemMatch: {$size: 2}}}", List.of(1), "{_id: 1, v: [[1, 5]]}", "{_id: 2, v: [3]}");
  }

  @Test
  void anObjectFormElemMatchReadsAnArrayElementByPosition() {
    assertMatches("{v: {$elemMatch: {'1': 5}}}", List.of(1), "{_id: 1, v: [[7, 5]]}", "{_id: 2, v: [[5, 7]]}", "{_id: 3, v: [5]}");
  }

  @Test
  void modMatchesNothingPastTheRangeOfALong() {
    assertMatches("{a: {$mod: [1, 0]}}", List.of(2), "{_id: 1, a: 1e20}", "{_id: 2, a: 7}");
    assertThatThrownBy(() -> collection("{_id: 1, a: 1}").find(Document.parse("{a: {$mod: [1e20, 0]}}")).first())
        .hasMessageContaining("malformed mod");
  }

  @Test
  void nanIsNeitherGreaterNorLessThanANumber() {
    final Document nan = new Document("_id", 1).append("a", Double.NaN);
    final Document zero = new Document("_id", 2).append("a", 0);
    assertMatches(new Document("a", new Document("$lt", 0)), List.of(), nan, zero);
    assertMatches(new Document("a", new Document("$lte", 0)), List.of(2), nan, zero);
    assertMatches(new Document("a", new Document("$gte", Double.NaN)), List.of(1), nan, zero);
    assertMatches(new Document("a", new Document("$gt", Double.NaN)), List.of(), nan, zero);
    assertMatches(new Document("a", Double.NaN), List.of(1), nan, zero);
  }

  @Test
  void arraysAndDocumentsCompareElementByElementInOrder() {
    assertMatches("{a: {$gt: [1]}}", List.of(1), "{_id: 1, a: [2, 0]}", "{_id: 2, a: [0, 9]}");
    assertMatches("{a: {$lt: [1, 5]}}", List.of(1, 2), "{_id: 1, a: [1]}", "{_id: 2, a: [1, 4, 9]}", "{_id: 3, a: [1, 5]}");
    assertMatches("{d: {$gt: {x: 1, y: 1}}}", List.of(1), "{_id: 1, d: {x: 1, y: 2}}", "{_id: 2, d: {x: 1, y: 0}}");
  }

  @Test
  void decimalsPastTheRangeOfADoubleStayDistinct() {
    final Document small = new Document("_id", 1).append("d", Decimal128.parse("1E400"));
    final Document large = new Document("_id", 2).append("d", Decimal128.parse("2E400"));
    assertMatches(new Document("d", Decimal128.parse("1E400")), List.of(1), small, large);
    assertMatches(new Document("d", new Document("$gt", Decimal128.parse("1E400"))), List.of(2), small, large);
  }

  private void assertMatches(final String filter, final List<Integer> expected, final Object... documents) {
    assertMatches(Document.parse(filter), expected, documents);
  }

  /** find, count, updateMany and deleteMany must all select {@code expected}. */
  private void assertMatches(final Document filter, final List<Integer> expected, final Object... documents) {
    final MongoCollection<Document> collection = collection(documents);
    final List<Object> found = new ArrayList<>();
    for (final Document d : collection.find(filter).sort(new Document("_id", 1)))
      found.add(d.get("_id"));
    assertThat(found).as("find(%s)", filter.toJson()).containsExactlyElementsOf(expected);
    assertThat(collection.countDocuments(filter)).as("countDocuments(%s)", filter.toJson()).isEqualTo(expected.size());
    assertThat(collection.updateMany(filter, new Document("$set", new Document("touched", true))).getMatchedCount())
        .as("updateMany(%s)", filter.toJson()).isEqualTo(expected.size());
    assertThat(collection.deleteMany(filter).getDeletedCount()).as("deleteMany(%s)", filter.toJson()).isEqualTo(expected.size());
  }

  private MongoCollection<Document> collection(final Object... documents) {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("c" + (collections++));
    final List<Document> list = new ArrayList<>();
    for (final Object document : documents)
      list.add(document instanceof Document d ? d : Document.parse((String) document));
    if (!list.isEmpty())
      collection.insertMany(list);
    return collection;
  }
}
